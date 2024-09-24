/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 *  LICENSE file in the root directory of this source tree.
 */

import {
  Sampling,
  isSamplingEnable,
  isSamplingPercent,
  isSamplingRows,
  FieldAtomicTypeDef,
  TimeDeltaExpr,
  TypecastExpr,
  MeasureTimeExpr,
  TimeTruncExpr,
  TimeExtractExpr,
  TimeLiteralNode,
  RegexMatchExpr,
} from '../../model/malloy_types';
import {
  DialectFunctionOverloadDef,
  expandOverrideMap,
  expandBlueprintMap,
} from '../functions';
import {Dialect, DialectFieldList, QueryInfo} from '../dialect';
import {IGNITES_DIALECT_FUNCTIONS} from './dialect_functions';
import {IGNITES_MALLOY_STANDARD_OVERLOADS} from './function_overrides';

// These are the units that "TIMESTAMP_ADD" "TIMESTAMP_DIFF" accept
function timestampMeasureable(units: string): boolean {
  return [
    'microsecond',
    'millisecond',
    'second',
    'minute',
    'hour',
    'day',
  ].includes(units);
}

const igniteToMalloyTypes: {[key: string]: FieldAtomicTypeDef} = {
  'java.lang.Boolean': {type: 'boolean'},
  'java.lang.Byte': {type: 'string'},
  'java.lang.Character': {type: 'string'},
  'java.lang.Double': {type: 'number', numberType: 'float'},
  'java.lang.Float': {type: 'number', numberType: 'float'},
  'java.lang.Integer': {type: 'number', numberType: 'integer'},
  'java.lang.Long': {type: 'number', numberType: 'integer'},
  'java.lang.Short': {type: 'number', numberType: 'integer'},
  'java.lang.String': {type: 'string'},
  'java.math.BigDecimal': {type: 'number', numberType: 'float'},
  'java.sql.Date': {type: 'date'},
  'java.sql.Time': {type: 'timestamp'},
  'java.sql.Timestamp': {type: 'timestamp'},
  'java.util.Date': {type: 'date'},
  'java.util.UUID': {type: 'sql native', rawType: 'UUID'},
};

/**
 * Return a non UTC timezone, if one was specificed.
 */
function qtz(qi: QueryInfo): string | undefined {
  const tz = qi.queryTimezone;
  if (tz && tz !== 'UTC') {
    return tz;
  }
}

const extractMap: Record<string, string> = {
  day_of_week: 'dow',
  day_of_year: 'doy',
};

declare interface TimeMeasure {
  use: string;
  ratio: number;
}

export class IgniteDialect extends Dialect {
  name = 'ignite';
  defaultNumberType = 'DOUBLE PRECISION';
  defaultDecimalType = 'DOUBLE PRECISION';
  udfPrefix = 'pg_temp.__udf';
  hasFinalStage = false; // ignite sorting getting messed up with final stage
  divisionIsInteger = true;
  supportsSumDistinctFunction = false;
  unnestWithNumbers = false;
  defaultSampling = {rows: 50000};
  supportUnnestArrayAgg = true;
  supportsAggDistinct = true;
  supportsCTEinCoorelatedSubQueries = false;
  supportsSafeCast = false;
  dontUnionIndex = false;
  supportsQualify = false;
  supportsNesting = false; // this is not working, ignite does not support nested queries
  experimental = false;
  readsNestedData = false;
  supportsComplexFilteredSources = false;

  quoteTablePath(tablePath: string): string {
    return tablePath
      .split('.')
      .map(part => `"${part}"`)
      .join('.');
  }

  sqlGroupSetTable(groupSetCount: number): string {
    return `CROSS JOIN (SELECT x group_set FROM TABLE(SYSTEM_RANGE(0, ${groupSetCount}))) as group_set_alias`;
  }

  sqlAnyValue(_groupSet: number, fieldName: string): string {
    return `MAX(${fieldName})`;
  }

  mapFields(fieldList: DialectFieldList): string {
    return fieldList
      .map(
        f =>
          `\n  ${f.sqlExpression}${
            f.type === 'number' ? `::${this.defaultNumberType}` : ''
          } as ${f.sqlOutputName}`
        //`${f.sqlExpression} ${f.type} as ${f.sqlOutputName}`
      )
      .join(', ');
  }

  // can array agg or any_value a struct...
  sqlAggregateTurtle(
    groupSet: number,
    fieldList: DialectFieldList,
    orderBy: string | undefined,
    _limit: number | undefined
  ): string {
    // let tail = '';
    // if (limit !== undefined) {
    //   tail += ` LIMIT ${limit}`;
    // }
    const fields = fieldList
      .map(
        f =>
          `\n  '${f.sqlOutputName.substring(1, f.sqlOutputName.length - 1)}': ${
            f.sqlExpression
          }`
      )
      .join(',');
    return `ARRAY_AGG(CASE WHEN group_set=${groupSet} THEN JSON_OBJECT(${fields}\n) END ${orderBy})`;
  }

  sqlAnyValueTurtle(groupSet: number, fieldList: DialectFieldList): string {
    const fields = fieldList
      .map(
        f =>
          `\n  '${f.sqlOutputName.substring(1, f.sqlOutputName.length - 1)}': ${
            f.sqlExpression
          }`
      )
      .join(',');
    return `ANY_VALUE(CASE WHEN group_set=${groupSet} THEN JSON_OBJECT(${fields}) END)`;
  }

  sqlAnyValueLastTurtle(
    name: string,
    groupSet: number,
    sqlName: string
  ): string {
    return `(ARRAY_AGG(${name}) FILTER (WHERE group_set=${groupSet} AND ${name} IS NOT NULL))[1] as ${sqlName}`;
  }

  sqlCoaleseMeasuresInline(
    _groupSet: number,
    _fieldList: DialectFieldList
  ): string {
    throw new Error('sqlCoaleseMeasuresInline: Not supported!');
  }

  sqlUnnestAlias(
    _source: string,
    _alias: string,
    _fieldList: DialectFieldList,
    _needDistinctKey: boolean,
    _isArray: boolean,
    _isInNestedPipeline: boolean
  ): string {
    throw new Error('sqlUnnestAlias: Not supported!');
  }

  sqlSumDistinctHashedKey(sqlDistinctKey: string): string {
    return `('x' || MD5(${sqlDistinctKey}::varchar))::bit(64)::bigint::DECIMAL(65,0)  *18446744073709551616 + ('x' || SUBSTRING(MD5(${sqlDistinctKey}::varchar),17))::bit(64)::bigint::DECIMAL(65,0)`;
  }

  sqlGenerateUUID(): string {
    throw new Error('sqlGenerateUUID: Not supported!');
  }

  sqlFieldReference(
    alias: string,
    fieldName: string,
    fieldType: string,
    isNested: boolean,
    _isArray: boolean
  ): string {
    if (isNested) {
      throw new Error('multi-level nesting not supported!');
    } else {
      if (fieldType === 'date') {
        return `${alias}."${fieldName}"::timestamp`;
      }
      return `${alias}."${fieldName}"`;
    }
  }

  sqlUnnestPipelineHead(
    _isSingleton: boolean,
    _sourceSQLExpression: string
  ): string {
    throw new Error('sqlUnnestPipelineHead: Not supported!');
  }

  sqlCreateFunction(_id: string, _funcText: string): string {
    throw new Error('sqlCreateFunction: Not supported!');
  }

  sqlCreateFunctionCombineLastStage(
    lastStageName: string,
    fieldList: DialectFieldList
  ): string {
    const fields = fieldList.map(f => f.sqlExpression).join(', ');
    // const definitions = this.buildTypeExpression(fieldList);
    return `SELECT ARRAY_AGG(ROW(${fields})) FROM ${lastStageName}\n`;
  }

  sqlFinalStage(lastStageName: string, _fields: string[]): string {
    return `SELECT * FROM ${lastStageName}`;
  }

  sqlSelectAliasAsStruct(alias: string): string {
    return `ROW(${alias})`;
  }

  sqlMaybeQuoteIdentifier(identifier: string): string {
    return `"${identifier}"`;
  }

  sqlCreateTableAsSelect(_tableName: string, _sql: string): string {
    throw new Error('sqlCreateTableAsSelect: Not implemented Yet');
  }

  sqlCast(qi: QueryInfo, cast: TypecastExpr): string {
    const op = `${cast.srcType}::${cast.dstType}`;
    const tz = qtz(qi);
    const src = cast.e.sql || '';
    if (op === 'timestamp::date' && tz) {
      return `DATE(${src},'${tz}')`;
    }
    if (op === 'date::timestamp' && tz) {
      return `TIMESTAMP(${src}, '${tz}')`;
    }
    if (cast.srcType !== cast.dstType) {
      const dstType =
        typeof cast.dstType === 'string'
          ? this.malloyTypeToSQLType({type: cast.dstType})
          : cast.dstType.raw;
      const castFunc = cast.safe ? 'SAFE_CAST' : 'CAST';
      return `${castFunc}(${src} AS ${dstType})`;
    }
    return src;
  }

  sqlMeasureTimeExpr(measure: MeasureTimeExpr): string {
    const measureMap: Record<string, TimeMeasure> = {
      'microsecond': {use: 'microsecond', ratio: 1},
      'millisecond': {use: 'microsecond', ratio: 1000},
      'second': {use: 'millisecond', ratio: 1000},
      'minute': {use: 'second', ratio: 60},
      'hour': {use: 'minute', ratio: 60},
      'day': {use: 'hour', ratio: 24},
      'week': {use: 'day', ratio: 7},
    };
    const from = measure.kids.left;
    const to = measure.kids.right;
    let lVal = from.sql;
    let rVal = to.sql;
    if (measureMap[measure.units]) {
      const {use: measureIn, ratio} = measureMap[measure.units];
      if (!timestampMeasureable(measureIn)) {
        throw new Error(`Measure in '${measureIn} not implemented`);
      }
      if (from.dataType !== to.dataType) {
        throw new Error("Can't measure difference between different types");
      }
      if (from.dataType === 'date') {
        lVal = `CAST(${lVal} AS TIMESTAMP)`;
        rVal = `CAST(${rVal} AS TIMESTAMP)`;
      }
      let measured = `TIMESTAMPDIFF(${measureIn},${lVal},${rVal})`;
      if (ratio !== 1) {
        measured = `FLOOR(${measured}/${ratio.toString()}.0)`;
      }
      return measured;
    }
    throw new Error(`Measure '${measure.units} not implemented`);
  }

  sqlAlterTimeExpr(df: TimeDeltaExpr): string {
    const from = df.kids.base;
    let dataType: string = from.dataType;
    let sql = from.sql;
    if (df.units !== 'day' && timestampMeasureable(df.units)) {
      // The units must be done in timestamp, no matter the input type
      if (dataType !== 'timestamp') {
        sql = `CAST(${sql} AS TIMESTAMP)`;
        dataType = 'timestamp';
      }
      // } else if (dataType === 'timestamp') {
      //   sql = `DATETIME(${sql})`;
      //   dataType = 'datetime';
    }
    const newTime = `TIMESTAMPADD(${df.units}, ${df.op}${df.kids.delta.sql}, ${sql})`;
    if (dataType === from.dataType) {
      return newTime;
    }
    return `${from.dataType.toUpperCase()}(${newTime})`;
  }

  sqlSumDistinct(_key: string, _value: string, _funcName: string): string {
    throw new Error('sqlAggDistinct: Not supported!');
  }

  sqlAggDistinct(
    _key: string,
    _values: string[],
    _func: (valNames: string[]) => string
  ): string {
    throw new Error('sqlAggDistinct: Not supported!');
  }

  sqlSampleTable(tableSQL: string, sample: Sampling | undefined): string {
    if (sample !== undefined) {
      if (isSamplingEnable(sample) && sample.enable) {
        sample = this.defaultSampling;
      }
      if (isSamplingRows(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE SYSTEM(${sample.rows}))`;
      } else if (isSamplingPercent(sample)) {
        return `(SELECT * FROM ${tableSQL} TABLESAMPLE SYSTEM (${sample.percent}))`;
      }
    }
    return tableSQL;
  }

  sqlOrderBy(orderTerms: string[]): string {
    return `ORDER BY ${orderTerms.map(t => `${t} NULLS LAST`).join(',')}`;
  }

  sqlLiteralString(literal: string): string {
    return "'" + literal.replace(/'/g, "''") + "'";
  }

  sqlLiteralRegexp(literal: string): string {
    return "'" + literal.replace(/'/g, "''") + "'";
  }

  getDialectFunctionOverrides(): {
    [name: string]: DialectFunctionOverloadDef[];
  } {
    return expandOverrideMap(IGNITES_MALLOY_STANDARD_OVERLOADS);
  }

  getDialectFunctions(): {[name: string]: DialectFunctionOverloadDef[]} {
    return expandBlueprintMap(IGNITES_DIALECT_FUNCTIONS);
  }

  malloyTypeToSQLType(malloyType: FieldAtomicTypeDef): string {
    if (malloyType.type === 'number') {
      if (malloyType.numberType === 'integer') {
        return 'INT';
      } else {
        return 'DOUBLE';
      }
    } else if (malloyType.type === 'string') {
      return 'VARCHAR';
    }
    return malloyType.type;
  }

  sqlTypeToMalloyType(sqlType: string): FieldAtomicTypeDef | undefined {
    // Remove trailing params
    // const baseSqlType = sqlType.match(/^([\w\s]+)/)?.at(0) ?? sqlType;
    return igniteToMalloyTypes[sqlType];
  }

  castToString(expression: string): string {
    return `CAST(${expression} as VARCHAR)`;
  }

  concat(...values: string[]): string {
    return values.join(' || ');
  }

  validateTypeName(sqlType: string): boolean {
    // Letters:              BIGINT
    // Numbers:              INT8
    // Spaces:               TIMESTAMP WITH TIME ZONE
    // Parentheses, Commas:  NUMERIC(5, 2)
    // Square Brackets:      INT64[]
    return sqlType.match(/^[A-Za-z\s(),[\]0-9]*$/) !== null;
  }

  sqlNowExpr(): string {
    return 'CURRENT_TIMESTAMP';
  }

  sqlTruncExpr(qi: QueryInfo, df: TimeTruncExpr): string {
    return `FLOOR(${df.e.sql} TO ${df.units})`;
  }

  sqlTimeExtractExpr(qi: QueryInfo, te: TimeExtractExpr): string {
    const extractTo = extractMap[te.units] || te.units;
    const tz = te.e.dataType === 'timestamp' && qtz(qi);
    const tzAdd = tz ? ` AT TIME ZONE '${tz}'` : '';
    return `EXTRACT(${extractTo} FROM ${te.e.sql}${tzAdd})`;
  }

  sqlRegexpMatch(df: RegexMatchExpr): string {
    return `${df.kids.expr.sql} ~ ${df.kids.regex.sql}`;
  }

  sqlLiteralTime(qi: QueryInfo, lit: TimeLiteralNode): string {
    if (lit.dataType === 'date') {
      return `CAST('${lit.literal}' AS TIMESTAMP)`;
    } else if (lit.dataType === 'timestamp') {
      const tz = lit.timezone || qtz(qi);
      if (tz && tz !== 'UTC') {
        return `CAST('${lit.literal} ${tz}' AS TIMESTAMP WITH LOCAL TIME ZONE)::TIMESTAMP`;
      }
      return `CAST('${lit.literal}' AS TIMESTAMP)`;
    } else {
      throw new Error(`Unsupported Literal time format ${lit.dataType}`);
    }
  }
}
