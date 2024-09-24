/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 *  LICENSE file in the root directory of this source tree.
 */

import * as crypto from 'crypto';
import {
  Connection,
  ConnectionConfig,
  FetchSchemaOptions,
  MalloyQueryData,
  NamedStructDefs,
  PersistSQLResults,
  PooledConnection,
  IgniteDialect,
  QueryData,
  QueryDataRow,
  QueryOptionsReader,
  QueryRunStats,
  RunSQLOptions,
  SQLBlock,
  StreamingConnection,
  StructDef,
} from '@malloydata/malloy';
import {BaseConnection} from '@malloydata/malloy/connection';

import {randomUUID} from 'crypto';
import IgniteClient from 'apache-ignite-client';
import {toJson} from './utils';
const {IgniteClientConfiguration, CacheConfiguration, SqlFieldsQuery} =
  IgniteClient;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type IgniteCache = any;

interface IgniteConnectionConfiguration {
  endPoint?: string;
  defaultCacheName?: string;
}

type IgniteConnectionConfigurationReader =
  | IgniteConnectionConfiguration
  | (() => Promise<IgniteConnectionConfiguration>);

const DEFAULT_PAGE_SIZE = 1000;
const SCHEMA_PAGE_SIZE = 1000;

export interface IgniteConnectionOptions
  extends ConnectionConfig,
    IgniteConnectionConfiguration {}

export class IgniteConnection
  extends BaseConnection
  implements Connection, StreamingConnection, PersistSQLResults
{
  public readonly name: string;
  private queryOptionsReader: QueryOptionsReader = {};
  private configReader: IgniteConnectionConfigurationReader = {};

  private readonly dialect = new IgniteDialect();
  private schemaCache = new Map<
    string,
    | {schema: StructDef; error?: undefined; timestamp: number}
    | {error: string; schema?: undefined; timestamp: number}
  >();
  private sqlSchemaCache = new Map<
    string,
    | {structDef: StructDef; error?: undefined; timestamp: number}
    | {error: string; structDef?: undefined; timestamp: number}
  >();

  constructor(
    options: IgniteConnectionOptions,
    queryOptionsReader?: QueryOptionsReader
  );
  constructor(
    name: string,
    queryOptionsReader?: QueryOptionsReader,
    configReader?: IgniteConnectionConfigurationReader
  );
  constructor(
    arg: string | IgniteConnectionOptions,
    queryOptionsReader?: QueryOptionsReader,
    configReader?: IgniteConnectionConfigurationReader
  ) {
    super();
    if (typeof arg === 'string') {
      this.name = arg;
      if (configReader) {
        this.configReader = configReader;
      }
    } else {
      const {name, ...configReader} = arg;
      this.name = name;
      this.configReader = configReader;
    }
    if (queryOptionsReader) {
      this.queryOptionsReader = queryOptionsReader;
    }
  }

  private async readQueryConfig(): Promise<RunSQLOptions> {
    if (this.queryOptionsReader instanceof Function) {
      return this.queryOptionsReader();
    } else {
      return this.queryOptionsReader;
    }
  }

  protected async readConfig(): Promise<IgniteConnectionConfiguration> {
    if (this.configReader instanceof Function) {
      return this.configReader();
    } else {
      return this.configReader;
    }
  }

  get dialectName(): string {
    return 'ignite';
  }

  public isPool(): this is PooledConnection {
    return false;
  }

  public canPersist(): this is PersistSQLResults {
    return true;
  }

  public canStream(): this is StreamingConnection {
    return true;
  }

  public get supportsNesting(): boolean {
    return false;
  }

  public async fetchSchemaForTables(
    missing: Record<string, string>,
    {refreshTimestamp}: FetchSchemaOptions
  ): Promise<{
    schemas: Record<string, StructDef>;
    errors: Record<string, string>;
  }> {
    const schemas: NamedStructDefs = {};
    const errors: {[name: string]: string} = {};

    for (const tableKey in missing) {
      let inCache = this.schemaCache.get(tableKey);
      if (
        !inCache ||
        (refreshTimestamp && refreshTimestamp > inCache.timestamp)
      ) {
        const tablePath = missing[tableKey];
        const timestamp = refreshTimestamp || Date.now();
        try {
          inCache = {
            schema: await this.getTableSchema(tableKey, tablePath),
            timestamp,
          };
          this.schemaCache.set(tableKey, inCache);
        } catch (error) {
          inCache = {error: error.message, timestamp};
        }
      }
      if (inCache.schema !== undefined) {
        schemas[tableKey] = inCache.schema;
      } else {
        errors[tableKey] = inCache.error || 'Unknown schema fetch error';
      }
    }
    return {schemas, errors};
  }

  public async fetchSchemaForSQLBlock(
    sqlRef: SQLBlock,
    {refreshTimestamp}: FetchSchemaOptions
  ): Promise<
    | {structDef: StructDef; error?: undefined}
    | {error: string; structDef?: undefined}
  > {
    const key = sqlRef.name;
    let inCache = this.sqlSchemaCache.get(key);
    if (
      !inCache ||
      (refreshTimestamp && refreshTimestamp > inCache.timestamp)
    ) {
      const timestamp = refreshTimestamp ?? Date.now();
      try {
        inCache = {
          structDef: await this.getSQLBlockSchema(sqlRef),
          timestamp,
        };
      } catch (error) {
        inCache = {error: error.message, timestamp};
      }
      this.sqlSchemaCache.set(key, inCache);
    }
    return inCache;
  }

  protected async getClient(): Promise<[IgniteClient, IgniteCache]> {
    const {endPoint, defaultCacheName} = await this.readConfig();
    const igniteClient = new IgniteClient();
    await igniteClient.connect(new IgniteClientConfiguration(endPoint));
    const cache = await igniteClient.getOrCreateCache(
      defaultCacheName,
      new CacheConfiguration().setSqlSchema('PUBLIC')
    );
    return [igniteClient, cache];
  }

  protected async runIgniteQuery(
    sqlCommand: string,
    _pageSize: number,
    _rowIndex: number,
    _deJSON: boolean
  ): Promise<MalloyQueryData> {
    const [client, cache] = await this.getClient();
    // await this.connectionSetup(client, cache);

    const query = new SqlFieldsQuery(sqlCommand);
    query.setIncludeFieldNames(true);
    const cursor = await cache.query(query);
    const fieldNames = cursor.getFieldNames();
    const result = await cursor.getAll();
    const rows = result.map(row => {
      const obj: Record<string, unknown> = {};
      for (let i = 0; i < fieldNames.length; i++) {
        let value = row[i];
        value = toJson(value);
        obj[fieldNames[i]] = value;
      }
      return obj;
    });
    await client.disconnect();
    return {
      rows: rows as QueryData,
      totalRows: rows.length,
    };
  }

  private async getSQLBlockSchema(sqlRef: SQLBlock): Promise<StructDef> {
    const structDef: StructDef = {
      type: 'struct',
      dialect: 'ignite',
      name: sqlRef.name,
      structSource: {
        type: 'sql',
        method: 'subquery',
        sqlBlock: sqlRef,
      },
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };

    const tempTableName = `tmp${randomUUID()}`.replace(/-/g, '');
    const infoQuery = `
      drop table if exists ${tempTableName};
      create temp table ${tempTableName} as SELECT * FROM (
        ${sqlRef.selectStr}
      ) as x where false;
      SELECT /*+ QUERY_ENGINE('h2') */ COLUMN_NAME, TYPE
      FROM SYS.TABLE_COLUMNS WHERE TABLE_NAME = '${tempTableName}'
      AND COLUMN_NAME NOT IN ('_KEY', '_VAL');`;
    try {
      await this.schemaFromQuery(infoQuery, structDef);
    } catch (error) {
      throw new Error(`Error fetching schema for ${sqlRef.name}: ${error}`);
    }
    return structDef;
  }

  private async schemaFromQuery(
    infoQuery: string,
    structDef: StructDef
  ): Promise<void> {
    const {rows, totalRows} = await this.runIgniteQuery(
      infoQuery,
      SCHEMA_PAGE_SIZE,
      0,
      false
    );
    if (!totalRows) {
      throw new Error('Unable to read schema.');
    }
    for (const row of rows) {
      const igniteDataType = row['TYPE'] as string;
      let s = structDef;
      let malloyType = this.dialect.sqlTypeToMalloyType(igniteDataType);
      let name = row['COLUMN_NAME'] as string;
      if (igniteDataType === 'ARRAY') {
        malloyType = this.dialect.sqlTypeToMalloyType(row['TYPE'] as string);
        s = {
          type: 'struct',
          name: row['COLUMN_NAME'] as string,
          dialect: this.dialectName,
          structRelationship: {
            type: 'nested',
            fieldName: name,
            isArray: true,
          },
          structSource: {type: 'nested'},
          fields: [],
        };
        structDef.fields.push(s);
        name = 'value';
      }
      if (malloyType) {
        s.fields.push({...malloyType, name});
      } else {
        s.fields.push({
          type: 'sql native',
          rawType: igniteDataType.toLowerCase(),
          name,
        });
      }
    }
  }

  private async getTableSchema(
    tableKey: string,
    tablePath: string
  ): Promise<StructDef> {
    const structDef: StructDef = {
      type: 'struct',
      name: tableKey,
      dialect: 'ignite',
      structSource: {type: 'table', tablePath},
      structRelationship: {
        type: 'basetable',
        connectionName: this.name,
      },
      fields: [],
    };
    const [schema, table] = tablePath.split('.');
    if (table === undefined) {
      throw new Error('Default schema not yet supported in Ignite');
    }
    const infoQuery = `SELECT /*+ QUERY_ENGINE('h2') */ COLUMN_NAME, TYPE
      FROM SYS.TABLE_COLUMNS WHERE TABLE_NAME = '${table.toUpperCase()}' AND SCHEMA_NAME = '${schema.toUpperCase()}'
      AND COLUMN_NAME NOT IN ('_KEY', '_VAL')`;

    try {
      await this.schemaFromQuery(infoQuery, structDef);
    } catch (error) {
      throw new Error(`Error fetching schema for ${tablePath}: ${error}`);
    }
    return structDef;
  }

  public async test(): Promise<void> {
    await this.runSQL('SELECT 1');
  }

  public async connectionSetup(
    _client: IgniteClient,
    _cache: IgniteCache
  ): Promise<void> {
    // await cache.query("SET TIME ZONE 'UTC'");
  }

  public async runSQL(
    sql: string,
    {rowLimit}: RunSQLOptions = {},
    rowIndex = 0
  ): Promise<MalloyQueryData> {
    const config = await this.readQueryConfig();

    return this.runIgniteQuery(
      sql,
      rowLimit ?? config.rowLimit ?? DEFAULT_PAGE_SIZE,
      rowIndex,
      true
    );
  }

  public async *runSQLStream(
    sqlCommand: string,
    {rowLimit, abortSignal}: RunSQLOptions = {}
  ): AsyncIterableIterator<QueryDataRow> {
    const [client, cache] = await this.getClient();
    const query = new SqlFieldsQuery(sqlCommand);
    const cursor = cache.query(query);
    let index = 0;
    for (const row of await cursor.getAll()) {
      yield row as QueryDataRow;
      index += 1;
      if (
        (rowLimit !== undefined && index >= rowLimit) ||
        abortSignal?.aborted
      ) {
        // query.destroy();
        break;
      }
    }
    await client.disconnect();
  }

  public async estimateQueryCost(_: string): Promise<QueryRunStats> {
    return {};
  }

  public async manifestTemporaryTable(sqlCommand: string): Promise<string> {
    const hash = crypto.createHash('md5').update(sqlCommand).digest('hex');
    const tableName = `tt${hash}`;

    const cmd = `CREATE TEMPORARY TABLE IF NOT EXISTS ${tableName} AS (${sqlCommand});`;
    // console.log(cmd);
    await this.runIgniteQuery(cmd, 1000, 0, false);
    return tableName;
  }

  async close(): Promise<void> {
    return;
  }
}
