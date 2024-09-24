/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 *  LICENSE file in the root directory of this source tree.
 */

import {MalloyStandardFunctionImplementations as OverrideMap} from '../functions/malloy_standard_functions';

function greatestOrLeastSQL(name: string) {
  return `${name}(\${...values})`;
}

export const IGNITES_MALLOY_STANDARD_OVERLOADS: OverrideMap = {
  byte_length: {function: 'OCTET_LENGTH'},
  greatest: {sql: greatestOrLeastSQL('GREATEST')},
  least: {sql: greatestOrLeastSQL('LEAST')},
  ifnull: {sql: 'COALESCE(${value}, ${default})'},
  is_inf: {
    sql: "COALESCE(${value} = CAST('Infinity' AS DOUBLE) OR ${value} = CAST('-Infinity' AS DOUBLE), false)",
  },
  is_nan: {sql: "COALESCE(${value} = CAST('NaN' AS DOUBLE), false)"},
  regexp_extract: {
    expr: {
      node: 'error',
      message: 'regexp_extract is not supported',
    },
  },
  replace: {
    regular_expression: {
      sql: 'REGEXP_REPLACE(${value}, ${pattern}, ${replacement})',
    },
  },
  round: {
    to_integer: {sql: 'ROUND((${value})::NUMERIC)'},
    to_precision: {sql: 'ROUND((${value})::NUMERIC, ${precision})'},
  },
  stddev: {sql: 'STDDEV(${value}::DOUBLE PRECISION)'},
  substr: {
    position_only: {
      sql: 'SUBSTRING(${value}, CASE WHEN ${position} < 0 THEN LENGTH(${value}) + ${position} + 1 ELSE ${position} END)',
    },
    with_length: {
      sql: 'SUBSTRING(${value}, CASE WHEN ${position} < 0 THEN LENGTH(${value}) + ${position} + 1 ELSE ${position} END, ${length})',
    },
  },
  trunc: {
    to_integer: {
      sql: 'TRUNCATE(${value}::NUMERIC)',
    },
    to_precision: {
      sql: 'TRUNCATE((${value}::NUMERIC), ${precision})',
    },
  },
  unicode: {function: 'ASCII'},
  strpos: {sql: 'POSITION(${search_string} IN ${test_string})'},
  starts_with: {sql: "COALESCE((${value} LIKE ${prefix}||'%'), false)"},
  ends_with: {sql: "COALESCE((${value} LIKE '%'||${suffix}), false)"},
  pi: {sql: 'PI'},
  trim: {
    whitespace: {
      sql: "TRIM(' ' FROM ${value})",
    },
    characters: {
      sql: 'TRIM(${trim_characters} FROM ${value})',
    },
  },
  ltrim: {
    whitespace: {
      sql: "TRIM(LEADING ' ' FROM ${value})",
    },
    characters: {
      sql: 'TRIM(LEADING ${trim_characters} FROM ${value})',
    },
  },
  rtrim: {
    whitespace: {
      sql: "TRIM(TRAILING ' ' FROM ${value})",
    },
    characters: {
      sql: 'TRIM(TRAILING ${trim_characters} FROM ${value})',
    },
  },
  pow: {function: 'POWER'},
  div: {sql: 'TRUNCATE(${dividend} / ${divisor})'},
  log: {sql: '(LN(${value}) / LN(${base}))'},
};
