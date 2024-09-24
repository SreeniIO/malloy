/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 *  LICENSE file in the root directory of this source tree.
 */

import {IgniteConnection} from './ignite_connection';
import {SQLBlock} from '@malloydata/malloy';
import {describeIfDatabaseAvailable} from '@malloydata/malloy/test';

const [describe] = describeIfDatabaseAvailable(['ignite']);

/*
 * !IMPORTANT
 *
 * The connection is reused for each test, so if you do not name your tables
 * and keys uniquely for each test you will see cross test interactions.
 */

describe('IgniteConnection', () => {
  let connection: IgniteConnection;
  let getTableSchema: jest.SpyInstance;
  let getSQLBlockSchema: jest.SpyInstance;

  beforeAll(async () => {
    connection = new IgniteConnection(
      'ignite',
      {},
      {endPoint: 'localhost:10800', defaultCacheName: 'SQL_PUBLIC_DEFAULT'}
    );
    await connection.runSQL('SELECT 1');
  });

  afterAll(async () => {
    await connection.close();
  });

  beforeEach(async () => {
    getTableSchema = jest
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .spyOn(IgniteConnection.prototype as any, 'getTableSchema')
      .mockResolvedValue({
        type: 'struct',
        dialect: 'ignite',
        name: 'name',
        structSource: {type: 'table', tablePath: 'test'},
        structRelationship: {
          type: 'basetable',
          connectionName: 'ignite',
        },
        fields: [],
      });

    getSQLBlockSchema = jest
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .spyOn(IgniteConnection.prototype as any, 'getSQLBlockSchema')
      .mockResolvedValue({
        type: 'struct',
        dialect: 'ignite',
        name: 'name',
        structSource: {
          type: 'sql',
          method: 'subquery',
          sqlBlock: SQL_BLOCK_1,
        },
        structRelationship: {
          type: 'basetable',
          connectionName: 'ignite',
        },
        fields: [],
      });
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('caches table schema', async () => {
    await connection.fetchSchemaForTables({'test1': 'table1'}, {});
    expect(getTableSchema).toBeCalledTimes(1);
    await connection.fetchSchemaForTables({'test1': 'table1'}, {});
    expect(getTableSchema).toBeCalledTimes(1);
  });

  it('refreshes table schema', async () => {
    await connection.fetchSchemaForTables({'test2': 'table2'}, {});
    expect(getTableSchema).toBeCalledTimes(1);
    await connection.fetchSchemaForTables(
      {'test2': 'table2'},
      {refreshTimestamp: Date.now() + 10}
    );
    expect(getTableSchema).toBeCalledTimes(2);
  });

  it('caches sql schema', async () => {
    await connection.fetchSchemaForSQLBlock(SQL_BLOCK_1, {});
    expect(getSQLBlockSchema).toBeCalledTimes(1);
    await connection.fetchSchemaForSQLBlock(SQL_BLOCK_1, {});
    expect(getSQLBlockSchema).toBeCalledTimes(1);
  });

  it('refreshes sql schema', async () => {
    await connection.fetchSchemaForSQLBlock(SQL_BLOCK_2, {});
    expect(getSQLBlockSchema).toBeCalledTimes(1);
    await connection.fetchSchemaForSQLBlock(SQL_BLOCK_2, {
      refreshTimestamp: Date.now() + 10,
    });
    expect(getSQLBlockSchema).toBeCalledTimes(2);
  });
});

const SQL_BLOCK_1 = {
  type: 'sqlBlock',
  name: 'block1',
  selectStr: `
SELECT
created_at,
sale_price,
inventory_item_id
FROM 'order_items.parquet'
SELECT
id,
product_department,
product_category,
created_at AS inventory_items_created_at
FROM "inventory_items.parquet"
`,
} as SQLBlock;

const SQL_BLOCK_2 = {
  type: 'sqlBlock',
  name: 'block2',
  selectStr: `
SELECT
created_at,
sale_price,
inventory_item_id
FROM read_parquet('order_items2.parquet', arg='value')
SELECT
id,
product_department,
product_category,
created_at AS inventory_items_created_at
FROM read_parquet("inventory_items2.parquet")
`,
} as SQLBlock;
