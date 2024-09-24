/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 *  LICENSE file in the root directory of this source tree.
 */

export default function compactMap<T, U>(
  array: T[],
  fn: (item: T, index: number) => U | undefined
): U[] {
  const result: U[] = [];
  for (let i = 0; i < array.length; i++) {
    const value = fn(array[i], i);
    if (value !== undefined) {
      result.push(value);
    }
  }
  return result;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function toJson(value: any): any {
  if (!Array.isArray(value)) {
    return value;
  }
  value = value.filter((v: string) => v !== null);
  if (
    value.length > 0 &&
    typeof value[0] === 'string' &&
    value[0].startsWith('{') &&
    value[0].endsWith('}')
  ) {
    value = compactMap(value, (v: string | null) => {
      // eslint-disable-next-line eqeqeq
      if (v == null) {
        return undefined;
      }
      const val = JSON.parse(v);
      if (typeof val === 'object' && val !== null) {
        Object.keys(val).forEach(key => {
          if (Array.isArray(val[key])) {
            val[key] = toJson(val[key]);
          }
        });
      }
      return val;
    });
  }
  return value;
}
