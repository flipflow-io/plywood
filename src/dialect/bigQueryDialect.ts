/*
 * Copyright 2015-2020 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { Duration, Timezone } from 'chronoshift';
import type { SqlBase, SqlExpression } from 'druid-query-toolkit';
import { NamedArray } from 'immutable-class';

import { Attributes } from '../datatypes';
import { PlyType } from '../types';

import { SQLDialect } from './baseDialect';

export interface BigQueryDialectOptions {
  attributes?: Attributes;
  lookupTable?: (lookup: string) => string;
}

// The results mirror DruidDialect: same values for the same Plywood expression on the same rows
export class BigQueryDialect extends SQLDialect {
  static TIME_TRUNC: Record<string, string> = {
    PT1S: 'SECOND',
    PT1M: 'MINUTE',
    PT1H: 'HOUR',
    P1D: 'DAY',
    P1W: 'ISOWEEK',
    P1M: 'MONTH',
    P3M: 'QUARTER',
    P1Y: 'YEAR',
  };

  // Druid's ISO day of week (Monday = 1); BigQuery DAYOFWEEK has Sunday = 1
  static ISO_DOW = 'MOD(EXTRACT(DAYOFWEEK FROM $$ AT TIME ZONE ##)+5,7)+1';

  static TIME_PART_TO_FUNCTION: Record<string, string> = {
    SECOND_OF_MINUTE: 'EXTRACT(SECOND FROM $$ AT TIME ZONE ##)',
    SECOND_OF_HOUR:
      '(EXTRACT(MINUTE FROM $$ AT TIME ZONE ##)*60+EXTRACT(SECOND FROM $$ AT TIME ZONE ##))',
    SECOND_OF_DAY:
      '((EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##))*60+EXTRACT(SECOND FROM $$ AT TIME ZONE ##))',
    SECOND_OF_WEEK:
      '(((MOD(DOW+6,7)*24)+EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##))*60+EXTRACT(SECOND FROM $$ AT TIME ZONE ##))',
    SECOND_OF_MONTH:
      '((((EXTRACT(DAY FROM $$ AT TIME ZONE ##)-1)*24)+EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##))*60+EXTRACT(SECOND FROM $$ AT TIME ZONE ##))',
    SECOND_OF_YEAR:
      '((((EXTRACT(DAYOFYEAR FROM $$ AT TIME ZONE ##)-1)*24)+EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##))*60+EXTRACT(SECOND FROM $$ AT TIME ZONE ##))',

    MINUTE_OF_HOUR: 'EXTRACT(MINUTE FROM $$ AT TIME ZONE ##)',
    MINUTE_OF_DAY:
      'EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##)',
    MINUTE_OF_WEEK:
      '(MOD(DOW+6,7)*24)+EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##)',
    MINUTE_OF_MONTH:
      '((EXTRACT(DAY FROM $$ AT TIME ZONE ##)-1)*24)+EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##)',
    MINUTE_OF_YEAR:
      '((EXTRACT(DAYOFYEAR FROM $$ AT TIME ZONE ##)-1)*24)+EXTRACT(HOUR FROM $$ AT TIME ZONE ##)*60+EXTRACT(MINUTE FROM $$ AT TIME ZONE ##)',

    HOUR_OF_DAY: 'EXTRACT(HOUR FROM $$ AT TIME ZONE ##)',
    HOUR_OF_WEEK: '(MOD(DOW+6,7)*24+EXTRACT(HOUR FROM $$ AT TIME ZONE ##))',
    HOUR_OF_MONTH:
      '((EXTRACT(DAY FROM $$ AT TIME ZONE ##)-1)*24+EXTRACT(HOUR FROM $$ AT TIME ZONE ##))',
    HOUR_OF_YEAR:
      '((EXTRACT(DAYOFYEAR FROM $$ AT TIME ZONE ##)-1)*24+EXTRACT(HOUR FROM $$ AT TIME ZONE ##))',

    DAY_OF_WEEK: 'MOD(DOW+6,7)+1',
    DAY_OF_MONTH: 'EXTRACT(DAY FROM $$ AT TIME ZONE ##)',
    DAY_OF_YEAR: 'EXTRACT(DAYOFYEAR FROM $$ AT TIME ZONE ##)',

    // WEEK_OF_MONTH: ???,
    WEEK_OF_YEAR: 'EXTRACT(ISOWEEK FROM $$ AT TIME ZONE ##)',

    MONTH_OF_YEAR: 'EXTRACT(MONTH FROM $$ AT TIME ZONE ##)',
    YEAR: 'EXTRACT(YEAR FROM $$ AT TIME ZONE ##)',
  };

  // Recorded as TO: FROM: FN
  static CAST_TO_FUNCTION: Record<string, Record<string, string>> = {
    TIME: {
      NUMBER: 'TIMESTAMP_MILLIS(CAST($$ AS INT64))',
      _: 'CAST($$ AS TIMESTAMP)',
    },
    NUMBER: {
      TIME: 'UNIX_MILLIS($$)',
      _: 'SAFE_CAST($$ AS FLOAT64)', // Druid gives NULL for text that is not a number
    },
    STRING: {
      TIME: "FORMAT_TIMESTAMP('%F %T', $$)", // Druid's text for a timestamp
      _: 'CAST($$ AS STRING)',
    },
    BOOLEAN: {
      NUMBER: '($$ = 1)',
      STRING: `($$ = 'true')`,
      _: `(CAST($$ AS STRING) IN ('1','true'))`,
    },
  };

  // Druid SQL types (in raw s${} formulas) → BigQuery types
  static RAW_SQL_TYPE: Record<string, string> = {
    DOUBLE: 'FLOAT64',
    FLOAT: 'FLOAT64',
    REAL: 'FLOAT64',
    DECIMAL: 'FLOAT64',
    BIGINT: 'INT64',
    INTEGER: 'INT64',
    INT: 'INT64',
    LONG: 'INT64',
    SMALLINT: 'INT64',
    TINYINT: 'INT64',
    VARCHAR: 'STRING',
    CHAR: 'STRING',
    STRING: 'STRING',
    TIMESTAMP: 'TIMESTAMP',
    DATE: 'DATE',
    BOOLEAN: 'BOOL',
  };

  // Druid SQL functions (in raw s${} formulas) with a direct BigQuery form; $1, $2… are the arguments
  static RAW_SQL_FUNCTION: Record<string, string> = {
    REGEXP_LIKE: 'REGEXP_CONTAINS(CAST($1 AS STRING), $2)',
    CONTAINS_STRING: 'STRPOS(CAST($1 AS STRING), $2)>0',
    ICONTAINS_STRING: 'STRPOS(LOWER(CAST($1 AS STRING)), LOWER($2))>0',
    STRLEN: 'CHAR_LENGTH($1)',
    LENGTH: 'CHAR_LENGTH($1)',
    CHAR_LENGTH: 'CHAR_LENGTH($1)',
    CHARACTER_LENGTH: 'CHAR_LENGTH($1)',
    NVL: 'IFNULL($1, $2)',
    TEXTCAT: 'CONCAT($1, $2)',
    STRING_TO_ARRAY: 'SPLIT($1, $2)',
    ARRAY_OFFSET: '$1[SAFE_OFFSET($2)]',
    ARRAY_ORDINAL: '$1[SAFE_ORDINAL($2)]',
    MILLIS_TO_TIMESTAMP: 'TIMESTAMP_MILLIS(CAST($1 AS INT64))',
    TIMESTAMP_TO_MILLIS: 'UNIX_MILLIS($1)',
    TRY_PARSE_JSON: 'SAFE.PARSE_JSON($1)',
    LATEST_BY: 'MAX_BY($1, $2)',
    EARLIEST_BY: 'MIN_BY($1, $2)',
    LATEST: 'MAX_BY($1, `__time`)',
    EARLIEST: 'MIN_BY($1, `__time`)',
    ANY_VALUE: 'ANY_VALUE($1)',
    APPROX_COUNT_DISTINCT: 'COUNT(DISTINCT $1)',
    APPROX_COUNT_DISTINCT_DS_HLL: 'COUNT(DISTINCT $1)',
    APPROX_COUNT_DISTINCT_DS_THETA: 'COUNT(DISTINCT $1)',
  };

  // Joda time format tokens (Druid TIME_FORMAT / TIME_PARSE) → BigQuery format elements
  static JODA_TO_STRFTIME: Record<string, string> = {
    yyyy: '%Y',
    yy: '%y',
    MMMM: '%B',
    MMM: '%b',
    MM: '%m',
    dd: '%d',
    HH: '%H',
    hh: '%I',
    mm: '%M',
    ss: '%S',
    SSS: '%E3S',
    EEEE: '%A',
    EEE: '%a',
    a: '%p',
    D: '%j',
  };

  private readonly attributes?: Attributes;
  private readonly lookupTable?: (lookup: string) => string;

  constructor(options: BigQueryDialectOptions = {}) {
    super();
    this.attributes = options.attributes;
    this.lookupTable = options.lookupTable;
  }

  public escapeName(name: string): string {
    name = name.replace(/\\/g, '\\\\').replace(/`/g, '\\`');
    return '`' + name + '`';
  }

  public escapeLiteral(name: string): string {
    if (name === null) return this.nullConstant();
    name = name
      .replace(/\\/g, '\\\\')
      .replace(/'/g, "\\'")
      .replace(/\n/g, '\\n')
      .replace(/\r/g, '\\r');
    return "'" + name + "'";
  }

  public dateToSQLDateString(date: Date): string {
    return date
      .toISOString()
      .replace('T', ' ')
      .replace('Z', '')
      .replace(/\.000$/, '');
  }

  public floatDivision(numerator: string, denominator: string): string {
    // Druid returns Infinity on a division by zero; a plain `/` fails the whole BigQuery job
    return `IEEE_DIVIDE(${numerator},${denominator})`;
  }

  public emptyGroupBy(): string {
    return 'GROUP BY ()';
  }

  public timeToSQL(date: Date): string {
    if (!date) return this.nullConstant();
    return `TIMESTAMP '${this.dateToSQLDateString(date)}'`;
  }

  public stringArrayToSQL(value: string[]): string {
    const arr = value.map((v: string) => this.escapeLiteral(v));
    return `[${arr.join(',')}]`;
  }

  public orderByExpression(expression: string, direction: string, type?: PlyType): string {
    // Druid's groupBy orders NULL lowest, as BigQuery does, but NaN highest where BigQuery puts it lowest
    if (type !== 'NUMBER') return `${expression} ${direction}`;
    return `IS_NAN(${expression}) IS TRUE ${direction}, ${expression} ${direction}`;
  }

  public concatExpression(a: string, b: string): string {
    return `(${a}||${b})`;
  }

  public containsExpression(a: string, b: string, insensitive: boolean): string {
    a = `CAST(${a} AS STRING)`;
    if (insensitive) {
      a = `LOWER(${a})`;
      b = `LOWER(${b})`;
    }
    return `STRPOS(${a},${b})>0`;
  }

  // Multi-value columns are ARRAY<STRUCT<v STRING>> so that NULL elements survive, as in Druid
  public mvContainsExpression(a: string, b: string[]): string {
    const distinct = b.filter((v, i) => b.indexOf(v) === i);
    return `((SELECT COUNT(DISTINCT v) FROM UNNEST(${a}) WHERE v IN UNNEST(${this.stringArrayToSQL(
      distinct,
    )}))=${distinct.length})`;
  }

  public mvFilterOnlyExpression(a: string, b: string[]): string {
    return `ARRAY(SELECT AS STRUCT v FROM UNNEST(${a}) WHERE v IN UNNEST(${this.stringArrayToSQL(
      b,
    )}))`;
  }

  public mvOverlapExpression(a: string, b: string[]): string {
    return `EXISTS(SELECT 1 FROM UNNEST(${a}) WHERE v IN UNNEST(${this.stringArrayToSQL(b)}))`;
  }

  public substrExpression(a: string, position: number, length: number): string {
    // Druid gives NULL when the start is past the end of the string, BigQuery ''
    return `IF(${position + 1}>CHAR_LENGTH(${a}),NULL,SUBSTR(${a},${position + 1},${length}))`;
  }

  public countDistinctExpression(a: string, _parameterAttributeName: string | undefined): string {
    // Exact for every native type: BigQuery has no theta or HLL columns to reproduce Druid's sketches
    return `COUNT(DISTINCT ${a})`;
  }

  public isNotDistinctFromExpression(a: string, b: string): string {
    const nullConst = this.nullConstant();
    if (a === nullConst) return `${b} IS ${nullConst}`;
    if (b === nullConst) return `${a} IS ${nullConst}`;
    return `(${a}=${b})`;
  }

  public filterAggregatorExpression(aggregate: string, whereFilter: string): string {
    if (whereFilter.indexOf('WHERE') === -1) return aggregate;
    throw new Error('filtered aggregators are not supported in BigQuery dialect');
  }

  public castExpression(
    inputType: PlyType | undefined,
    operand: string,
    targetType: string,
  ): string {
    if (targetType === 'SET/STRING') targetType = 'STRING';
    if (inputType === targetType) return operand;
    const castForTarget = BigQueryDialect.CAST_TO_FUNCTION[targetType];
    const castFunction = castForTarget
      ? castForTarget[inputType || '_'] || castForTarget['_']
      : undefined;
    if (!castFunction) {
      throw new Error(
        `unsupported cast from ${inputType || 'unknown'} to ${targetType} in BigQuery dialect`,
      );
    }
    return castFunction.replace(/\$\$/g, operand);
  }

  private timezoneSQL(timezone: Timezone): string {
    return this.escapeLiteral(timezone.toString());
  }

  public timeFloorExpression(operand: string, duration: Duration, timezone: Timezone): string {
    const tz = timezone.isUTC() ? '' : `, ${this.timezoneSQL(timezone)}`;
    const trunc = BigQueryDialect.TIME_TRUNC[duration.toString()];
    if (trunc) return `TIMESTAMP_TRUNC(${operand}, ${trunc}${tz})`;

    const { year, month, week, day, hour, minute, second } = duration.valueOf();
    if (week) throw new Error(`unsupported duration '${duration}' in BigQuery dialect`);
    if (year || month) {
      if (day || hour || minute || second) {
        throw new Error(`unsupported duration '${duration}' in BigQuery dialect`);
      }
      // Druid counts calendar periods from 1970-01-01 in the time zone
      const months = (year || 0) * 12 + (month || 0);
      const local = timezone.isUTC() ? operand : `DATETIME(${operand}${tz})`;
      const index = `((EXTRACT(YEAR FROM ${local})-1970)*12+EXTRACT(MONTH FROM ${local})-1)`;
      return `TIMESTAMP(DATETIME_ADD(DATETIME '1970-01-01 00:00:00', INTERVAL ${index}-MOD(${index},${months}) MONTH)${tz})`;
    }

    // Fixed-length buckets counted from 1970-01-01 00:00 in the time zone, as Druid does
    const seconds = (((day || 0) * 24 + (hour || 0)) * 60 + (minute || 0)) * 60 + (second || 0);
    return `TIMESTAMP_BUCKET(${operand}, INTERVAL ${seconds} SECOND, TIMESTAMP('1970-01-01 00:00:00'${tz}))`;
  }

  public timeBucketExpression(operand: string, duration: Duration, timezone: Timezone): string {
    return this.timeFloorExpression(operand, duration, timezone);
  }

  public timePartExpression(operand: string, part: string, timezone: Timezone): string {
    const timePartFunction = BigQueryDialect.TIME_PART_TO_FUNCTION[part];
    if (!timePartFunction) throw new Error(`unsupported part ${part} in BigQuery dialect`);
    return timePartFunction
      .replace(/DOW/g, `(${BigQueryDialect.ISO_DOW})`)
      .replace(/\$\$/g, operand)
      .replace(/##/g, this.timezoneSQL(timezone));
  }

  public timeShiftExpression(
    operand: string,
    duration: Duration,
    step: int,
    timezone: Timezone,
  ): string {
    if (step === 0) return operand;

    const sqlFn = step > 0 ? 'DATETIME_ADD' : 'DATETIME_SUB';
    const spans = duration.multiply(Math.abs(step)).valueOf();
    const tz = this.timezoneSQL(timezone);
    let local = `DATETIME(${operand}, ${tz})`;
    const units: [number, string][] = [
      [spans.year, 'YEAR'],
      [spans.month, 'MONTH'],
      [(spans.week || 0) * 7 + (spans.day || 0), 'DAY'],
      [spans.hour, 'HOUR'],
      [spans.minute, 'MINUTE'],
      [spans.second, 'SECOND'],
    ];
    for (const [amount, unit] of units) {
      if (amount) local = `${sqlFn}(${local}, INTERVAL ${amount} ${unit})`;
    }
    return `TIMESTAMP(${local}, ${tz})`;
  }

  public extractExpression(operand: string, regexp: string): string {
    return `REGEXP_EXTRACT(CAST(${operand} AS STRING), ${this.escapeLiteral(regexp)})`;
  }

  public regexpExpression(expression: string, regexp: string): string {
    return `REGEXP_CONTAINS(CAST(${expression} AS STRING), ${this.escapeLiteral(regexp)})`;
  }

  public indexOfExpression(str: string, substr: string): string {
    return `(STRPOS(${str},${substr})-1)`;
  }

  public quantileExpression(
    str: string,
    quantile: number,
    _parameterAttributeName: string | undefined,
  ): string {
    return `APPROX_QUANTILES(${str}, 1000)[OFFSET(${Math.round(quantile * 1000)})]`;
  }

  public logExpression(base: string, operand: string): string {
    if (base === String(Math.E)) return `LN(${operand})`;
    if (base === '10') return `LOG10(${operand})`;
    return `LOG(${operand},${base})`;
  }

  public lookupExpression(base: string, lookup: string): string {
    if (!this.lookupTable) throw new Error(`no table for lookup '${lookup}' in BigQuery dialect`);
    // Lookups are key/value tables (k, v); a missing key gives NULL, as in Druid
    return `(SELECT v FROM ${this.escapeName(this.lookupTable(lookup))} WHERE k = ${base})`;
  }

  public modeExpression(_expressionSQL: string, _operandSQL: string): string {
    throw new Error(
      'MODE has no inline SQL form in BigQuery; it must be lowered by SQLExternal (ROW_NUMBER decomposition or scalar subquery)',
    );
  }

  public collectExpression(expressionSQL: string): string {
    return `STRING_AGG(DISTINCT CAST(${expressionSQL} AS STRING), ', ' ORDER BY CAST(${expressionSQL} AS STRING))`;
  }

  // -----------------
  // Raw Druid SQL (cube formulas written as s${…}), rendered from its parse tree

  public translateRawSql(parsedSql: SqlExpression, _sql: string): string {
    return this.rawSql(parsedSql);
  }

  private rawSqlType(name: string): string {
    const type = BigQueryDialect.RAW_SQL_TYPE[name.toUpperCase()];
    if (!type) throw new Error(`unsupported type '${name}' in BigQuery dialect`);
    return type;
  }

  private rawSqlColumnType(node: SqlBase): PlyType | undefined {
    if (node.type !== 'column') return undefined;
    const name = (node as any).getName();
    if (name === '__time') return 'TIME';
    const attribute = NamedArray.findByName(this.attributes || [], name);
    return attribute ? attribute.type : undefined;
  }

  private rawSqlIsNumberLiteral(node: SqlBase): boolean {
    return node.type === 'literal' && typeof (node as any).value === 'number';
  }

  private jodaToStrftime(pattern: string): string {
    let out = '';
    let i = 0;
    while (i < pattern.length) {
      const c = pattern[i];
      if (c === "'") {
        const end = pattern.indexOf("'", i + 1);
        out += end === -1 ? pattern.slice(i + 1) : pattern.slice(i + 1, end);
        i = end === -1 ? pattern.length : end + 1;
      } else if (/[A-Za-z]/.test(c)) {
        let j = i;
        while (j < pattern.length && pattern[j] === c) j++;
        const token = pattern.slice(i, j);
        const element = BigQueryDialect.JODA_TO_STRFTIME[token];
        if (!element) throw new Error(`unsupported time format '${token}' in BigQuery dialect`);
        out += element;
        i = j;
      } else {
        out += c === '%' ? '%%' : c;
        i++;
      }
    }
    return out;
  }

  /**
   * A Druid JSONPath in BigQuery's syntax: `['key']` becomes `."key"`. Null when Druid can not read the
   * path (it does not read `."key"`), undefined when the path is not one of the simple forms.
   */
  private jsonPath(path: string): string | null | undefined {
    if (!path.startsWith('$')) return undefined;
    let out = '$';
    let i = 1;
    while (i < path.length) {
      const rest = path.slice(i);
      let m: RegExpMatchArray | null;
      if ((m = rest.match(/^\.([A-Za-z_][A-Za-z0-9_]*)/))) {
        out += '.' + m[1];
      } else if ((m = rest.match(/^\[(\d+)\]/))) {
        out += '[' + m[1] + ']';
      } else if ((m = rest.match(/^\['([^']*)'\]/)) || (m = rest.match(/^\["([^"]*)"\]/))) {
        out += /^[A-Za-z_][A-Za-z0-9_]*$/.test(m[1]) ? '.' + m[1] : '."' + m[1] + '"';
      } else if (rest.startsWith('."')) {
        return null;
      } else {
        return undefined;
      }
      i += m[0].length;
    }
    return out;
  }

  private rawSqlLiteral(node: any): string {
    const { value } = node;
    if (value === null) return 'NULL';
    if (value === true || value === false) return String(value).toUpperCase();
    if (typeof value === 'number' || typeof value === 'bigint') {
      return node.stringValue || String(value);
    }
    if (value instanceof Date) return this.timeToSQL(value);
    if (typeof value === 'string') return this.escapeLiteral(value);
    throw new Error(`unsupported literal '${node}' in BigQuery dialect`);
  }

  private rawSqlComparisonSide(node: SqlBase, other: SqlBase): string {
    // Druid compares text with a number as numbers ('01' = 1 is true); BigQuery refuses the types
    if (this.rawSqlColumnType(node) === 'STRING' && this.rawSqlIsNumberLiteral(other)) {
      return `SAFE_CAST(${this.rawSql(node)} AS FLOAT64)`;
    }
    return this.rawSql(node, true);
  }

  private rawSqlComparison(node: any): string {
    const { op, lhs, rhs } = node;
    switch (op) {
      case 'IN':
      case 'NOT IN': {
        if (rhs.type !== 'record') throw new Error(`unsupported IN '${node}' in BigQuery dialect`);
        const values: SqlBase[] = rhs.expressions ? rhs.expressions.values : [];
        const numberValues = values.some((v: SqlBase) => this.rawSqlIsNumberLiteral(v));
        const left =
          numberValues && this.rawSqlColumnType(lhs) === 'STRING'
            ? `SAFE_CAST(${this.rawSql(lhs)} AS FLOAT64)`
            : this.rawSql(lhs, true);
        return `${left} ${op} (${values.map((v: SqlBase) => this.rawSql(v)).join(', ')})`;
      }

      case 'LIKE':
      case 'NOT LIKE': {
        // BigQuery reads \ in a LIKE pattern as an escape; Druid has no default escape
        const pattern =
          rhs.type === 'literal' && typeof rhs.value === 'string'
            ? this.escapeLiteral(rhs.value.replace(/\\/g, '\\\\'))
            : this.rawSql(rhs, true);
        return `${this.rawSql(lhs, true)} ${op} ${pattern}`;
      }

      case 'BETWEEN':
      case 'NOT BETWEEN':
        return `${this.rawSql(lhs, true)} ${op} ${this.rawSql(rhs.start, true)} AND ${this.rawSql(
          rhs.end,
          true,
        )}`;

      case 'IS':
      case 'IS NOT':
      case 'IS DISTINCT FROM':
      case 'IS NOT DISTINCT FROM':
        return `${this.rawSql(lhs, true)} ${op} ${this.rawSql(rhs, true)}`;

      default:
        return `${this.rawSqlComparisonSide(lhs, rhs)} ${op} ${this.rawSqlComparisonSide(
          rhs,
          lhs,
        )}`;
    }
  }

  private rawSqlCast(arg: SqlBase, typeNode: any): string {
    const type = this.rawSqlType(typeNode.value);
    const argType = this.rawSqlColumnType(arg);
    const argSQL = this.rawSql(arg);
    if (argType === 'TIME') {
      if (type === 'INT64') return `UNIX_MILLIS(${argSQL})`;
      if (type === 'STRING') return `FORMAT_TIMESTAMP('%F %T', ${argSQL})`;
    }
    if (argType === 'NUMBER' && type === 'TIMESTAMP') {
      return `TIMESTAMP_MILLIS(CAST(${argSQL} AS INT64))`;
    }
    // Druid truncates toward zero and gives NULL for text that is not a number
    if (type === 'INT64') return `SAFE_CAST(TRUNC(SAFE_CAST(${argSQL} AS FLOAT64)) AS INT64)`;
    if (type === 'STRING') return `CAST(${argSQL} AS STRING)`;
    return `SAFE_CAST(${argSQL} AS ${type})`;
  }

  private rawSqlFunction(node: any): string {
    const name = node.getEffectiveFunctionName();
    const args: SqlBase[] = node.getArgArray();
    const decorator = node.getEffectiveDecorator();
    if (node.windowSpec) throw new Error(`unsupported window '${node}' in BigQuery dialect`);

    switch (name) {
      case 'CAST':
        return this.rawSqlCast(args[0], args[1]);

      case 'TRIM': {
        const trimFn =
          decorator === 'LEADING' ? 'LTRIM' : decorator === 'TRAILING' ? 'RTRIM' : 'TRIM';
        if (args.length === 1) return `${trimFn}(${this.rawSql(args[0])})`;
        return `${trimFn}(${this.rawSql(args[1])}, ${this.rawSql(args[0])})`;
      }

      case 'SUBSTR':
      case 'SUBSTRING': {
        // Druid gives NULL when the start is past the end of the string, BigQuery ''
        const str = this.rawSql(args[0]);
        const start = this.rawSql(args[1]);
        const rest = args.slice(1).map(a => this.rawSql(a));
        return `IF(${start}>CHAR_LENGTH(${str}),NULL,SUBSTR(${str},${rest.join(',')}))`;
      }

      case 'POSITION':
        return `STRPOS(${this.rawSql(args[1])}, ${this.rawSql(args[0])})`;

      case 'LOOKUP': {
        const lookupName = (args[1] as any).value;
        const looked = this.lookupExpression(this.rawSql(args[0]), lookupName);
        return args[2] ? `COALESCE(${looked}, ${this.rawSql(args[2])})` : looked;
      }

      case 'TIME_FORMAT': {
        const format =
          args[1] && (args[1] as any).value != null
            ? this.jodaToStrftime((args[1] as any).value)
            : '%Y-%m-%dT%H:%M:%E3SZ';
        const tz = args[2] ? `, ${this.rawSql(args[2])}` : '';
        return `FORMAT_TIMESTAMP(${this.escapeLiteral(format)}, ${this.rawSql(args[0])}${tz})`;
      }

      case 'TIME_PARSE': {
        const format = args[1] ? this.jodaToStrftime((args[1] as any).value) : null;
        const tz = args[2] ? `, ${this.rawSql(args[2])}` : '';
        return format
          ? `SAFE.PARSE_TIMESTAMP(${this.escapeLiteral(format)}, ${this.rawSql(args[0])}${tz})`
          : `SAFE_CAST(${this.rawSql(args[0])} AS TIMESTAMP)`;
      }

      case 'TIMESTAMPDIFF':
        return `TIMESTAMP_DIFF(${this.rawSql(args[2])}, ${this.rawSql(args[1])}, ${String(
          (args[0] as any).value || args[0],
        ).toUpperCase()})`;

      case 'REGEXP_EXTRACT':
        return `REGEXP_EXTRACT(CAST(${this.rawSql(args[0])} AS STRING), ${this.rawSql(args[1])})`;

      case 'REGEXP_REPLACE': {
        // Druid writes group references as $1, BigQuery as \1
        const replacement =
          args[2] && args[2].type === 'literal'
            ? this.escapeLiteral((args[2] as any).value.replace(/\$(\d)/g, '\\$1'))
            : this.rawSql(args[2]);
        return `REGEXP_REPLACE(${this.rawSql(args[0])}, ${this.rawSql(args[1])}, ${replacement})`;
      }

      case 'JSON_VALUE':
      case 'JSON_QUERY': {
        const path = args[1] && args[1].type === 'literal' ? (args[1] as any).value : undefined;
        const bigQueryPath = typeof path === 'string' ? this.jsonPath(path) : undefined;
        // Druid does not read `$."quoted key"` paths and returns NULL
        if (bigQueryPath === null) return 'CAST(NULL AS STRING)';
        const pathSQL = bigQueryPath ? this.escapeLiteral(bigQueryPath) : this.rawSql(args[1]);
        const sql = `${name}(${this.rawSql(args[0])}, ${pathSQL})`;
        // JSON can not be grouped on in BigQuery; Druid gives the text of the document, NULL for a JSON null
        return name === 'JSON_QUERY' ? `NULLIF(TO_JSON_STRING(${sql}), 'null')` : sql;
      }
    }

    const template = BigQueryDialect.RAW_SQL_FUNCTION[name];
    if (template) {
      return template.replace(/\$(\d)/g, (_, i) => this.rawSql(args[Number(i) - 1]));
    }

    const argSQLs = args.map(a => this.rawSql(a));
    if (node.whereClause) {
      // BigQuery has no FILTER (WHERE …): the filter goes inside the aggregate
      const condition = this.rawSql(node.whereClause.expression);
      if (name === 'COUNT' && (!args.length || args[0].type === 'star')) {
        return `COUNTIF(${condition})`;
      }
      return `${name}(${decorator ? decorator + ' ' : ''}IF(${condition}, ${argSQLs[0]}, NULL))`;
    }
    return `${name}(${decorator ? decorator + ' ' : ''}${argSQLs.join(', ')})`;
  }

  private rawSql(node: SqlBase, nested = false): string {
    const n = node as any;
    switch (node.type) {
      case 'literal':
        return this.rawSqlLiteral(n);

      case 'column': {
        const table = n.getTableName();
        return (table ? this.escapeName(table) + '.' : '') + this.escapeName(n.getName());
      }

      case 'star':
        return '*';

      case 'type':
        return this.rawSqlType(n.value);

      case 'record':
        return `(${(n.expressions ? n.expressions.values : [])
          .map((a: SqlBase) => this.rawSql(a))
          .join(', ')})`;

      case 'function':
        return this.rawSqlFunction(n);

      case 'comparison': {
        const sql = this.rawSqlComparison(n);
        return nested ? `(${sql})` : sql;
      }

      case 'multi': {
        const args: SqlBase[] = n.getArgArray();
        let sql: string;
        if (n.op === '/') {
          sql = args
            .map(a => this.rawSql(a, true))
            .reduce((acc: string, a: string) => this.floatDivision(acc, a));
        } else {
          sql = args.map(a => this.rawSql(a, true)).join(` ${n.op} `);
        }
        return nested ? `(${sql})` : sql;
      }

      case 'unary': {
        const sql = `${n.op === 'NOT' ? 'NOT ' : n.op}${this.rawSql(n.argument, true)}`;
        return nested ? `(${sql})` : sql;
      }

      case 'case': {
        const parts = [
          'CASE',
          n.caseExpression ? this.rawSql(n.caseExpression, true) : '',
          ...n.whenThenParts.values.map(
            (part: any) =>
              `WHEN ${part.whenExpressions.values
                .map((w: SqlBase) => this.rawSql(w))
                .join(', ')} THEN ${this.rawSql(part.thenExpression)}`,
          ),
          n.elseExpression ? `ELSE ${this.rawSql(n.elseExpression)}` : '',
          'END',
        ];
        return parts.filter(Boolean).join(' ');
      }

      default:
        throw new Error(`unsupported SQL '${node}' in BigQuery dialect`);
    }
  }
}
