/*
 * Copyright 2016-2020 Imply Data, Inc.
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

import { SqlExpression } from 'druid-query-toolkit';

import { PlywoodValue } from '../datatypes';
import { SQLDialect } from '../dialect/baseDialect';

import { ChainableExpression, Expression, ExpressionJS, ExpressionValue } from './baseExpression';
import { Aggregate } from './mixins/aggregate';

export class SqlAggregateExpression extends ChainableExpression {
  static op = 'SqlAggregate';

  static KNOWN_AGGREGATIONS = [
    'COUNT',
    'SUM',
    'MIN',
    'MAX',
    'AVG',
    'APPROX_COUNT_DISTINCT',
    'APPROX_COUNT_DISTINCT_DS_HLL',
    'APPROX_COUNT_DISTINCT_DS_THETA',
    'DS_HLL',
    'DS_THETA',
    'APPROX_QUANTILE',
    'APPROX_QUANTILE_DS',
    'APPROX_QUANTILE_FIXED_BUCKETS',
    'DS_QUANTILES_SKETCH',
    'BLOOM_FILTER',
    'TDIGEST_QUANTILE',
    'TDIGEST_GENERATE_SKETCH',
    'VAR_POP',
    'VAR_SAMP',
    'VARIANCE',
    'STDDEV_POP',
    'STDDEV_SAMP',
    'STDDEV',
    'EARLIEST',
    'LATEST',
    'ANY_VALUE',
  ];

  static registerKnownAggregation(aggregation: string): void {
    if (SqlAggregateExpression.KNOWN_AGGREGATIONS.includes(aggregation)) return;
    SqlAggregateExpression.KNOWN_AGGREGATIONS.push(aggregation);
  }

  static substituteFilter(sqlExpression: SqlExpression, condition: SqlExpression): SqlExpression {
    return sqlExpression.addFilterToAggregations(
      condition,
      SqlAggregateExpression.KNOWN_AGGREGATIONS,
    );
  }

  static fromJS(parameters: ExpressionJS): SqlAggregateExpression {
    const value = ChainableExpression.jsToValue(parameters);
    value.sql = parameters.sql;
    return new SqlAggregateExpression(value);
  }

  public sql: string;
  public parsedSql: SqlExpression;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this.sql = parameters.sql;
    this._ensureOp('sqlAggregate');
    this._checkOperandTypes('DATASET');
    this.type = 'NUMBER';

    this.parsedSql = SqlExpression.parse(this.sql);
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    value.sql = this.sql;
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    js.sql = this.sql;
    return js;
  }

  public equals(other: SqlAggregateExpression | undefined): boolean {
    return super.equals(other) && this.sql === other.sql;
  }

  protected _toStringParameters(_indent?: int): string[] {
    return [this.sql];
  }

  protected _calcChainableHelper(_operandValue: any): PlywoodValue {
    throw new Error('can not compute on SQL aggregate');
  }

  protected _getSQLChainableHelper(dialect: SQLDialect, operandSQL: string): string {
    let sql = this.sql;
    if (operandSQL.includes(' WHERE ')) {
      const filterParse = SqlExpression.parse(operandSQL.split(' WHERE ')[1]);
      sql = String(SqlAggregateExpression.substituteFilter(this.parsedSql, filterParse));
    }
    return `(${sql})`;
  }

  public extractPivotNestedAggComponents(): {
    subSplitExpression: string;
    innerAggregateExpr: string;
    innerAggregateValueAlias: string;
    outerAggregateExpr: string;
  } | null {
    const functionName = 'PIVOT_NESTED_AGG';

    // Ensure the SQL starts with the function name
    const trimmedSql = this.sql.trim();
    if (!trimmedSql.toUpperCase().startsWith(functionName + '(')) return null;

    // Extract the arguments inside the parentheses
    const argsString = trimmedSql.substring(functionName.length + 1, trimmedSql.length - 1); // Remove function name and parentheses

    // Function to split arguments considering nested parentheses
    function splitArgs(str: string): string[] {
      const args = [];
      let currentArg = '';
      let parenLevel = 0;
      let inSingleQuote = false;
      let inDoubleQuote = false;
      let escapeNextChar = false;

      for (let i = 0; i < str.length; i++) {
        const char = str[i];

        if (escapeNextChar) {
          currentArg += char;
          escapeNextChar = false;
          continue;
        }

        if (char === '\\') {
          escapeNextChar = true;
          currentArg += char;
          continue;
        }

        if (char === "'" && !inDoubleQuote) {
          inSingleQuote = !inSingleQuote;
          currentArg += char;
          continue;
        }

        if (char === '"' && !inSingleQuote) {
          inDoubleQuote = !inDoubleQuote;
          currentArg += char;
          continue;
        }

        if (!inSingleQuote && !inDoubleQuote) {
          if (char === '(') {
            parenLevel++;
          } else if (char === ')') {
            parenLevel--;
          } else if (char === ',' && parenLevel === 0) {
            args.push(currentArg.trim());
            currentArg = '';
            continue;
          }
        }

        currentArg += char;
      }

      if (currentArg.length > 0) {
        args.push(currentArg.trim());
      }

      return args;
    }

    const args = splitArgs(argsString);

    if (args.length !== 3) return null;

    const subSplitExpression = args[0];

    // Parse innerAggregateExpr and innerAggregateValueAlias
    const innerAggregateMatch = /(.+?)\s+AS\s+"?([^"]+)"?$/i.exec(args[1]);
    if (!innerAggregateMatch) return null;

    const innerAggregateExpr = innerAggregateMatch[1].trim();
    const innerAggregateValueAlias = innerAggregateMatch[2].trim();
    const outerAggregateExpr = args[2];

    return {
      subSplitExpression,
      innerAggregateExpr,
      innerAggregateValueAlias,
      outerAggregateExpr,
    };
  }
}

Expression.applyMixins(SqlAggregateExpression, [Aggregate]);
Expression.register(SqlAggregateExpression);
