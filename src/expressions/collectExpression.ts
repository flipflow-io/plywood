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

import { Dataset, PlywoodValue, Set } from '../datatypes';
import { SQLDialect } from '../dialect/baseDialect';

import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';
import { Aggregate, DecomposeTrait } from './mixins/aggregate';

export class CollectExpression extends ChainableUnaryExpression implements Aggregate {
  static op = 'Collect';
  // Collect-into-set is a multiset-union semantic — union of A and B
  // is not recoverable from per-partition collects because
  // duplicates between partitions are dropped post-pre-aggregation
  // (same fundamental as countDistinct). Out-of-spec discovery: the
  // spec table at section 2 didn't list CollectExpression; declared
  // 'none' for safety per P1 ("If you encounter an aggregator that
  // the spec does not list, declare 'none' for safety AND report").
  static decomposable: DecomposeTrait = 'none';
  static fromJS(parameters: ExpressionJS): CollectExpression {
    const value = ChainableUnaryExpression.jsToValue(parameters);
    if ((parameters as any).groupByKeys) {
      (value as any).groupByKeys = (parameters as any).groupByKeys;
    }
    return new CollectExpression(value);
  }

  public groupByKeys: string[] | null;

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('collect');
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes(
      'BOOLEAN',
      'NUMBER',
      'TIME',
      'STRING',
      'NUMBER_RANGE',
      'TIME_RANGE',
      'STRING_RANGE',
    );
    this.type = Set.wrapSetType(this.expression.type);
    this.groupByKeys = (parameters as any).groupByKeys || null;
  }

  public valueOf(): ExpressionValue {
    const value = super.valueOf();
    if (this.groupByKeys) {
      (value as any).groupByKeys = this.groupByKeys;
    }
    return value;
  }

  public toJS(): ExpressionJS {
    const js = super.toJS();
    if (this.groupByKeys) {
      (js as any).groupByKeys = this.groupByKeys;
    }
    return js;
  }

  protected _calcChainableUnaryHelper(operandValue: any, _expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).collect(this.expression) : null;
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return dialect.collectExpression(dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL));
  }
}

Expression.applyMixins(CollectExpression, [Aggregate]);
Expression.register(CollectExpression);
