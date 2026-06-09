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

import { Dataset, PlywoodValue } from '../datatypes';
import { SQLDialect } from '../dialect/baseDialect';

import {
  ChainableUnaryExpression,
  Expression,
  ExpressionJS,
  ExpressionValue,
} from './baseExpression';
import { Aggregate, DecomposeTrait } from './mixins/aggregate';

export class AverageExpression extends ChainableUnaryExpression implements Aggregate {
  static op = 'Average';
  // avg(A∪B) ≠ avg(avg(A), avg(B)) unless weighted by counts. The
  // existing `decomposeAverage` (see :54) already rewrites avg to
  // sum/count, but that rewrite must happen BEFORE the cross-source
  // decomposition (currently it doesn't fire on the cross-source path).
  // Until that rewrite is moved upstream of getCrossExternalDecomposition,
  // trait stays 'none' (R-3 + section 8 out-of-scope follow-up).
  // TODO(plywood-cross-source): promote to 'sum' once decomposeAverage
  // is called pre-cross-source.
  static decomposable: DecomposeTrait = 'none';
  static fromJS(parameters: ExpressionJS): AverageExpression {
    return new AverageExpression(ChainableUnaryExpression.jsToValue(parameters));
  }

  constructor(parameters: ExpressionValue) {
    super(parameters, dummyObject);
    this._ensureOp('average');
    this._checkOperandTypes('DATASET');
    this._checkExpressionTypes('NUMBER');
    this.type = 'NUMBER';
  }

  protected _calcChainableUnaryHelper(operandValue: any, _expressionValue: any): PlywoodValue {
    return operandValue ? (operandValue as Dataset).average(this.expression) : null;
  }

  protected _getSQLChainableUnaryHelper(
    dialect: SQLDialect,
    operandSQL: string,
    expressionSQL: string,
  ): string {
    return `AVG(${dialect.aggregateFilterIfNeeded(operandSQL, expressionSQL)})`;
  }

  public decomposeAverage(countEx?: Expression, nullAwareCount = true): Expression {
    const { operand, expression } = this;
    // SQL AVG(x) ignores rows where x IS NULL: it is SUM(x) / COUNT(x), NOT
    // SUM(x) / COUNT(*). The decomposed form must match that NULL-aware
    // semantics, otherwise sub-groups whose averaged value is NULL inflate the
    // denominator and understate the average (Ogievetsky BUG 1). The count
    // channel is therefore a NULL-aware count of the AVERAGED column —
    // `operand.filter(expression IS NOT NULL).count()` — which CountExpression
    // lowers to a CASE-summed non-null count (SUM(CASE WHEN x IS NOT NULL THEN
    // 1 ELSE 0 END)), never a bare COUNT(*). The explicit `countEx` override
    // (cross-source weighted recombination) keeps its caller-chosen channel.
    //
    // `nullAwareCount=false` keeps the historical plain COUNT(*): used only by
    // the Druid-native aggregation builder, where a non-null filter on a
    // rolled-up/unsplitable metric is not expressible as a Druid filter.
    let denominator: Expression;
    if (countEx) {
      denominator = operand.sum(countEx);
    } else if (nullAwareCount) {
      // `.simplify()` the filtered operand so that when the average ALREADY
      // carries a measure filter (e.g. `$main.filter(promo).average(price)`),
      // the two predicates collapse into ONE — `filter(promo AND price IS NOT
      // NULL)` — instead of a nested `filter(promo).filter(price IS NOT NULL)`
      // that the single-WHERE count lowering would mangle into invalid SQL
      // (`CASE WHEN promo WHERE price ...`). The simplify is scoped to the count
      // operand only, so the numerator and the rest of the tree are untouched.
      denominator = operand.filter(expression.isnt(null)).simplify().count();
    } else {
      denominator = operand.count();
    }
    return operand.sum(expression).divide(denominator);
  }
}

Expression.applyMixins(AverageExpression, [Aggregate]);
Expression.register(AverageExpression);
