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

import { Expression } from '../baseExpression';

/**
 * Decomposability trait — declared as a `static` on every class
 * implementing the Aggregate mixin (INV-3). Single source of truth
 * for whether a measure can be re-aggregated after a pre-aggregate-
 * then-join across sources.
 *
 *   'sum'  — associative + commutative across disjoint partitions:
 *            `f(A∪B) = f(A) + f(B)`. Safe for JS-join.
 *   'min'  — `min(A∪B) = min(min(A), min(B))`. Safe, but requires a
 *            min-reducer (not sum) in the post-join JS step; routed
 *            through native-JOIN until that reducer lands (R-4).
 *   'max'  — symmetric to min.
 *   'none' — not losslessly re-aggregatable; native-JOIN is the
 *            only correct path. Default for safety when the trait
 *            isn't reasoned about (e.g. caller-supplied SQL).
 *
 * Every aggregator class declares the trait explicitly. A missing
 * trait is never treated as 'sum' — `Expression.isMeasureDecomposable`
 * throws `PlywoodTraitMissing` on the omission (fail-loud, INV-3).
 */
export type DecomposeTrait = 'sum' | 'min' | 'max' | 'none';

export class Aggregate {
  public operand: Expression;

  public isAggregate(): boolean {
    return true;
  }

  public isNester(): boolean {
    return true;
  }

  public fullyDefined(): boolean {
    const expression: Expression = (this as any).expression;
    return this.operand.isOp('literal') && (expression ? expression.resolved() : true);
  }
}
