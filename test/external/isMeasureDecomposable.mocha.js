/*
 * RED spec — Expression.isMeasureDecomposable predicate (INV-2 + INV-3).
 *
 * Decides whether an ApplyExpression's value can be losslessly
 * re-aggregated post-join, by reading each Aggregate node's static
 * `decomposable` trait. Returns true iff every walked aggregate
 * declares 'sum'. Anything else ('min', 'max', 'none') routes the
 * query through native-JOIN, not JS-join.
 *
 * Cases pinned:
 *   (1) count(*) → true.
 *   (2) sum($x) → true.
 *   (3) countDistinct($x) → false.
 *   (4) average($x) → false (trait 'none' until decomposeAverage
 *       moves upstream of getCrossExternalDecomposition).
 *   (5) count() + sum($x) (additive composition) → true.
 *   (6) sum(countDistinct nested somehow) — really: an apply that
 *       contains BOTH a sum and a countDistinct in its subtree
 *       → false.
 *   (7) An aggregate class without the trait → PlywoodTraitMissing.
 *       (Currently never happens after Phase 2; pinned via a hand-
 *       crafted mock that clears the trait.)
 *
 * Counterfactual: hard-coding the predicate to always-true makes
 * (3),(4),(6) flip; always-false breaks (1),(2),(5).
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { $, Expression, ply, PlywoodTraitMissing } = plywood;

describe('Expression.isMeasureDecomposable', () => {
  function applyOf(name, ex) {
    return ply().apply(name, ex).expression.actions ? null : null;
  }

  function findApply(plyExpr, name) {
    // ply().apply('m', ex) builds a LiteralExpression of dataset
    // wrapped in an ApplyExpression chain. The apply node is the
    // top-level operand of the resulting chain — Plywood's
    // `ply().apply(...)` returns a chainable expression. Easier:
    // construct the ApplyExpression directly via Expression.fromJS.
    return null;
  }

  function applyExprFromJS(name, jsBody) {
    return Expression.fromJS({
      op: 'apply',
      operand: { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' },
      expression: jsBody,
      name,
    });
  }

  it('count(*) is decomposable', () => {
    const apply = applyExprFromJS('cnt', {
      op: 'count',
      operand: { op: 'ref', name: 'main' },
    });
    expect(Expression.isMeasureDecomposable(apply)).to.equal(true);
  });

  it('sum($x) is decomposable', () => {
    const apply = applyExprFromJS('s', {
      op: 'sum',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: 'price' },
    });
    expect(Expression.isMeasureDecomposable(apply)).to.equal(true);
  });

  it('countDistinct($x) is NOT decomposable', () => {
    const apply = applyExprFromJS('uniq', {
      op: 'countDistinct',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: 'userIdProduct' },
    });
    expect(Expression.isMeasureDecomposable(apply)).to.equal(false);
  });

  it('average($x) is NOT decomposable (trait none — see AverageExpression)', () => {
    const apply = applyExprFromJS('avgPrice', {
      op: 'average',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: 'price' },
    });
    expect(Expression.isMeasureDecomposable(apply)).to.equal(false);
  });

  it('sum($a) + sum($b) is decomposable (every leaf is sum)', () => {
    const apply = applyExprFromJS('sumAB', {
      op: 'add',
      operand: {
        op: 'sum',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'a' },
      },
      expression: {
        op: 'sum',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'b' },
      },
    });
    expect(Expression.isMeasureDecomposable(apply)).to.equal(true);
  });

  it('sum($a) + countDistinct($b) is NOT decomposable (mixed)', () => {
    const apply = applyExprFromJS('mixed', {
      op: 'add',
      operand: {
        op: 'sum',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'a' },
      },
      expression: {
        op: 'countDistinct',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'b' },
      },
    });
    expect(Expression.isMeasureDecomposable(apply)).to.equal(false);
  });

  it('min($x) is NOT JS-join-decomposable (trait min, not sum — routes through native-JOIN)', () => {
    const apply = applyExprFromJS('mn', {
      op: 'min',
      operand: { op: 'ref', name: 'main' },
      expression: { op: 'ref', name: 'price' },
    });
    expect(Expression.isMeasureDecomposable(apply)).to.equal(false);
  });

  it('throws PlywoodTraitMissing when an Aggregate class lacks the trait', () => {
    // Build a real aggregate, then strip the static. Restore after the
    // assertion so other tests aren't poisoned.
    const { CountExpression } = plywood;
    const saved = CountExpression.decomposable;
    delete CountExpression.decomposable;
    try {
      const apply = applyExprFromJS('cnt', {
        op: 'count',
        operand: { op: 'ref', name: 'main' },
      });
      expect(() => Expression.isMeasureDecomposable(apply)).to.throw(PlywoodTraitMissing);
    } finally {
      CountExpression.decomposable = saved;
    }
  });
});
