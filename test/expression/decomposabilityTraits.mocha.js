/*
 * RED spec — single SoT for Aggregate decomposability traits (INV-3).
 *
 * Every Expression class that implements the Aggregate mixin MUST
 * declare a static `decomposable: DecomposeTrait` field as one of
 * 'sum' | 'min' | 'max' | 'none'. The trait is the single switch
 * read by the cross-source decomposition gate to decide whether a
 * measure can be losslessly re-aggregated after a pre-aggregate-
 * then-join: 'sum' is safe, everything else routes to the native-
 * JOIN path.
 *
 * This test pins:
 *   (1) Every aggregator listed in the spec section 2 has the
 *       static declared.
 *   (2) The value is one of the four allowed enum values.
 *   (3) The per-class trait matches the table in section 2 of the
 *       spec.
 *   (4) Structural sweep: iterate Expression.classMap and assert
 *       every registered class whose prototype claims isAggregate()
 *       === true has the trait declared. Catches future PRs that
 *       add a new aggregator without declaring the trait.
 *
 * Counterfactual: forgetting one aggregator (e.g. ModeExpression)
 * makes the structural test red with the missing class name.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const {
  Expression,
  CountExpression,
  SumExpression,
  MinExpression,
  MaxExpression,
  CountDistinctExpression,
  AverageExpression,
  QuantileExpression,
  ModeExpression,
  CustomAggregateExpression,
  SqlAggregateExpression,
} = plywood;

const VALID_TRAITS = new Set(['sum', 'min', 'max', 'none']);

describe('Aggregate decomposability traits', () => {
  // Per-class assertions — fault-localization. A future PR that
  // forgets to update CountDistinctExpression specifically will
  // fail the named assertion below, not just the structural sweep.
  const EXPECTED = [
    ['CountExpression', CountExpression, 'sum'],
    ['SumExpression', SumExpression, 'sum'],
    ['MinExpression', MinExpression, 'min'],
    ['MaxExpression', MaxExpression, 'max'],
    ['CountDistinctExpression', CountDistinctExpression, 'none'],
    ['AverageExpression', AverageExpression, 'none'],
    ['QuantileExpression', QuantileExpression, 'none'],
    ['ModeExpression', ModeExpression, 'none'],
    ['CustomAggregateExpression', CustomAggregateExpression, 'none'],
    ['SqlAggregateExpression', SqlAggregateExpression, 'none'],
  ];

  for (const [name, Cls, expected] of EXPECTED) {
    it(`${name} declares decomposable = '${expected}'`, () => {
      expect(Cls, `${name} class is exported`).to.exist;
      expect(Cls.decomposable, `${name}.decomposable`).to.equal(expected);
    });
  }

  it('every aggregator trait is one of sum|min|max|none', () => {
    for (const [name, Cls] of EXPECTED) {
      expect(VALID_TRAITS.has(Cls.decomposable), `${name}.decomposable invalid value`).to.equal(
        true,
      );
    }
  });

  // Structural sweep — every class in Expression.classMap whose
  // prototype carries isAggregate()===true must have the static.
  // If a new aggregator lands without the trait, this fails with
  // the class name embedded in the message.
  it('every registered Aggregate subclass declares the trait', () => {
    const missing = [];
    for (const op in Expression.classMap) {
      const Cls = Expression.classMap[op];
      // The mixin lives on the prototype; instantiating to call
      // isAggregate() would require a valid ExpressionValue. Read
      // the prototype's method directly — Aggregate.isAggregate
      // returns true unconditionally on subclasses that mixed it in.
      const proto = Cls && Cls.prototype;
      if (!proto || typeof proto.isAggregate !== 'function') continue;
      // The non-aggregate base returns false; the mixin override
      // returns true. Call the prototype method bound to a stub
      // — it never touches `this` for either implementation.
      let isAgg = false;
      try {
        isAgg = proto.isAggregate.call({});
      } catch (_) {
        // If the method touches `this` for some unusual subclass,
        // skip it — the spec's structural sweep only catches the
        // ones the mixin produces and those don't access `this`.
        continue;
      }
      if (!isAgg) continue;
      if (!VALID_TRAITS.has(Cls.decomposable)) {
        missing.push(`${Cls.name || op}: decomposable=${JSON.stringify(Cls.decomposable)}`);
      }
    }
    expect(
      missing,
      `Aggregate subclasses without a valid decomposable trait: [${missing.join(', ')}]`,
    ).to.have.length(0);
  });
});
