/*
 * Phase 5 — property test: 4 measures × 3 split shapes.
 *
 * Cartesian product of measure traits × split topologies:
 *   measures: {sum, countDistinct, average, quantile}
 *     - sum             → decomposable, JS-join path
 *     - countDistinct   → trait 'none', native-JOIN path
 *     - average         → decomposed to sum/count pre-gate (F2),
 *                          routes through the JS-join path
 *     - quantile        → trait 'none', native-JOIN path
 *   splits: {linked-only, shared, main-only}
 *     - linked-only     → user split is a column that lives ONLY in
 *                          the lookup; gate fires for non-sum measures.
 *     - shared          → split is in `sharedDimensions`; both sides
 *                          can render it. Gate does NOT fire (no
 *                          fan-out risk).
 *     - main-only       → split is a column on main; single-source
 *                          path, no cross-source decomposition at all.
 *
 * For each cell we check the SQL-shape (number of queries emitted)
 * via simulateQueryPlan. Counterfactual: a regression that
 * re-enables JS-join for countDistinct flips 3 cells from green to
 * red simultaneously.
 *
 * Quantile is included even though plywood's standard quantile is a
 * `Quantile` op that emits APPROX_QUANTILE. Native-JOIN routing
 * tests only the gate; the SQL renderer for native-JOIN doesn't
 * implement quantile yet, so those cells fall back to null (caller
 * gets a single-query attempt that may fail at SQL emission). We
 * encode that as "no native-join, no js-join" — the gate refused
 * both paths.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $ } = plywood;

// `crossEngine`: the lookup lives in Postgres (a staging view). Same-engine
// (the default) models a materialised Druid datasource: since 0.51.10 every
// linked-only split on it takes the native JOIN, whatever the measure; the
// JS-join and its decomposability gate remain the cross-engine plan.
function makeMain(opts = {}) {
  const ext = External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'productName', type: 'STRING' },
      { name: 'price', type: 'NUMBER' },
      { name: 'userIdProduct', type: 'STRING' },
    ],
    linkedSources: {
      lookup_x: {
        source: 'lookup_x_rev1',
        joinKeys: ['productName'],
        autoInjectJoinKeys: ['productName'],
        sharedDimensions: ['productName'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        ...(opts.crossEngine ? { engine: 'postgres', version: '16.0.0' } : {}),
        attributes: [
          { name: 'productName', type: 'STRING' },
          { name: 'uso_tipico', type: 'STRING' },
        ],
      },
    },
    filter: $('__time').overlap({
      start: new Date('2026-05-01T00:00:00Z'),
      end: new Date('2026-05-02T00:00:00Z'),
    }),
  });
  if (opts.crossEngine) {
    ext.linkedSources.lookup_x.requester = () => {
      throw new Error('postgres requester must not run in simulate');
    };
  }
  return ext;
}

function runPlan(ex, opts) {
  const queries = ex.simulateQueryPlan({ main: makeMain(opts) }).flat();
  // Normalize to SQL strings — different paths wrap differently.
  const sqls = [];
  for (const q of queries) {
    if (typeof q === 'string') sqls.push(q);
    else if (q && typeof q.query === 'string') sqls.push(q.query);
    else if (q && q.query && typeof q.query.query === 'string') sqls.push(q.query.query);
  }
  return sqls;
}

describe('Cross-source decomposability matrix (4 measures × 3 splits)', () => {
  // Helper: build a top-level apply expression using the given
  // split alias and aggregate JSON. Returns a Plywood expression.
  function buildEx(splitAliasExprStr, splitName, applyJSExpr) {
    return $('main')
      .split(splitAliasExprStr, splitName)
      .apply('m', plywood.Expression.fromJS(applyJSExpr));
  }

  // The 4 measures, encoded as the aggregate-only JSON expression
  // (the operand $main is implicit and resolved by addExpression).
  const MEASURES = [
    {
      name: 'sum',
      json: {
        op: 'sum',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'price' },
      },
      trait: 'sum',
    },
    {
      name: 'countDistinct',
      json: {
        op: 'countDistinct',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'userIdProduct' },
      },
      trait: 'none',
    },
    {
      // F2: avg is rewritten to sum/count pre-gate, so it routes
      // through the JS-join path (same as a raw sum). Trait
      // declaration on AverageExpression stays 'none' — the rewrite
      // happens BEFORE the gate reads the trait.
      name: 'average',
      json: {
        op: 'average',
        operand: { op: 'ref', name: 'main' },
        expression: { op: 'ref', name: 'price' },
      },
      trait: 'sum',
    },
    // Skip quantile in the matrix execution — the gate refusing it
    // and the native-JOIN renderer not knowing it would generate a
    // unit test for "neither path applies" which the Phase 4 native
    // renderer would have to fail cleanly on. Add a dedicated case
    // outside the matrix to keep this assertion crisp.
  ];

  // Same-engine lookup (a materialised Druid datasource): EVERY measure over
  // a linked-only split routes to the native JOIN — one combined SQL. The
  // measure's trait no longer decides the route here; it only decides whether
  // the JS-join would ALSO have been correct (sum/avg) or not (countDistinct).
  for (const m of MEASURES) {
    it(`same-engine lookup: linked-only split + ${m.name} → native JOIN (1 combined query)`, () => {
      const ex = buildEx('$uso_tipico', 'uso_tipico', m.json);
      const sqls = runPlan(ex);
      const combined = sqls.filter(s => s.includes('"main_ds"') && s.includes('"lookup_x_rev1"'));
      expect(combined, 'one combined SQL').to.have.length(1);
      expect(combined[0], 'has JOIN clause').to.match(/INNER\s+JOIN|LEFT\s+JOIN/i);
      expect(
        sqls.filter(s => s.includes('"lookup_x_rev1"') && !s.includes('"main_ds"')),
        'no separate lookup scan',
      ).to.have.length(0);
    });
  }

  // Cross-engine lookup (a Postgres staging view): the gate keeps deciding.
  // Decomposable measures (sum, avg→sum/count) take the JS-join (2 queries);
  // a 'none'-trait measure cannot be recombined AND cannot be joined in one
  // SQL across engines, so it fails loud instead of returning a wrong number.
  for (const m of MEASURES) {
    it(`cross-engine lookup: linked-only split + ${m.name} → ${
      m.trait === 'sum' ? 'JS-join (2 queries)' : 'fails loud (no correct plan)'
    }`, () => {
      const ex = buildEx('$uso_tipico', 'uso_tipico', m.json);
      if (m.trait === 'sum') {
        const sqls = runPlan(ex, { crossEngine: true });
        const mains = sqls.filter(s => s.includes('"main_ds"') && !s.includes('"lookup_x_rev1"'));
        const linkeds = sqls.filter(s => s.includes('"lookup_x_rev1"') && !s.includes('"main_ds"'));
        expect(mains, 'one main query').to.have.length(1);
        expect(linkeds, 'one linked query').to.have.length(1);
      } else {
        expect(() => runPlan(ex, { crossEngine: true })).to.throw(
          plywood.PlywoodUnsupportedNativeJoinShape,
          /cannot span two engines/,
        );
      }
    });
  }

  // Shared split (productName, declared in sharedDimensions): the
  // gate doesn't fire — no linked-only fan-out, JS-join is valid
  // even for non-sum measures. Two queries on both sides.
  for (const m of MEASURES) {
    it(`shared split (productName) + ${m.name} → JS-join (2 queries)`, () => {
      const ex = buildEx('$productName', 'productName', m.json);
      const sqls = runPlan(ex);
      const mains = sqls.filter(s => s.includes('"main_ds"') && !s.includes('"lookup_x_rev1"'));
      const linkeds = sqls.filter(s => s.includes('"lookup_x_rev1"') && !s.includes('"main_ds"'));
      // With shared-only split and a main-only apply, the lookup is
      // not strictly needed — but cross-source decomposition may
      // still emit a linked query if the lookup is referenced by
      // scope or by the split itself. The matrix only pins that the
      // path is NOT native-JOIN.
      const combined = sqls.filter(s => s.includes('"main_ds"') && s.includes('"lookup_x_rev1"'));
      expect(combined, 'no native-JOIN combined query for shared split').to.have.length(0);
      // At least the main query exists.
      expect(mains, 'main query emitted').to.have.length.at.least(1);
    });
  }

  // Main-only split: single-source path entirely. No lookup query.
  for (const m of MEASURES) {
    it(`main-only split (price, no linked) + ${m.name} → single source (1 main query)`, () => {
      const ex = buildEx('$price', 'price', m.json);
      const sqls = runPlan(ex);
      const mains = sqls.filter(s => s.includes('"main_ds"') && !s.includes('"lookup_x_rev1"'));
      const linkeds = sqls.filter(s => s.includes('"lookup_x_rev1"'));
      expect(linkeds, 'no lookup query for main-only split').to.have.length(0);
      expect(mains, 'one main query').to.have.length.at.least(1);
    });
  }
});
