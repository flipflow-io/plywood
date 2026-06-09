/*
 * MORE cross-source query shapes from the front catalog
 * (/tmp/turnilo-query-shapes.md) that the avg-fixture test does NOT cover, so
 * the segregate-then-recombine fix is pinned across the full surface the
 * front (`visualization-query.ts`) can emit, not just the single captured S2/S3.
 *
 * Shapes (catalog §3):
 *   S1 — Totals (`GROUP BY ()`), multi-measure incl. derived ratio, NO split.
 *        No fan-out → the fix MUST be inert: avg/ratio render as native
 *        single-column SQL. Simulate + compute.
 *   S4 — Nested splits (SPLIT inside SPLIT). Outer = linked-only
 *        `competitor_country` (fan-out), inner = shared `competitor`. The main
 *        sub-query still segregates the outer-grain measures into homomorphic
 *        leaves. SIMULATE-ONLY — see the note on the describe block: the
 *        per-sub-split recombination of leaf columns inside a nested SPLIT is
 *        NOT exercised by the captured (single-split) fixtures and is out of
 *        scope for this fix; only the SQL plan well-formedness is pinned here.
 *   S5 — Timeshift / compare-previous-period. current = avg(price);
 *        previous = avg(price) over the shifted window; delta = current −
 *        previous. Over a linked-only split, BOTH avgs decompose to homomorphic
 *        leaves (current → SUM/COUNT; previous → SUM(CASE…)/SUM(CASE 1…)) that
 *        survive the JOIN fan-out — this is the "timeshift-decomposition" half
 *        the branch name promises. SIMULATE-ONLY: the timeshift fixtures were
 *        not captured from the wire (catalog §S5 "Not in captured fixtures");
 *        the compute recombination of three-way current/previous/delta over a
 *        cross-source split is not pinned here, only the leaf SQL plan.
 *   S6 — timeBucket / numberBucket: documented as NOT a cross-source fan-out
 *        shape. The captured fixtures split on a plain STRING (competitor_country);
 *        a bucketed split is over a main numeric/time column, and a magic-dim
 *        column is never numeric/time. A timeBucket on `__time` under a magic
 *        apply fails loud (same-named column on both sides) — an intentional
 *        guard, not part of this fix. No S6 test is asserted.
 *
 * Counterfactuals: per test where applicable.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');
const { External, $, ply, r } = plywood;

const MAGIC = 'magic_cc';

// Same GrupoIfa subsetFormula shape as the wire fixtures (NOT(url IN) AND 0<price).
const SUBSET = $('url')
  .in(['https://x/p1'])
  .not()
  .and(r(0).lessThan($('price')));

const TF = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-02T00:00:00Z'),
});

function promiseFnToStream(promiseRq) {
  return rq => {
    const stream = new PassThrough({ objectMode: true });
    promiseRq(rq).then(
      res => {
        if (Array.isArray(res)) for (const row of res) stream.write(row);
        else if (res) stream.write(res);
        stream.end();
      },
      e => {
        stream.emit('error', e);
        stream.end();
      },
    );
    return stream;
  };
}

function makeMain(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'histories_507',
      timeAttribute: '__time',
      allowEternity: true,
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'url', type: 'STRING' },
        { name: 'competitor', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
      ],
      filter: SUBSET,
      linkedSources: {
        [MAGIC]: {
          source: 'lookup_cc_rev1',
          joinKeys: ['competitor'],
          autoInjectJoinKeys: ['competitor'],
          sharedDimensions: ['competitor'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: '__time', type: 'TIME' },
            { name: 'competitor', type: 'STRING' },
            { name: 'competitor_country', type: 'STRING' },
          ],
        },
      },
    },
    requester,
  );
}

function planSqls(expression) {
  return expression
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .map(q => (typeof q === 'string' ? q : q && q.query))
    .filter(q => typeof q === 'string');
}

function assertWellFormed(sql) {
  expect(sql, `no dangling comma before FROM:\n${sql}`).to.not.match(/,\s*\n?\s*FROM/i);
  expect(sql, `no empty select item (double comma):\n${sql}`).to.not.match(/,\s*,/);
  const groupByMatch = sql.match(/GROUP BY ([\d,\s]+)/);
  const selectCols = (sql.match(/ AS "/g) || []).length;
  if (groupByMatch) {
    const positions = groupByMatch[1].split(',').map(s => parseInt(s.trim(), 10));
    const maxPos = Math.max(...positions);
    expect(
      selectCols,
      `GROUP BY position ${maxPos} > ${selectCols} select items:\n${sql}`,
    ).to.be.at.least(maxPos);
  }
  const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
  if (orderMatch) {
    expect(sql, `ORDER BY "${orderMatch[1]}" must be projected:\n${sql}`).to.include(
      `AS "${orderMatch[1]}"`,
    );
  }
}

describe('More cross-source shapes — catalog S1/S4/S5', () => {
  describe('S1 — totals (no split): the fix is inert, avg/ratio render natively', () => {
    function buildTotals() {
      return ply()
        .apply('main', $('main').filter(TF))
        .apply('magic_cc', $('magic_cc').filter(TF))
        .apply('avg_price', '$main.average($price)')
        .apply(
          'diff_with_pvp',
          '($main.average($price).subtract($main.average($pvp))).divide($main.average($pvp))',
        )
        .apply('count', '$main.count()');
    }

    it('emits a single GROUP BY () SQL with native AVG + ratio, no leaves, no JOIN', () => {
      const sqls = planSqls(buildTotals());
      expect(sqls.length, 'single totals query (no fan-out)').to.equal(1);
      const sql = sqls[0];
      assertWellFormed(sql);
      expect(sql, 'GROUP BY ()').to.include('GROUP BY ()');
      expect(sql, 'native AVG(price)').to.match(/AVG\("price"\) AS "avg_price"/);
      expect(sql, 'native derived ratio').to.match(/AS "diff_with_pvp"/);
      expect(sql, 'no synthetic leaf columns (fix inert with no split)').to.not.match(/!T_\d+/);
      expect(sql, 'no JOIN in totals').to.not.match(/JOIN/i);
    });

    it('compute: totals avg + ratio computed directly from the single engine row', async () => {
      const req = promiseFnToStream(() =>
        Promise.resolve([{ avg_price: 12.5, diff_with_pvp: -0.2, count: 999 }]),
      );
      const result = await buildTotals().compute({ main: makeMain(req) });
      const datum = result.toJS().data[0];
      expect(datum.avg_price, 'avg passes through').to.equal(12.5);
      expect(datum.diff_with_pvp, 'ratio passes through').to.equal(-0.2);
      expect(datum.count, 'count passes through').to.equal(999);
    });
  });

  describe('S4 — nested split (SPLIT-in-SPLIT) — SIMULATE-ONLY (plan well-formedness)', () => {
    // NOTE: only the emitted SQL plan is pinned. The per-sub-split JS
    // recombination of leaf columns inside a nested SPLIT is NOT exercised by
    // the captured single-split fixtures and is out of scope for this fix.
    function buildNested() {
      const inner = $('main')
        .split('$competitor', 'competitor')
        .apply('avg_price', '$main.average($price)')
        .sort('$avg_price', 'descending')
        .limit(10);
      const outer = $('main')
        .split('$competitor_country', 'competitor_country')
        .apply('avg_price', '$main.average($price)')
        .apply('SPLIT', inner)
        .sort('$avg_price', 'descending')
        .limit(10);
      return ply()
        .apply('main', $('main').filter(TF))
        .apply('magic_cc', $('magic_cc').filter(TF))
        .apply('SPLIT', outer);
    }

    it('outer linked-only split segregates the outer avg into SUM/COUNT leaves; all SQL well-formed', () => {
      const sqls = planSqls(buildNested());
      for (const sql of sqls) assertWellFormed(sql);
      const main = sqls.find(
        s => s.includes('histories_507') && !s.includes('lookup_cc_rev1') && /GROUP BY 1\b/.test(s),
      );
      expect(main, 'main GROUP BY 1 sub-query exists').to.exist;
      expect(main, 'SUM(price) leaf for the outer avg').to.match(/SUM\("price"\) AS "!T_\d+"/);
      // NULL-aware count of price (SQL AVG semantics, Ogievetsky BUG 1), not COUNT(*).
      expect(main, 'NULL-aware COUNT leaf for the outer avg').to.match(
        /SUM\(CASE WHEN \("price" IS NULL\) IS NOT TRUE THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(main, 'no bare COUNT(*) leaf').to.not.match(/COUNT\(\*\) AS "!T_\d+"/);
      // The un-decomposed ratio column must NOT be the main projection (that was
      // the malformed shape on a stale/no-fix build).
      expect(main, 'no ratio column in main SQL').to.not.match(
        /\(SUM\("price"\)\*1\.0\/.*\) AS "avg_price"/,
      );
    });
  });

  describe('S5 — timeshift current/previous/delta — SIMULATE-ONLY (leaf SQL plan)', () => {
    // Timeshift fixtures were not captured from the wire (catalog §S5). Only the
    // leaf-decomposition SQL is pinned: both the current and previous avgs must
    // become homomorphic leaves so the JOIN fan-out is recombinable.
    function buildTimeshift() {
      const widerTF = $('__time').overlap({
        start: new Date('2026-04-01T00:00:00Z'),
        end: new Date('2026-05-02T00:00:00Z'),
      });
      const prevWindow = $('main').filter(
        $('__time').overlap({
          start: new Date('2026-04-01T00:00:00Z'),
          end: new Date('2026-04-16T00:00:00Z'),
        }),
      );
      const cur = $('main').average($('price'));
      const prev = prevWindow.average($('price'));
      const split = $('main')
        .split('$competitor_country', 'competitor_country')
        .apply('avg_price', cur)
        .apply('avg_price_prev', prev)
        .apply('avg_price_delta', cur.subtract(prev))
        .sort('$avg_price', 'descending')
        .limit(10);
      return ply()
        .apply('main', $('main').filter(widerTF))
        .apply('magic_cc', $('magic_cc').filter(widerTF))
        .apply('SPLIT', split);
    }

    it('current AND previous avgs both decompose to homomorphic leaves over the fan-out split', () => {
      const sqls = planSqls(buildTimeshift());
      for (const sql of sqls) assertWellFormed(sql);
      const main = sqls.find(
        s => s.includes('histories_507') && !s.includes('lookup_cc_rev1') && /GROUP BY 1\b/.test(s),
      );
      expect(main, 'main GROUP BY 1 sub-query exists').to.exist;
      // current avg → unconditional SUM + NULL-aware count leaves (the count is
      // a count of non-null price — SQL AVG semantics, Ogievetsky BUG 1 — not
      // COUNT(*)).
      expect(main, 'current SUM(price) leaf').to.match(/SUM\("price"\) AS "!T_\d+"/);
      expect(main, 'current NULL-aware COUNT leaf').to.match(
        /SUM\(CASE WHEN \("price" IS NULL\) IS NOT TRUE THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(main, 'no bare COUNT(*) leaf').to.not.match(/COUNT\(\*\) AS "!T_\d+"/);
      // previous avg → window-conditioned SUM(CASE…) + SUM(CASE 1…) leaves —
      // both still homomorphic (SUM-reducible) so the fan-out collapses correctly.
      expect(main, 'previous SUM(CASE…) leaf').to.match(
        /SUM\(CASE WHEN .*THEN "price" ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(main, 'previous COUNT-as-SUM(CASE 1) leaf').to.match(
        /SUM\(CASE WHEN .*THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      // No derived measure (delta/ratio) and no ORDER BY leaks into the main SQL.
      expect(main, 'no derived measure column in main SQL').to.not.match(
        /AS "avg_price(_prev|_delta)?"/,
      );
      expect(main, 'no ORDER BY on derived measure in main SQL').to.not.match(/ORDER BY/);
      // Counterfactual: a non-decomposable previous-window aggregate (e.g.
      // countDistinct over the shifted window) would force native-JOIN; here the
      // SUM(CASE) homomorphism is what keeps timeshift on the JS-join leaf path.
    });
  });
});
