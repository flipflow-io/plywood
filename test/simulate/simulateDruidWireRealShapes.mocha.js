/*
 * Wire-real cross-source query shapes — reproduce the EXACT Expressions the
 * Turnilo front (`flipflow-front`) POSTs to `/plywood`, replayed through
 * `Expression.fromJS(body.expression)` just as the server route does at
 * `src/server/routes/plywood/plywood.ts:71`.
 *
 * Two captured fixtures (GrupoIfa team 507, cube
 * `histories-a435270b90e9b7091c77f478df0b8f78dddd32079b75698b8de902061f74efaf`,
 * captured 2026-06-02 from app.dev):
 *
 *   /tmp/wire-avg-request.json  — split by `competitor_country` (a column that
 *       lives ONLY in the magic-dim lookup `lookup_4584…_rev1`, joinKey
 *       `competitor`) with measures avg_price=avg(price), avg_pvp=avg(pvp),
 *       min_price=min(price), diff_with_pvp=(avg(price)-avg(pvp))/avg(pvp),
 *       count; sort desc by avg_price; limit 50; plus a totals `GROUP BY ()`
 *       query. THIS is the request that produced the 500
 *       ("Column avg_price not found") on plywood 0.51.1.
 *   /tmp/wire-count-request.json — the same split with count only. Works post
 *       0.51.1; pinned here as the regression floor — the fix must not perturb
 *       it.
 *
 * Both fixtures carry the wire-real extras verbatim: the
 * `MillisecondsInInterval` literal apply, and the sibling
 * `magic_4584fb7a-8804-4805-8217-1cfa9ca431f5` linked-source apply with its own
 * `__time` overlap filter — the signal that triggers cross-source fan-out.
 *
 * The External mirror reproduces the real cube: histories source + an inner
 * `competitor`-keyed lookup carrying `competitor_country`, and the GrupoIfa
 * subsetFormula shape `$url.in([...]).not().and(0 < $price)` (the YAML
 * `subsetFormula` arrives at plywood as `External.filter`; see
 * /tmp/turnilo-query-shapes.md §2). A second variant uses the compact
 * canonical NOT-IN of team 275 (`$competitor.in([...]).not()`).
 *
 * The fix under test (working tree, segregate-then-recombine): only when a
 * linked-only split fans main rows out, each main-side measure is segregated
 * into homomorphic LEAF aggregates (SUM/COUNT/MIN/MAX) carried as separate
 * columns, the leaves are re-aggregated per bucket after the JOIN, and the
 * deriving scalar function (divide/subtract) is replayed at the split grain
 * before sort/limit. `renderAggregateSQL` now THROWS on an unrenderable op
 * instead of silently dropping a SELECT item.
 *
 * Counterfactuals: spelled out per test. On 0.51.1 the avg fixture either threw
 * PlywoodCardinalityViolation (jsJoin: ratio column un-reaggregatable) or
 * dropped avg_price's SELECT item while ORDER BY "avg_price" survived (native
 * JOIN: `divide` had no renderer case).
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');
const fs = require('fs');

const plywood = require('../plywood');
const { External, Expression, $, r } = plywood;

const WIRE_AVG = JSON.parse(fs.readFileSync(require('path').join(__dirname, 'fixtures', 'wire-avg-request.json'), 'utf8'));
const WIRE_COUNT = JSON.parse(fs.readFileSync(require('path').join(__dirname, 'fixtures', 'wire-count-request.json'), 'utf8'));

// The linked-source apply name in the wire fixtures (hyphenated UUID).
const MAGIC = 'magic_4584fb7a-8804-4805-8217-1cfa9ca431f5';

// GrupoIfa team 507 subsetFormula shape: NOT(url IN [...]) AND 0 < price. The
// real one is ~150 URLs / 12 KB; two stand-ins keep the predicate structure
// (negated set membership over a main-only column + literal-first range).
const SUBSET_GRUPOIFA = $('url')
  .in(['https://www.carrefour.es/p1', 'https://tienda.mercadona.es/p2'])
  .not()
  .and(r(0).lessThan($('price')));

// Alimentacionglobal team 275 subsetFormula: the compact canonical NOT-IN over
// the join-key column (`competitor`) — which DOES exist on the lookup side.
const SUBSET_NOTIN_COMPETITOR = $('competitor').in(['Alcampoes', 'Waitrosecom']).not();

function makeMain(requester, filterExpr) {
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
      filter: filterExpr || SUBSET_GRUPOIFA,
      linkedSources: {
        [MAGIC]: {
          source: 'lookup_4584fb7a_rev1',
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

function planSqls(expression, filterExpr) {
  return expression
    .simulateQueryPlan({ main: makeMain(undefined, filterExpr) })
    .flat()
    .map(q => (typeof q === 'string' ? q : q && q.query))
    .filter(q => typeof q === 'string');
}

function mainSubQuery(sqls) {
  // The main GROUP-BY-1 sub-query: hits the histories source, joins on
  // competitor, is NOT the lookup query and NOT the totals GROUP BY () query.
  return sqls.find(
    s => s.includes('histories_507') && !s.includes('lookup_4584fb7a') && /GROUP BY 1\b/.test(s),
  );
}

function lookupSubQuery(sqls) {
  return sqls.find(s => s.includes('lookup_4584fb7a_rev1'));
}

// Generic structural well-formedness assertions shared by every emitted SQL.
function assertWellFormed(sql) {
  expect(sql, `no dangling comma before FROM:\n${sql}`).to.not.match(/,\s*\n?\s*FROM/i);
  expect(sql, `no empty select item (double comma):\n${sql}`).to.not.match(/,\s*,/);
  // GROUP BY positions must all reference a projected select item.
  const groupByMatch = sql.match(/GROUP BY ([\d,\s]+)/);
  const selectCols = (sql.match(/ AS "/g) || []).length;
  if (groupByMatch) {
    const positions = groupByMatch[1].split(',').map(s => parseInt(s.trim(), 10));
    const maxPos = Math.max(...positions);
    expect(
      selectCols,
      `GROUP BY references position ${maxPos} but only ${selectCols} select items:\n${sql}`,
    ).to.be.at.least(maxPos);
  }
  // Any ORDER BY column must be a projected alias (the orphan-ORDER-BY bug).
  const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
  if (orderMatch) {
    expect(sql, `ORDER BY "${orderMatch[1]}" must be a projected column:\n${sql}`).to.include(
      `AS "${orderMatch[1]}"`,
    );
  }
}

describe('Wire-real cross-source shapes (Expression.fromJS of captured front requests)', () => {
  describe('S2/S3 — the avg fixture (the 500 the working-tree fix addresses)', () => {
    it('parses the captured wire expression unchanged (round-trip is the server contract)', () => {
      const ex = Expression.fromJS(WIRE_AVG.expression);
      // The hyphenated magic apply, the split on the linked-only dim, and the
      // derived diff_with_pvp ratio are all present verbatim.
      const s = ex.toString();
      expect(s, 'magic linked-source apply present').to.include(MAGIC);
      expect(s, 'split on competitor_country').to.include('split($competitor_country');
      expect(s, 'derived diff_with_pvp ratio present').to.include(
        'average($price).subtract($main.average($pvp)).divide($main.average($pvp))',
      );
      expect(s, 'sort desc by avg_price').to.include('sort($avg_price,descending)');
    });

    it('emits exactly 3 well-formed SQLs (totals + main leaves + lookup); none mutilated', () => {
      const sqls = planSqls(Expression.fromJS(WIRE_AVG.expression));
      expect(sqls.length, 'totals + main + lookup').to.equal(3);
      for (const sql of sqls) assertWellFormed(sql);
    });

    it('main sub-query projects SUM/COUNT leaves for the avgs, MIN by name, and NO ratio column', () => {
      const sqls = planSqls(Expression.fromJS(WIRE_AVG.expression));
      const main = mainSubQuery(sqls);
      expect(main, 'main GROUP BY 1 sub-query exists').to.exist;
      // avg_price + avg_pvp decompose to SUM leaves (price, pvp), EACH with its
      // OWN NULL-aware count of the averaged column (Ogievetsky BUG 1 — counts
      // only non-null values, never a shared COUNT(*)); min_price + the user's
      // row `count` are kept under their own names.
      expect(main, 'SUM(price) leaf').to.match(/SUM\("price"\) AS "!T_\d+"/);
      expect(main, 'SUM(pvp) leaf').to.match(/SUM\("pvp"\) AS "!T_\d+"/);
      expect(main, 'null-aware count of price').to.match(
        /SUM\(CASE WHEN \("price" IS NULL\) IS NOT TRUE THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(main, 'null-aware count of pvp').to.match(
        /SUM\(CASE WHEN \("pvp" IS NULL\) IS NOT TRUE THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(main, "user's row count kept as COUNT(*)").to.match(/COUNT\(\*\) AS "count"/);
      expect(main, 'min_price by name').to.match(/MIN\("price"\) AS "min_price"/);
      // The un-decomposed ratio columns (avg_price / diff_with_pvp) must NOT be
      // projected by the main SQL — they are post-aggregate recombinations.
      expect(main, 'no avg_price ratio column in main SQL').to.not.match(
        /\(SUM\("price"\)\*1\.0\/COUNT\(\*\)\) AS "avg_price"/,
      );
      expect(main, 'no diff_with_pvp derived column in main SQL').to.not.match(
        /AS "diff_with_pvp"/,
      );
      // Sort on a derived measure (avg_price) must NOT stay on the main SQL —
      // it is a post-aggregate name, forced post-join. (This is the exact orphan
      // that produced "Column avg_price not found" on 0.51.1.)
      expect(main, 'no ORDER BY on derived measure in main SQL').to.not.match(/ORDER BY/);
    });

    it('lookup sub-query never WHERE FALSE and never leaks main-only columns', () => {
      const sqls = planSqls(Expression.fromJS(WIRE_AVG.expression));
      const lookup = lookupSubQuery(sqls);
      expect(lookup, 'lookup sub-query exists').to.exist;
      expect(lookup, 'lookup not emptied to FALSE').to.not.match(/WHERE\s+FALSE/i);
      expect(lookup, 'no url leak into lookup').to.not.match(/"url"/);
      expect(lookup, 'no price leak into lookup').to.not.match(/"price"/);
      // It projects the join key + the linked-only dim.
      expect(lookup, 'projects join key').to.match(/"competitor" AS "__join_competitor"/);
      expect(lookup, 'projects linked-only dim').to.match(
        /"competitor_country" AS "competitor_country"/,
      );
    });

    it('compute: per-bucket weighted average + derived ratio, NOT media-de-medias', async () => {
      // Fixture engineered so equal-weighting is provably wrong:
      //   ES = C1(price sum=100 count=100 → avg 1) + C2(price sum=100 count=1 → avg 100)
      //     true weighted avg(price) = 200/101 = 1.98019…  (media-de-medias = 50.5)
      //     pvp: C1 sum=200, C2 sum=200 → weighted avg(pvp) = 400/101 = 3.96039…
      //     diff_with_pvp = (1.98019 − 3.96039)/3.96039 = −0.5
      //     min_price = min(1,5) = 1 ; count = 100+1 = 101
      //   FR = C3(price sum=70 count=10 → avg 7) min=7 ; pvp sum=70 → avg(pvp)=7 → diff=0
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_4584fb7a_rev1')) {
          return Promise.resolve([
            { __join_competitor: 'C1', competitor_country: 'ES' },
            { __join_competitor: 'C2', competitor_country: 'ES' },
            { __join_competitor: 'C3', competitor_country: 'FR' },
          ]);
        }
        if (sql.includes('histories_507') && /GROUP BY 1\b/.test(sql)) {
          // Column shape mirrors the simulate plan: SUM(price)=!T_0,
          // count(non-null price)=!T_1, SUM(pvp)=!T_2, count(non-null pvp)=!T_3;
          // MIN(price)=min_price; the user's `count` is a separate row COUNT(*).
          // Each avg carries its OWN null-aware count (Ogievetsky BUG 1) — no
          // shared COUNT(*). Every row has both columns non-null, so the two
          // null-aware counts equal the row count per competitor.
          return Promise.resolve([
            {
              '__join_competitor': 'C1',
              'min_price': 1,
              'count': 100,
              '!T_0': 100,
              '!T_1': 100,
              '!T_2': 200,
              '!T_3': 100,
            },
            {
              '__join_competitor': 'C2',
              'min_price': 5,
              'count': 1,
              '!T_0': 100,
              '!T_1': 1,
              '!T_2': 200,
              '!T_3': 1,
            },
            {
              '__join_competitor': 'C3',
              'min_price': 7,
              'count': 10,
              '!T_0': 70,
              '!T_1': 10,
              '!T_2': 70,
              '!T_3': 10,
            },
          ]);
        }
        // totals GROUP BY () — single multi-measure row.
        return Promise.resolve([
          { avg_price: 0, avg_pvp: 0, min_price: 0, diff_with_pvp: 0, count: 0 },
        ]);
      });

      const result = await Expression.fromJS(WIRE_AVG.expression).compute({
        main: makeMain(req),
      });
      const rows = result.toJS().data[0].SPLIT.data;

      // Fan-out collapsed: exactly one row per country.
      expect(rows.map(r => r.competitor_country).sort(), 'one row per country').to.deep.equal([
        'ES',
        'FR',
      ]);

      const es = rows.find(r => r.competitor_country === 'ES');
      const fr = rows.find(r => r.competitor_country === 'FR');

      expect(es.avg_price, 'ES weighted avg(price)').to.be.closeTo(200 / 101, 1e-9);
      expect(es.avg_price, 'ES avg is NOT media-de-medias (50.5)').to.not.be.closeTo(50.5, 1e-6);
      expect(es.avg_pvp, 'ES weighted avg(pvp)').to.be.closeTo(400 / 101, 1e-9);
      expect(es.diff_with_pvp, 'ES derived ratio recombined at bucket grain').to.be.closeTo(
        -0.5,
        1e-9,
      );
      expect(es.min_price, 'ES min-of-mins').to.equal(1);
      expect(es.count, 'ES count = sum of per-competitor counts').to.equal(101);

      expect(fr.avg_price, 'FR avg(price)').to.equal(7);
      expect(fr.diff_with_pvp, 'FR diff zero').to.equal(0);
      expect(fr.min_price, 'FR min').to.equal(7);
      expect(fr.count, 'FR count').to.equal(10);

      // Sort desc by avg_price: FR(7) before ES(1.98); limit 50 keeps both.
      expect(
        rows.map(r => r.competitor_country),
        'sort desc by avg_price moved post-join',
      ).to.deep.equal(['FR', 'ES']);

      // No synthetic leaf column leaks to the caller.
      for (const row of rows) {
        for (const k of Object.keys(row)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
        }
      }
      // Counterfactual: on 0.51.1 this fixture 500'd — jsJoin refused to
      // re-aggregate the ratio column (PlywoodCardinalityViolation) or, with
      // min present, native-JOIN dropped avg_price's SELECT item.
    });

    it('the limit (50) caps AFTER recombination + sort, not pre-join row count', async () => {
      // 3 competitors → 3 distinct countries; limit is 50 so all survive. The
      // point: limit is applied to the recombined+sorted result, never to the
      // pre-join main rows (which would silently drop buckets).
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_4584fb7a_rev1')) {
          return Promise.resolve([
            { __join_competitor: 'C1', competitor_country: 'ES' },
            { __join_competitor: 'C2', competitor_country: 'FR' },
            { __join_competitor: 'C3', competitor_country: 'IT' },
          ]);
        }
        if (sql.includes('histories_507') && /GROUP BY 1\b/.test(sql)) {
          // !T_0=SUM(price), !T_1=count(non-null price) [avg_price divisor],
          // !T_2=SUM(pvp), !T_3=count(non-null pvp) [avg_pvp divisor]. All rows
          // non-null, so both null-aware counts equal the row count (5).
          return Promise.resolve([
            {
              '__join_competitor': 'C1',
              'min_price': 1,
              'count': 5,
              '!T_0': 50,
              '!T_1': 5,
              '!T_2': 60,
              '!T_3': 5,
            },
            {
              '__join_competitor': 'C2',
              'min_price': 2,
              'count': 5,
              '!T_0': 30,
              '!T_1': 5,
              '!T_2': 60,
              '!T_3': 5,
            },
            {
              '__join_competitor': 'C3',
              'min_price': 3,
              'count': 5,
              '!T_0': 40,
              '!T_1': 5,
              '!T_2': 60,
              '!T_3': 5,
            },
          ]);
        }
        return Promise.resolve([
          { avg_price: 0, avg_pvp: 0, min_price: 0, diff_with_pvp: 0, count: 0 },
        ]);
      });
      const result = await Expression.fromJS(WIRE_AVG.expression).compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      expect(rows.length, 'all 3 buckets survive limit 50').to.equal(3);
      // avg_price = !T_0/!T_1 : ES=10, IT=8, FR=6 → sorted desc.
      expect(
        rows.map(r => r.competitor_country),
        'sorted desc by avg_price',
      ).to.deep.equal(['ES', 'IT', 'FR']);
    });
  });

  describe('S2 — the count fixture (0.51.1 regression floor; fix must be inert)', () => {
    it('parses + emits 3 well-formed SQLs with NO leaf decomposition', () => {
      const ex = Expression.fromJS(WIRE_COUNT.expression);
      expect(ex.toString(), 'single count measure').to.include('count,$main.count()');
      const sqls = planSqls(ex);
      expect(sqls.length, 'totals + main + lookup').to.equal(3);
      for (const sql of sqls) assertWellFormed(sql);
      const main = mainSubQuery(sqls);
      expect(main, 'main sub-query exists').to.exist;
      expect(main, 'count kept under its own name').to.match(/COUNT\(\*\) AS "count"/);
      // A single homomorphic aggregate over a fan-out split never needs the
      // segregate-then-recombine machinery: NO synthetic leaves are minted.
      expect(main, 'no synthetic leaf columns (fix inert for plain count)').to.not.match(/!T_\d+/);
    });

    it('compute: count fans out then re-aggregates by SUM to the country grain', async () => {
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_4584fb7a_rev1')) {
          return Promise.resolve([
            { __join_competitor: 'C1', competitor_country: 'ES' },
            { __join_competitor: 'C2', competitor_country: 'ES' },
            { __join_competitor: 'C3', competitor_country: 'FR' },
          ]);
        }
        if (sql.includes('histories_507') && /GROUP BY 1\b/.test(sql)) {
          return Promise.resolve([
            { __join_competitor: 'C1', count: 30 },
            { __join_competitor: 'C2', count: 12 },
            { __join_competitor: 'C3', count: 7 },
          ]);
        }
        return Promise.resolve([{ count: 0 }]);
      });
      const result = await Expression.fromJS(WIRE_COUNT.expression).compute({
        main: makeMain(req),
      });
      const rows = result.toJS().data[0].SPLIT.data;
      const es = rows.find(r => r.competitor_country === 'ES');
      const fr = rows.find(r => r.competitor_country === 'FR');
      expect(es.count, 'ES = 30 + 12').to.equal(42);
      expect(fr.count, 'FR = 7').to.equal(7);
      // Sort desc by count: ES(42) before FR(7).
      expect(rows.map(r => r.competitor_country)).to.deep.equal(['ES', 'FR']);
    });
  });

  describe('subsetFormula variant — compact NOT-IN over the join key (team 275 shape)', () => {
    it('a NOT over the join-key column (competitor) SURVIVES the prune onto the lookup', () => {
      // `$competitor.in([...]).not()` references `competitor`, which exists on
      // BOTH sides. The exclusion must reach the lookup sub-query (not be pruned
      // to identity like a main-only column would be).
      const sqls = planSqls(Expression.fromJS(WIRE_AVG.expression), SUBSET_NOTIN_COMPETITOR);
      const lookup = lookupSubQuery(sqls);
      expect(lookup, 'lookup sub-query exists').to.exist;
      expect(lookup, 'lookup not WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
      // The exclusion appears on at least one side (main and/or lookup); the
      // key property is it is NOT silently dropped and produces no FALSE.
      const main = mainSubQuery(sqls);
      const exclusionPresent = /Alcampoes/.test(main || '') || /Alcampoes/.test(lookup || '');
      expect(exclusionPresent, 'join-key exclusion reaches a sub-query').to.equal(true);
      for (const sql of sqls) assertWellFormed(sql);
    });
  });
});
