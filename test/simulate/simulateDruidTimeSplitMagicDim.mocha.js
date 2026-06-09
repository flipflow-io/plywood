/*
 * Time-bucket split + magic-dimension (linked-only) split — the "Time (Day) ×
 * Brand Country" 500.
 *
 * Reproduced in the Turnilo UI (2026-06-02, reported by Ismael): a table panel
 * with TWO dimensions —
 *
 *   1. Time (Day)    → `$__time.timeBucket(P1D)` over the MAIN time attribute,
 *   2. Brand Country → `$brand_country`, a column that lives ONLY in the
 *                      magic-dim lookup (`lookup_d01f07da_…`, joinKey `brand`,
 *                      `timeAlignment: 'eternal'`),
 *
 * plus avg measures — returned a 500:
 *
 *   Split alias "time" refs [__time] resolve on both main and linked source
 *   "magic_d01f07da-…" — declare them in `sharedDimensions` to use them as a
 *   shared split, or qualify the reference to a side-specific column. The
 *   engine refuses to infer shared-ness from schema overlap…
 *
 * WHY THE THROW IS WRONG HERE (the semantics the fix encodes):
 *   The lookup is `timeAlignment: 'eternal'` — its `__time` column is the
 *   timestamp of the *materialisation snapshot* (a sentinel, e.g. 1970), NOT an
 *   event time. A split on `__time` (the MAIN external's `timeAttribute`) can
 *   therefore never be ambiguous against an eternal linked source: it must
 *   resolve to the MAIN side, no throw. The ambiguity guard MUST still fire for
 *   genuinely ambiguous business columns that appear on both sides without a
 *   `sharedDimensions` declaration — the fix narrows the exception to the main
 *   timeAttribute vs eternal linked sources ONLY (see the contrapositive test).
 *
 * THE FIX (baseExternal.ts `classifySplitAliases`): when a split alias's refs
 * are exactly the main `timeAttribute` and the linked source it overlaps is
 * `timeAlignment: 'eternal'`, classify the alias as main-side instead of
 * throwing. Everything downstream already exists: the main sub-query groups by
 * (timeBucket, __join_brand) with `!T_n` leaves for avg, the lookup query is
 * time-free (eternal prune), and `reAggregateToSplitGrain` re-aggregates by
 * BOTH split keys (time bucket + brand_country).
 *
 * Counterfactual: revert the fix → the first `it` throws the ambiguity error
 * again (asserted directly), and the compute tests never reach their data
 * assertions.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

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

// Two-day query window — the wire-real time filter the UI POSTs.
const timeFilter = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-03T00:00:00Z'),
});

// Main cube + eternal magic-dim lookup. NOTE the lookup carries its OWN `__time`
// attribute (a snapshot sentinel) — that overlap with main's `timeAttribute` is
// exactly what tripped the ambiguity guard before the fix.
function makeMain(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'histories_main',
      timeAttribute: '__time',
      allowEternity: true,
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'competitor', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
      ],
      filter: timeFilter,
      linkedSources: {
        'magic_d01f07da-1111-2222-3333-444455556666': {
          source: 'lookup_d01f07da_rev1',
          joinKeys: ['brand'],
          autoInjectJoinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: '__time', type: 'TIME' },
            { name: 'brand', type: 'STRING' },
            { name: 'brand_country', type: 'STRING' },
          ],
        },
      },
    },
    requester,
  );
}

const MAGIC = 'magic_d01f07da-1111-2222-3333-444455556666';

// Turnilo-style top-level expression with TWO dims: a Time(Day) timeBucket on
// main's __time + the linked-only `brand_country`. Mirrors the wrapper in
// simulateDruidWireRealShapes (scope registrations for each source + a SPLIT
// apply carrying the value applies + sort + limit).
function buildDoubleSplitExpr(valueApplies, sortName) {
  let split = $('main').split({
    time: '$__time.timeBucket(P1D)',
    brand_country: '$brand_country',
  });
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply(MAGIC, $(MAGIC).filter(timeFilter))
    .apply('SPLIT', split);
}

function planSql(expr) {
  return expr
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('Time-bucket split + magic-dimension (eternal linked source)', () => {
  describe('SQL shape — no ambiguity throw, well-formed sub-queries', () => {
    it('avg over [Time(Day) × brand_country] does NOT throw and groups main by bucket + __join_brand', () => {
      const expr = buildDoubleSplitExpr([
        ['avg_price', '$main.average($price)'],
        ['count', '$main.count()'],
      ]);

      // Counterfactual: pre-fix this line throws
      //   Split alias "time" refs [__time] resolve on both main and linked …
      let sqls;
      expect(() => {
        sqls = planSql(expr);
      }, 'must not throw the ambiguity error').to.not.throw();

      const mainSql = sqls.find(
        s => s.includes('"histories_main"') && !s.includes('lookup_d01f07da_rev1'),
      );
      const lookupSql = sqls.find(s => s.includes('lookup_d01f07da_rev1'));
      expect(mainSql, 'main sub-query exists').to.exist;
      expect(lookupSql, 'lookup sub-query exists').to.exist;

      // Main groups by the time bucket AND the synthetic join key.
      expect(mainSql, 'main projects a TIME_FLOOR day bucket').to.match(/TIME_FLOOR/i);
      expect(mainSql, 'main projects __join_brand').to.match(/AS "__join_brand"/);
      // avg decomposes into homomorphic leaves so the post-join re-aggregation
      // can sum them: a SUM("price") leaf and the avg's OWN divisor — a NULL-aware
      // count of price (SQL AVG semantics, Ogievetsky BUG 1), carried as a `!T_`
      // leaf. The panel's explicit `count` measure is a ROW count (COUNT(*),
      // projected `AS "count"`) and is NOT reused as the avg divisor: a row count
      // and a non-null-price count differ whenever any price is null. `avg_price`
      // recombines as `!T_sum / !T_count`, independent of the user's count.
      expect(mainSql, 'SUM leaf for avg').to.match(/SUM\("price"\) AS "!T_\d+"/);
      expect(mainSql, 'avg divisor is a NULL-aware count leaf (not COUNT(*))').to.match(
        /SUM\(CASE WHEN \("price" IS NULL\) IS NOT TRUE THEN 1 ELSE 0 END\) AS "!T_\d+"/,
      );
      expect(mainSql, "user's explicit count is a row COUNT(*)").to.match(/COUNT\(\*\) AS "count"/);
      // GROUP BY must reference ≥2 positions (bucket + join key) — never mutilated.
      const gb = mainSql.match(/GROUP BY ([\d,\s]+)/);
      expect(gb, 'main has a GROUP BY').to.exist;
      const positions = gb[1].split(',').map(s => parseInt(s.trim(), 10));
      expect(Math.max(...positions), 'GROUP BY covers bucket + join key').to.be.at.least(2);

      // Lookup query is TIME-FREE (eternal prune): no WHERE FALSE, no __time clause.
      expect(lookupSql, 'lookup has no WHERE FALSE').to.not.match(/WHERE\s+FALSE/i);
      expect(lookupSql, 'lookup does not filter on __time').to.not.match(/"__time"/);
      expect(lookupSql, 'lookup projects join key').to.match(/"brand" AS "__join_brand"/);

      // No mutilated SELECT anywhere.
      for (const sql of sqls) {
        expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
        expect(sql, 'no empty select item').to.not.match(/,\s*,/);
      }
    });
  });

  describe('Row-level correctness — grain (day, country), weighted avg, exact cardinality', () => {
    // 2 days × 2 brands (B1,B2 → Spain ; B3 → France would need 3 brands; keep
    // 2 brands mapping to 2 countries: B1→Spain, B2→France). Per-day, per-brand
    // leaves engineered so the weighted average ≠ media-de-medias and the day
    // dimension genuinely partitions:
    //   Day1: B1 sum=100 cnt=100 (Spain, avg 1) ; B2 sum=70 cnt=10 (France, avg 7)
    //   Day2: B1 sum=100 cnt=1   (Spain, avg 100); B2 sum=40 cnt=5  (France, avg 8)
    // Each (day, country) cell has exactly one contributing brand here, so the
    // weighted average per cell is just that brand's average — but the
    // re-aggregation must key on BOTH (day, country): collapsing on country
    // alone would wrongly merge Day1+Day2 Spain into one row.
    const DAY1 = new Date('2026-05-01T00:00:00Z');
    const DAY2 = new Date('2026-05-02T00:00:00Z');

    function reqUnequalCounts() {
      return promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_d01f07da_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B2', brand_country: 'France' },
          ]);
        }
        if (sql.includes('histories_main')) {
          // Leaf shape mirrors the emitted main SQL: avg_price segregates to
          // !T_0 = SUM(price) and !T_1 = NULL-aware count of price (its OWN
          // divisor, Ogievetsky BUG 1). The panel's explicit `count` is a
          // separate row COUNT(*). Every row here has price non-null, so the
          // non-null-price count (!T_1) equals the row count. `time` = day bucket.
          return Promise.resolve([
            { 'time': DAY1, '__join_brand': 'B1', '!T_0': 100, '!T_1': 100, 'count': 100 },
            { 'time': DAY1, '__join_brand': 'B2', '!T_0': 70, '!T_1': 10, 'count': 10 },
            { 'time': DAY2, '__join_brand': 'B1', '!T_0': 100, '!T_1': 1, 'count': 1 },
            { 'time': DAY2, '__join_brand': 'B2', '!T_0': 40, '!T_1': 5, 'count': 5 },
          ]);
        }
        return Promise.resolve([]);
      });
    }

    it('collapses to one row per (day, country) with the correct weighted avg', async () => {
      const expr = buildDoubleSplitExpr([
        ['avg_price', '$main.average($price)'],
        ['count', '$main.count()'],
      ]);
      const result = await expr.compute({ main: makeMain(reqUnequalCounts()) });
      const rows = result.toJS().data[0].SPLIT.data;

      // Exact cardinality: 2 days × 1 country-per-brand-cell = 4 distinct cells.
      expect(rows.length, 'exactly 4 (day,country) cells').to.equal(4);

      // The `time` cell is a P1D TimeRange ({ start, end }); match on its start.
      const cell = (day, country) =>
        rows.find(r => +new Date(r.time.start) === +day && r.brand_country === country);

      const d1Spain = cell(DAY1, 'Spain');
      const d1France = cell(DAY1, 'France');
      const d2Spain = cell(DAY2, 'Spain');
      const d2France = cell(DAY2, 'France');
      expect(d1Spain, 'Day1/Spain present').to.exist;
      expect(d1France, 'Day1/France present').to.exist;
      expect(d2Spain, 'Day2/Spain present').to.exist;
      expect(d2France, 'Day2/France present').to.exist;

      // Weighted averages per cell (sum/count of the single contributing brand).
      expect(d1Spain.avg_price, 'Day1/Spain avg').to.be.closeTo(100 / 100, 1e-9);
      expect(d1France.avg_price, 'Day1/France avg').to.be.closeTo(70 / 10, 1e-9);
      expect(d2Spain.avg_price, 'Day2/Spain avg').to.be.closeTo(100 / 1, 1e-9);
      expect(d2France.avg_price, 'Day2/France avg').to.be.closeTo(40 / 5, 1e-9);

      // Counts survive the re-aggregation at the (day,country) grain.
      expect(d1Spain.count, 'Day1/Spain count').to.equal(100);
      expect(d2Spain.count, 'Day2/Spain count').to.equal(1);

      // No synthetic leaf column leaks to the caller.
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
          expect(k, 'no __join_ leak').to.not.equal('__join_brand');
        }
      }
    });

    it('two brands in the SAME (day, country) cell collapse to one weighted-avg row', async () => {
      // Tighten the cardinality + weighted-avg assertion: put B1 and B2 BOTH in
      // Spain so each day's Spain cell receives two brands that must merge.
      //   Day1 Spain: B1 sum=100 cnt=100 (avg 1) + B2 sum=100 cnt=1 (avg 100)
      //     weighted = 200/101 = 1.98019…  (media-de-medias would be 50.5)
      const DAY1b = new Date('2026-05-01T00:00:00Z');
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_d01f07da_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B2', brand_country: 'Spain' },
          ]);
        }
        if (sql.includes('histories_main')) {
          // !T_1 = NULL-aware count of price (avg_price's own divisor); `count` is
          // the user's row COUNT(*). All rows have price non-null here, so equal.
          return Promise.resolve([
            { 'time': DAY1b, '__join_brand': 'B1', '!T_0': 100, '!T_1': 100, 'count': 100 },
            { 'time': DAY1b, '__join_brand': 'B2', '!T_0': 100, '!T_1': 1, 'count': 1 },
          ]);
        }
        return Promise.resolve([]);
      });
      const expr = buildDoubleSplitExpr([
        ['avg_price', '$main.average($price)'],
        ['count', '$main.count()'],
      ]);
      const result = await expr.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      expect(rows.length, 'one Day1/Spain cell (two brands merged)').to.equal(1);
      expect(rows[0].avg_price, 'weighted avg, NOT media-de-medias').to.be.closeTo(200 / 101, 1e-9);
      expect(rows[0].avg_price, 'avg is NOT 50.5').to.not.be.closeTo(50.5, 1e-6);
      expect(rows[0].count, 'count summed across both brands').to.equal(101);
    });
  });

  describe('Regression — count-only [Time(Day) × brand_country]', () => {
    it('count over the double split does not throw and re-aggregates per cell', async () => {
      const DAY1 = new Date('2026-05-01T00:00:00Z');
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('lookup_d01f07da_rev1')) {
          return Promise.resolve([
            { __join_brand: 'B1', brand_country: 'Spain' },
            { __join_brand: 'B2', brand_country: 'Spain' },
          ]);
        }
        if (sql.includes('histories_main')) {
          return Promise.resolve([
            { time: DAY1, __join_brand: 'B1', count: 30 },
            { time: DAY1, __join_brand: 'B2', count: 12 },
          ]);
        }
        return Promise.resolve([]);
      });
      let split = $('main').split({
        time: '$__time.timeBucket(P1D)',
        brand_country: '$brand_country',
      });
      split = split.apply('count', '$main.count()').sort('$count', 'descending').limit(100);
      const expr = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply(MAGIC, $(MAGIC).filter(timeFilter))
        .apply('SPLIT', split);
      const result = await expr.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;
      expect(rows.length, 'one Spain cell').to.equal(1);
      expect(rows[0].count, 'count summed 30+12').to.equal(42);
    });
  });

  describe('Orthogonality — time-only split (no linked dim) is a no-op against the lookup', () => {
    it('split by Time(Day) ALONE emits a single main query, never touches the lookup', () => {
      let split = $('main').split('$__time.timeBucket(P1D)', 'time');
      split = split.apply('avg_price', '$main.average($price)').apply('count', '$main.count()');
      const expr = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply(MAGIC, $(MAGIC).filter(timeFilter))
        .apply('SPLIT', split);
      const sqls = planSql(expr);
      // The lookup must not be queried — no fan-out, no cross-source path.
      const lookupSqls = sqls.filter(s => s.includes('lookup_d01f07da_rev1'));
      expect(lookupSqls.length, 'lookup not queried for a time-only split').to.equal(0);
      const mainSql = sqls.find(s => s.includes('"histories_main"'));
      expect(mainSql, 'main sub-query exists').to.exist;
      // avg stays a native single column (no leaf decomposition without fan-out).
      expect(mainSql, 'no synthetic leaf columns').to.not.match(/!T_\d+/);
      expect(mainSql, 'no synthetic join key').to.not.match(/__join_/);
    });
  });

  describe('Contrapositive — the ambiguity guard still fires for a genuine business overlap', () => {
    it('a non-time column present on both sides WITHOUT sharedDimensions still throws', () => {
      // `competitor` lives on main AND on the lookup, is NOT declared in
      // sharedDimensions (joinKey is `brand`), and is NOT the time attribute.
      // Splitting on it is genuinely ambiguous → must keep throwing. The fix
      // must NOT weaken this.
      //
      // It is paired with the linked-only `brand_country` so the cross-source
      // decomposition path (where the guard lives) is actually entered — a
      // main-only split alone never reaches `classifySplitAliases`.
      const mainAmbig = External.fromJS({
        engine: 'druidsql',
        source: 'histories_main',
        timeAttribute: '__time',
        allowEternity: true,
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'competitor', type: 'STRING' },
          { name: 'price', type: 'NUMBER', unsplitable: true },
        ],
        filter: timeFilter,
        linkedSources: {
          [MAGIC]: {
            source: 'lookup_d01f07da_rev1',
            joinKeys: ['brand'],
            autoInjectJoinKeys: ['brand'],
            sharedDimensions: ['brand'], // competitor deliberately NOT shared
            joinMode: 'inner',
            timeAlignment: 'eternal',
            attributes: [
              { name: '__time', type: 'TIME' },
              { name: 'brand', type: 'STRING' },
              { name: 'competitor', type: 'STRING' }, // same name, both sides
              { name: 'brand_country', type: 'STRING' },
            ],
          },
        },
      });
      let split = $('main').split({
        brand_country: '$brand_country',
        competitor: '$competitor',
      });
      split = split.apply('count', '$main.count()');
      const expr = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply(MAGIC, $(MAGIC).filter(timeFilter))
        .apply('SPLIT', split);
      expect(() => expr.simulateQueryPlan({ main: mainAmbig }).flat()).to.throw(
        /Split alias "competitor" refs \[competitor\] resolve on both main and linked source/,
      );
    });
  });
});
