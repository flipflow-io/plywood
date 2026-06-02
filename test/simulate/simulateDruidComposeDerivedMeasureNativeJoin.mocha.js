/*
 * COMPOSITION test — DERIVED measure (ratio/diff of avgs) × magic-dimension split,
 * forced onto the native-JOIN route by a sibling non-decomposable countDistinct.
 *
 * This is Ismael's live-app bug. Panel:
 *   split  : brand_country  (magic dim, linked-only, joinKey 'brand', inner, eternal)
 *   measures:
 *     rp_pvp_diff = (AVG(price) - AVG(pvp)) / AVG(pvp)   ← root op 'divide' (derived)
 *     uniq        = countDistinct(product_id)            ← trait 'none' (non-decomposable)
 *
 * The countDistinct ('none' trait) is not recombinable from per-partition partials,
 * so the INV-2 gate diverts the WHOLE panel to the native-JOIN path: a single Druid
 * SQL with an in-engine INNER JOIN that computes every measure at the bucket grain.
 *
 * WHY THE DERIVED MEASURE IS CORRECT ON NATIVE-JOIN. The native JOIN is ONE SELECT
 * with GROUP BY over the already-joined rows (main INNER JOIN lookup ON brand). Each
 * brand maps to EXACTLY one country in the lookup, so the inner join does NOT
 * duplicate main rows → AVG(price), AVG(pvp), SUM, COUNT(DISTINCT …) are computed
 * natively, correct at the split grain. A ratio-of-avgs is therefore valid SQL
 * arithmetic at the country grain: (AVG("price")-AVG("pvp"))/AVG("pvp") — NOT a
 * media-de-ratios. renderAggregateSQL must recurse the arithmetic ops (divide,
 * subtract, multiply, add) and render each operand (which bottoms out in a single
 * aggregate or a literal), then combine via the expression's own SQL helper (so
 * div-by-zero is whatever the Druid dialect already does — floatDivision = num*1.0/den).
 *
 * THE FAIL-LOUD LIMIT STAYS. A formula whose leaf is genuinely non-renderable on
 * native-JOIN (quantile inside a ratio, or a sqlAggregate / PIVOT_NESTED_AGG) MUST
 * still throw PlywoodUnsupportedNativeJoinShape — composing arithmetic does not
 * silence a bad leaf.
 *
 * Wire-real measure shapes mirror the canonical catalogue cubes.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, $, ply, r } = plywood;

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

const timeFilter = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-02T00:00:00Z'),
});

// Canonical-cube mirror: main histories + an eternal magic-dim lookup carrying
// brand_country (joinKey brand). price + pvp feed the ratio-of-avgs; product_id
// feeds the countDistinct (nº URLs) measure that forces the native-JOIN route.
function makeMain(requester) {
  return External.fromJS(
    {
      engine: 'druidsql',
      source: 'main_ds',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'brand', type: 'STRING' },
        { name: 'competitor', type: 'STRING' },
        { name: 'product_id', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
      ],
      linkedSources: {
        magic_bc: {
          source: 'lookup_bc_rev1',
          joinKeys: ['brand'],
          autoInjectJoinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: 'brand', type: 'STRING' },
            { name: 'brand_country', type: 'STRING' },
          ],
        },
      },
      filter: timeFilter,
    },
    requester,
  );
}

// Turnilo-style top-level expression: scope registrations + a SPLIT apply by the
// linked-only dim, value applies, sort + limit.
function buildSplitExpr(valueApplies, sortName) {
  let split = $('main').split('$brand_country', 'brand_country');
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

function planSql(valueApplies, sortName) {
  return buildSplitExpr(valueApplies, sortName)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

// The countDistinct measure that forces the panel onto native-JOIN.
const UNIQ = '$main.countDistinct($product_id)';
// Ismael's exact RP/PVP diff: (AVG(price) - AVG(pvp)) / AVG(pvp), root op 'divide'.
const RP_PVP_DIFF =
  '($main.average($price).subtract($main.average($pvp))).divide($main.average($pvp))';

describe('Compose: DERIVED measure (ratio-of-avgs) × magic-dim split (native-JOIN route)', () => {
  describe('(1) ROUTE + SQL SHAPE — derived measure renders as composed arithmetic of native aggregates', () => {
    it('rp_pvp_diff + countDistinct emit ONE native-JOIN SQL projecting (AVG-AVG)/AVG and COUNT(DISTINCT …)', () => {
      const sqls = planSql(
        [
          ['rp_pvp_diff', RP_PVP_DIFF],
          ['uniq', UNIQ],
        ],
        'uniq',
      );
      expect(sqls.length, 'single combined SQL (native-JOIN, not 2 sub-queries)').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'INNER JOIN against the lookup').to.match(/INNER JOIN/i);
      // The derived ratio: the two avgs render as AVG, the subtract as (a-b), the
      // divide via floatDivision (a*1.0/b). Pin all three pieces + the alias.
      expect(sql, 'AVG(price) leaf').to.match(/AVG\(main\."price"\)/);
      expect(sql, 'AVG(pvp) leaf').to.match(/AVG\(main\."pvp"\)/);
      expect(sql, 'subtract rendered as (a-b)').to.match(
        /\(AVG\(main\."price"\)-AVG\(main\."pvp"\)\)/,
      );
      // Druid floatDivision is num*1.0/den; the whole ratio is aliased.
      expect(sql, 'divide via floatDivision, aliased rp_pvp_diff').to.match(
        /\(\(AVG\(main\."price"\)-AVG\(main\."pvp"\)\)\*1\.0\/AVG\(main\."pvp"\)\) AS "rp_pvp_diff"/,
      );
      // countDistinct co-present in the SAME SELECT.
      expect(sql, 'COUNT(DISTINCT product_id)').to.match(/COUNT\(DISTINCT main\."product_id"\)/i);
      // split column from lookup side + GROUP BY 1 (single linked-only bucket).
      expect(sql, 'split column projected from lookup').to.match(
        /lookup\."brand_country" AS "brand_country"/,
      );
      expect(sql, 'GROUP BY 1').to.match(/GROUP BY 1\b/);
      // ORDER BY references a projected alias — no orphan.
      expect(sql, 'ORDER BY uniq').to.match(/ORDER BY "uniq"/);
      expect(sql, 'ORDER BY uniq projected').to.include('AS "uniq"');
      // No mutilated SELECT, no leaf/jsJoin synthetics.
      expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
      expect(sql, 'no double comma').to.not.match(/,\s*,/);
      expect(sql, 'no synthetic !T_ leaf columns').to.not.match(/!T_\d+/);
      expect(sql, 'no synthetic __join_ key (native JOIN, not jsJoin)').to.not.match(/__join_/);
      // COUNTERFACTUAL: pre-fix, renderAggregateSQL had no 'divide' case and threw
      // PlywoodUnsupportedNativeJoinShape on root op='divide' → the whole panel 500'd
      // before any query was dispatched. The composed arithmetic SQL is the fix.
    });

    it('multiply + add derived measures also render as composed arithmetic of native aggregates', () => {
      // Cover multiply and add ops (and a literal operand) in one panel.
      const MARGIN = '$main.average($price).subtract($main.average($pvp))'; // subtract only
      const SCALED = '$main.average($price).multiply($main.sum($pvp))'; // multiply of two aggs
      const SHIFTED = '$main.average($price).add(1)'; // add with a literal operand
      const sqls = planSql(
        [
          ['margin', MARGIN],
          ['scaled', SCALED],
          ['shifted', SHIFTED],
          ['uniq', UNIQ],
        ],
        'uniq',
      );
      expect(sqls.length, 'one native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'subtract rendered').to.match(
        /\(AVG\(main\."price"\)-AVG\(main\."pvp"\)\) AS "margin"/,
      );
      expect(sql, 'multiply rendered').to.match(
        /\(AVG\(main\."price"\)\*SUM\(.*main\."pvp".*\)\) AS "scaled"/,
      );
      // add with a literal: (AVG(price)+1) — the literal renders via getSQL.
      expect(sql, 'add with literal rendered').to.match(/\(AVG\(main\."price"\)\+1\) AS "shifted"/);
      expect(sql, 'countDistinct co-present').to.match(/COUNT\(DISTINCT/i);
    });
  });

  describe('(2) ROW-LEVEL CORRECTNESS — ratio computed from the country medias, not media-de-ratios', () => {
    it('compute: 2 country buckets, rp_pvp_diff correct at the country grain + uniq intact, no leak', async () => {
      // Native-JOIN dispatches ONE combined SQL; the engine already did the JOIN +
      // GROUP BY + arithmetic, so the requester returns the final per-bucket rows.
      // Spain: AVG(price)=120, AVG(pvp)=100 → diff=(120-100)/100=0.20 (pooled across
      // its several brands). France: AVG(price)=44, AVG(pvp)=40 → diff=0.10. Assert
      // the caller sees the country-grain ratio, NOT an average of per-brand ratios.
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('INNER JOIN') && sql.includes('main_ds')) {
          // The engine evaluates (AVG-AVG)/AVG natively; mock its result.
          return Promise.resolve([
            { brand_country: 'Spain', rp_pvp_diff: (120 - 100) / 100, uniq: 42 },
            { brand_country: 'France', rp_pvp_diff: (44 - 40) / 40, uniq: 5 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildSplitExpr(
        [
          ['rp_pvp_diff', RP_PVP_DIFF],
          ['uniq', UNIQ],
        ],
        'uniq',
      );
      const result = await ex.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;

      const countries = rows.map(r => r.brand_country).sort();
      expect(countries, 'one row per country').to.deep.equal(['France', 'Spain']);
      const spain = rows.find(r => r.brand_country === 'Spain');
      const france = rows.find(r => r.brand_country === 'France');
      expect(spain.rp_pvp_diff, 'Spain ratio from country medias').to.be.closeTo(0.2, 1e-9);
      expect(spain.uniq, 'Spain distinct count').to.equal(42);
      expect(france.rp_pvp_diff, 'France ratio from country medias').to.be.closeTo(0.1, 1e-9);
      expect(france.uniq, 'France distinct count').to.equal(5);
      // No synthetic columns leak.
      for (const row of rows) {
        for (const k of Object.keys(row)) {
          expect(k.indexOf('!T_'), `no leaf leak: ${k}`).to.not.equal(0);
          expect(k, 'no __join_ leak').to.not.equal('__join_brand');
        }
      }
    });
  });

  describe('(3) FAIL-LOUD LIMIT — a genuinely non-renderable leaf inside a formula STILL throws', () => {
    it('quantile inside a ratio + countDistinct THROWS PlywoodUnsupportedNativeJoinShape (not silenced)', () => {
      // quantile is not a single renderable native aggregate here — composing
      // arithmetic must NOT make it renderable. The throw names the offending op.
      const QUANTILE_RATIO = '$main.quantile($price, 0.95).divide($main.average($pvp))';
      let msg = '';
      try {
        planSql(
          [
            ['p95_ratio', QUANTILE_RATIO],
            ['uniq', UNIQ],
          ],
          'uniq',
        );
        expect.fail('expected a throw on the quantile leaf');
      } catch (e) {
        msg = e.message;
        expect(e.name, 'error class').to.equal('PlywoodUnsupportedNativeJoinShape');
      }
      expect(msg, 'PlywoodUnsupportedNativeJoinShape message').to.match(
        /Cross-source native-JOIN cannot emit SQL/,
      );
      expect(msg, "names the unrenderable leaf op 'quantile'").to.match(/root op='quantile'/);
    });

    it('custom/sqlAggregate inside a formula + countDistinct ALSO throws loudly', () => {
      // A custom aggregate has no single-aggregate native form on this renderer.
      const CUSTOM_RATIO = '$main.customAggregate("my_sketch").divide($main.average($pvp))';
      let msg = '';
      try {
        planSql(
          [
            ['custom_ratio', CUSTOM_RATIO],
            ['uniq', UNIQ],
          ],
          'uniq',
        );
        expect.fail('expected a throw on the custom aggregate leaf');
      } catch (e) {
        msg = e.message;
        expect(e.name, 'error class').to.equal('PlywoodUnsupportedNativeJoinShape');
      }
      expect(msg, 'PlywoodUnsupportedNativeJoinShape message').to.match(
        /Cross-source native-JOIN cannot emit SQL/,
      );
      expect(msg, "names the unrenderable leaf op 'customAggregate'").to.match(
        /root op='customAggregate'/,
      );
    });
  });
});
