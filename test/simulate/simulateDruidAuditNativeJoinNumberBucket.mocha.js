/*
 * AUDIT — native-JOIN multi-key split where the MAIN-SIDE key is a NUMBER bucket
 * (`$price.numberBucket(10)`) instead of a time bucket, paired with a linked-only
 * magic dim (`brand_country`) and a non-decomposable countDistinct that forces
 * the whole panel onto the native-JOIN path.
 *
 * WHY THIS SHAPE. The native-JOIN renderer learned to carry a MAIN-SIDE split key
 * (fix B) so a `[Time(Day) × brand_country]` double split keeps the time
 * dimension (see simulateDruidComposeSubquerySplitMagicDim §5). The mechanism is
 * NOT time-specific: `getNativeJoinDecomposition` collects every alias whose free
 * refs resolve on the main side into `mainSideSplitAliases`, then renders each via
 * `msExpr.getSQL(dialect)` against the `main` alias, projects it FIRST, and pushes
 * its position into the GROUP BY. The execution layer inflates each main-side key
 * via `getIntelligentInflater(splitExpr, name)`.
 *
 * The classification at baseExternal.ts ~4480 is REF-BASED, not shape-based:
 * `$price.numberBucket(10)` has free ref `price`, which lives in the main attrs →
 * the alias lands in `mainSideSplitAliases` exactly like a time bucket. The
 * renderer then walks the SAME code path: `numberBucket._getSQLChainableHelper`
 * emits `FLOOR(main."price" / 10) * 10` (continuousFloorExpression), and
 * `getIntelligentInflater` has an explicit `NumberBucketExpression` arm
 * (baseExternal.ts ~812 → `numberRangeInflaterFactory(label, size)`), so the
 * FLOOR'd integer inflates to a NumberRange cell.
 *
 * THE AUDIT QUESTION (A / B / C): does the native-JOIN multi-key path treat a
 * numeric bucket main-side key IDENTICALLY to a time bucket — i.e. does the
 * emitted SQL PROJECT the FLOOR(...) bucket AND GROUP BY both positions, and does
 * compute inflate it to a NumberRange and key on BOTH dimensions? Or is the bucket
 * silently dropped / mis-projected (the [Time × country] collapse bug, but for a
 * number bucket)?
 *
 * Every assertion carries a counterfactual. The shapes mirror the canonical
 * GrupoIfa-style cube: price (unsplitable NUMBER) bucketed main-side,
 * countDistinct(productId||competitor) the universal 'none'-trait measure.
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

const timeFilter = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-03T00:00:00Z'),
});

// Main cube espejo + eternal magic-dim lookup (joinKey brand, brand_country
// linked-only). price is an unsplitable NUMBER measure — but numberBucket makes
// it a SPLIT dimension main-side, which is exactly the audit subject.
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
        { name: 'productId', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
        { name: 'pvp', type: 'NUMBER', unsplitable: true },
      ],
      filter: timeFilter,
      linkedSources: {
        magic_bc: {
          source: 'lookup_bc_rev1',
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

const UNIQ = '$main.countDistinct($productId.concat($competitor))';

// Double split: a NUMBER bucket on main's price + the linked-only brand_country.
function buildDoubleSplitExpr(valueApplies, sortName) {
  let split = $('main').split({
    price_bucket: '$price.numberBucket(10)',
    brand_country: '$brand_country',
  });
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  return ply()
    .apply('main', $('main').filter(timeFilter))
    .apply('magic_bc', $('magic_bc').filter(timeFilter))
    .apply('SPLIT', split);
}

function planSql(expr) {
  return expr
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('AUDIT: native-JOIN [price.numberBucket(10) × brand_country] + countDistinct', () => {
  describe('SQL shape — does the numeric bucket project + GROUP BY like a time bucket?', () => {
    it('emits ONE native-JOIN SQL that PROJECTS the FLOOR bucket and GROUP BYs both positions', () => {
      const sqls = planSql(buildDoubleSplitExpr([['uniq', UNIQ]]));
      expect(sqls.length, 'single native-JOIN SQL (not 2 sub-queries)').to.equal(1);
      const sql = sqls[0];

      // The native JOIN itself.
      expect(sql, 'INNER JOIN against the lookup').to.match(/INNER JOIN/i);

      // (C-guard) The numeric bucket must be PROJECTED main-side via FLOOR, aliased
      // to the split name. continuousFloorExpression(size=10, offset=0) →
      // FLOOR(<price> / 10) * 10. If the bucket were silently dropped (the [Time ×
      // country] collapse bug, for a number) there would be no FLOOR at all.
      expect(sql, 'price bucket projected via FLOOR(... / 10) * 10').to.match(
        /FLOOR\(\s*"?main"?\."price"\s*\/\s*10\s*\)\s*\*\s*10\s+AS\s+"price_bucket"/i,
      );

      // The linked-only key projected from the lookup side.
      expect(sql, 'brand_country projected from lookup').to.match(
        /lookup\."brand_country" AS "brand_country"/,
      );

      // The countDistinct(concat) measure still renders correctly in the same SQL.
      expect(sql, 'countDistinct over the concat key').to.match(
        /COUNT\(DISTINCT \("?main"?\."productId"\|\|"?main"?\."competitor"\)\) AS "uniq"/i,
      );

      // (C-guard) GROUP BY must cover BOTH split-key positions — never the
      // mutilated `GROUP BY 1` that would collapse all price buckets into one
      // per-country row and make `uniq` an all-bucket distinct count.
      const gb = sql.match(/GROUP BY ([\d,\s]+)/);
      expect(gb, 'a positional GROUP BY exists').to.exist;
      const positions = gb[1].split(',').map(s => parseInt(s.trim(), 10));
      expect(positions.length, 'exactly two split-key positions').to.equal(2);
      expect(
        Math.max(...positions),
        'GROUP BY covers BOTH the price bucket and the country key',
      ).to.be.at.least(2);

      // No mutilated SELECT.
      expect(sql, 'no dangling comma before FROM').to.not.match(/,\s*\n?\s*FROM/i);
      expect(sql, 'no empty select item').to.not.match(/,\s*,/);

      // ORDER BY references a projected alias — no orphan.
      const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
      if (orderMatch) {
        expect(sql, `ORDER BY "${orderMatch[1]}" must be projected`).to.include(
          `AS "${orderMatch[1]}"`,
        );
      }

      // Counterfactual: if numberBucket were NOT classified main-side (or the
      // renderer ignored mainSideSplitAliases for non-time buckets), there would
      // be no FLOOR in SELECT and GROUP BY would be `1` — the price-bucket
      // dimension would vanish silently. The FLOOR projection + GROUP BY 1,2 is
      // the correct (time-bucket-parity) behavior.
    });

    it('avg (fix A) + countDistinct over the number-bucket double split keeps both keys + both measures', () => {
      const sqls = planSql(
        buildDoubleSplitExpr(
          [
            ['avg_price', '$main.average($price)'],
            ['uniq', UNIQ],
          ],
          'uniq',
        ),
      );
      expect(sqls.length, 'single native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'FLOOR price bucket projected').to.match(
        /FLOOR\(\s*"?main"?\."price"\s*\/\s*10\s*\)/i,
      );
      expect(sql, 'avg rendered as AVG (not the buggy divide)').to.match(
        /AVG\(main\."price"\) AS "avg_price"/,
      );
      expect(sql, 'countDistinct co-present').to.match(/COUNT\(DISTINCT/i);
      expect(sql, 'no synthetic !T_ leaf columns (native JOIN, not jsJoin)').to.not.match(/!T_\d+/);
      const gb = sql.match(/GROUP BY ([\d,\s]+)/);
      const positions = gb[1].split(',').map(s => parseInt(s.trim(), 10));
      expect(positions.length, 'two split-key positions').to.equal(2);
    });
  });

  describe('Row-level correctness — rows carry an inflated NumberRange bucket AND brand_country', () => {
    it('compute: one row per (price_bucket, country); price_bucket is a NumberRange', async () => {
      // The engine groups by (FLOOR(price/10)*10, brand_country); the requester
      // returns one row per cell with the FLOOR'd integer as the raw value.
      // getIntelligentInflater must turn it into a NumberRange via
      // numberRangeInflaterFactory(label, 10).
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        if (sql.includes('histories_main') && sql.includes('lookup_bc_rev1')) {
          return Promise.resolve([
            { price_bucket: 0, brand_country: 'Spain', uniq: 5 },
            { price_bucket: 10, brand_country: 'Spain', uniq: 4 },
            { price_bucket: 0, brand_country: 'France', uniq: 2 },
          ]);
        }
        return Promise.resolve([]);
      });
      const ex = buildDoubleSplitExpr([['uniq', UNIQ]]);
      const result = await ex.compute({ main: makeMain(req) });
      const rows = result.toJS().data[0].SPLIT.data;

      // CORRECT cardinality: 3 distinct (bucket, country) cells — buckets NOT
      // collapsed onto country.
      expect(rows.length, 'three (bucket,country) cells').to.equal(3);

      // (C-guard) Every row must carry the requested price_bucket dimension, and
      // it must be an inflated NumberRange ({ start, end }) — NOT a bare number,
      // NOT missing. A missing/scalar price_bucket would mean the main-side key
      // was dropped or not inflated.
      for (const r of rows) {
        expect(
          Object.prototype.hasOwnProperty.call(r, 'price_bucket'),
          'price_bucket present on the row',
        ).to.equal(true);
        expect(r.price_bucket, 'price_bucket is an object (NumberRange)').to.be.an('object');
        expect(r.price_bucket && r.price_bucket.start, 'NumberRange has a start').to.not.be
          .undefined;
        expect(r.price_bucket && r.price_bucket.end, 'NumberRange has an end').to.not.be.undefined;
      }

      const cell = (start, country) =>
        rows.find(r => r.price_bucket.start === start && r.brand_country === country);
      expect(cell(0, 'Spain'), '[0,10)/Spain present').to.exist;
      expect(cell(10, 'Spain'), '[10,20)/Spain present').to.exist;
      expect(cell(0, 'France'), '[0,10)/France present').to.exist;
      expect(cell(0, 'Spain').uniq, '[0,10)/Spain per-bucket distinct count').to.equal(5);
      expect(cell(10, 'Spain').uniq, '[10,20)/Spain per-bucket distinct count').to.equal(4);
      expect(cell(0, 'France').uniq, '[0,10)/France per-bucket distinct count').to.equal(2);

      // NumberRange width matches the bucket size.
      expect(cell(0, 'Spain').price_bucket.end, '[0,10) range width = size 10').to.equal(10);

      // No synthetic columns leak to the caller.
      for (const r of rows) {
        for (const k of Object.keys(r)) {
          expect(k.indexOf('!T_'), `no leaf column leak: ${k}`).to.not.equal(0);
          expect(k, 'no __join_ leak').to.not.equal('__join_brand');
        }
      }
    });
  });

  describe('Orthogonality — number-bucket split ALONE (no linked dim) never touches the lookup', () => {
    it('split by price.numberBucket(10) ALONE emits a single main query, lookup untouched', () => {
      let split = $('main').split('$price.numberBucket(10)', 'price_bucket');
      split = split.apply('uniq', UNIQ).sort('$uniq', 'descending').limit(100);
      const expr = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('SPLIT', split);
      const sqls = planSql(expr);
      const lookupSqls = sqls.filter(s => s.includes('lookup_bc_rev1'));
      expect(lookupSqls.length, 'lookup not queried for a number-only split').to.equal(0);
      const mainSql = sqls.find(s => s.includes('"histories_main"'));
      expect(mainSql, 'main sub-query exists').to.exist;
      expect(mainSql, 'no native INNER JOIN for a non-linked split').to.not.match(/INNER JOIN/i);
      expect(mainSql, 'numberBucket renders natively main-side').to.match(/FLOOR/i);
    });
  });
});
