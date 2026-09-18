/*
 * A filter on a column that lives only on a LEFT-joined linked source (a
 * mapping dimension: Druid main × Postgres mapping view, joinMode left).
 *
 * Seen on rc, 18 Sep 2026, cube histories-5109a4e1…, mapping
 * `generated_image` keyed by `imageUrl`:
 *   - filter generated_image = <url> + split productName → HTTP 500
 *     ("semijoin-to-root … joinMode=left cannot honour a linked-only filter").
 *   - filter generated_image = <url> + split generated_image → HTTP 200 but
 *     WRONG: the filter reached only the Postgres side, the Druid query ran
 *     unfiltered and the left join kept every orphan row, so the panel showed
 *     the whole cube as an "undefined" bucket (554280) next to the matched
 *     rows (13875), and the totals (568155) ignored the filter.
 *   - the same totals leak exists for an INNER magic dimension when the
 *     filtered column is also the split column (the "live consumer" stood the
 *     semijoin-to-root down for every external, the never-decomposing totals
 *     one included).
 *
 * Rule pinned here: a harvested linked-only clause that no orphan main row can
 * satisfy (`filterRejectsOrphans`: comparisons of a linked-only column against
 * non-null literals) makes LEFT and INNER agree, so for that request the
 * lookup is honoured through the inner-only machinery — the semijoin-to-root
 * IN-list on main (totals and main-side splits) and an inner in-memory join
 * (linked-only splits). Negations keep the left semantics and the old refusal.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');
const { External, Expression, $, ply } = plywood;

const MAP = 'mapping_4df31318';
const MAG = 'magic_c717fdfa';
const IMG = 'https://m.media-amazon.com/images/I/61JxMdQR-FL._SL1500_.jpg';

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

function sqlOf(rq) {
  const q = rq && rq.query;
  if (typeof q === 'string') return q;
  return (q && q.query) || '';
}

// Mirrors the rc cube: druidsql main, a Postgres mapping view left-joined on
// imageUrl, and a Druid lookup (magic dim) inner-joined on competitor.
function makeMain(requester, pgRequester) {
  const value = {
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: '__time',
    allowEternity: true,
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'productName', type: 'STRING' },
      { name: 'imageUrl', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
    ],
    linkedSources: {
      [MAP]: {
        source: 'mapping_view_4df31318',
        joinKeys: ['imageUrl'],
        autoInjectJoinKeys: ['imageUrl'],
        sharedDimensions: ['imageUrl'],
        joinMode: 'left',
        engine: 'postgres',
        version: '16.0.0',
        attributes: [
          { name: 'imageUrl', type: 'STRING' },
          { name: 'generated_image', type: 'STRING' },
        ],
      },
      [MAG]: {
        source: 'lookup_c717fdfa_rev1',
        joinKeys: ['competitor'],
        autoInjectJoinKeys: ['competitor'],
        sharedDimensions: ['competitor'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'competitor', type: 'STRING' },
          { name: 'tienda', type: 'STRING' },
        ],
      },
    },
  };
  const main = External.fromJS(value, requester);
  main.linkedSources[MAP].requester =
    pgRequester ||
    (() => {
      throw new Error('postgres requester not expected in this test');
    });
  return main;
}

const TIME = $('__time').overlap(
  new Date('2026-09-11T00:00:00Z'),
  new Date('2026-09-18T00:00:00Z'),
);
const lit = v => Expression.fromJS({ op: 'literal', value: v });

// The shape the Turnilo front POSTs: the same filter stamped on main and on
// every linked-source apply; totals `count` + a SPLIT.
function query(filter, splits) {
  return ply()
    .apply('main', $('main').filter(filter))
    .apply(MAP, $(MAP).filter(filter))
    .apply(MAG, $(MAG).filter(filter))
    .apply('count', $('main').count())
    .apply(
      'SPLIT',
      $('main')
        .split(splits)
        .apply('count', $('main').count())
        .sort($('count'), 'descending')
        .limit(50),
    );
}

function planSqls(expression) {
  return expression
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .map(q => (typeof q === 'string' ? q : q && q.query))
    .filter(q => typeof q === 'string');
}
const totalsSql = sqls => sqls.find(s => s.includes('"histories"') && /GROUP BY \(\)/.test(s));
// The OUTER `GROUP BY 1` (followed by a newline or the end) — a same-engine
// semijoin embeds `… GROUP BY 1)` inside its IN sub-query, which is not a split.
const mainSplitSql = sqls =>
  sqls.find(s => s.includes('"histories"') && /GROUP BY 1(?!\))/.test(s));
const mappingSql = sqls => sqls.find(s => s.includes('mapping_view_4df31318'));
const lookupSql = sqls => sqls.find(s => s.includes('lookup_c717fdfa_rev1'));

describe('linkedFilterRejectsOrphans', () => {
  const linkedOnly = { generated_image: true };
  const cases = [
    ['is literal', $('generated_image').is(lit(IMG)), true],
    ['overlap set', $('generated_image').overlap(lit(plywood.Set.fromJS([IMG, 'x']))), true],
    ['contains', $('generated_image').contains(lit('amazon')), true],
    ['literal-first lessThan', lit('a').lessThan($('generated_image')), true],
    ['and with a main clause', TIME.and($('generated_image').is(lit(IMG))), true],
    [
      'or of two rejecting',
      $('generated_image')
        .is(lit(IMG))
        .or($('generated_image').is(lit('y'))),
      true,
    ],
    [
      'or with a main clause',
      $('generated_image')
        .is(lit(IMG))
        .or($('productName').is(lit('P'))),
      false,
    ],
    ['not', $('generated_image').is(lit(IMG)).not(), false],
    ['is null', $('generated_image').is(lit(null)), false],
    [
      'set containing null',
      $('generated_image').overlap(
        lit(plywood.Set.fromJS({ setType: 'STRING', elements: [IMG, null] })),
      ),
      false,
    ],
    ['main column only', $('productName').is(lit('P')), false],
    ['time only', TIME, false],
  ];
  for (const [name, f, expected] of cases) {
    it(`${name} → ${expected}`, () => {
      expect(External.linkedFilterRejectsOrphans(f, linkedOnly)).to.equal(expected);
    });
  }
});

describe('LEFT-joined mapping dimension: filter on the linked-only column', () => {
  describe('main-side split (productName) — used to throw', () => {
    const ex = () =>
      query(TIME.and($('generated_image').is(lit(IMG))), { productName: $('productName') });

    it('plans without throwing: the mapping view is queried for its keys, main gets the IN-list', () => {
      const sqls = planSqls(ex());
      const mapping = mappingSql(sqls);
      expect(mapping, 'mapping key sub-query exists').to.exist;
      expect(mapping).to.match(/generated_image/);
      expect(mapping).to.include(IMG);
      const totals = totalsSql(sqls);
      expect(totals, 'totals exist').to.exist;
      expect(totals, 'totals restricted by imageUrl IN-list').to.match(/"imageUrl"/);
      const split = mainSplitSql(sqls);
      expect(split, 'main split exists').to.exist;
      expect(split, 'main split restricted by imageUrl IN-list').to.match(/WHERE[\s\S]*"imageUrl"/);
      expect(split, 'main split never references the linked column').to.not.match(
        /generated_image/,
      );
    });

    it('computes: only the products whose image maps to the filtered value survive', async () => {
      const seen = [];
      const druid = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        seen.push(sql);
        const filtered = /"imageUrl"/.test(sql.split('WHERE')[1] || '');
        if (/GROUP BY \(\)/.test(sql))
          return Promise.resolve([{ __VALUE__: filtered ? 100 : 1000 }]);
        return Promise.resolve(
          filtered
            ? [{ productName: 'P1', count: 100 }]
            : [
                { productName: 'P1', count: 100 },
                { productName: 'P2', count: 900 },
              ],
        );
      });
      const pg = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        seen.push(sql);
        expect(sql, 'postgres key query carries the filter').to.include(IMG);
        return Promise.resolve([{ imageUrl: 'u1' }]);
      });
      const main = makeMain(druid, pg);
      const ds = await ex().compute({ main });
      const row = ds.toJS().data[0];
      expect(row.count, 'totals honour the filter').to.equal(100);
      expect(row.SPLIT.data.map(r => r.productName)).to.deep.equal(['P1']);
      expect(
        seen.some(s => s.includes('mapping_view_4df31318')),
        'postgres was asked for the keys',
      ).to.equal(true);
    });
  });

  describe('linked-only split (generated_image) — used to keep every orphan row', () => {
    const ex = () =>
      query(TIME.and($('generated_image').is(lit(IMG))), {
        productName: $('productName'),
        generated_image: $('generated_image'),
      });

    it('plans: mapping side filtered, totals restricted by the IN-list', () => {
      const sqls = planSqls(ex());
      expect(mappingSql(sqls)).to.include(IMG);
      expect(totalsSql(sqls), 'totals restricted by imageUrl IN-list').to.match(/"imageUrl"/);
    });

    it('computes: orphan main rows are dropped (inner for this request), totals filtered', async () => {
      const druid = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        const filtered = /"imageUrl"/.test(sql.split('WHERE')[1] || '');
        if (/GROUP BY \(\)/.test(sql))
          return Promise.resolve([{ __VALUE__: filtered ? 100 : 1000 }]);
        // main leaves at join-key grain: u1 maps to IMG, u2 is an orphan
        return Promise.resolve([
          { __join_imageUrl: 'u1', productName: 'P1', count: 100 },
          { __join_imageUrl: 'u2', productName: 'P2', count: 900 },
        ]);
      });
      const pg = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        expect(sql).to.include(IMG);
        if (sql.includes('__join_imageUrl')) {
          return Promise.resolve([{ __join_imageUrl: 'u1', generated_image: IMG }]);
        }
        return Promise.resolve([{ imageUrl: 'u1' }]);
      });
      const ds = await ex().compute({ main: makeMain(druid, pg) });
      const row = ds.toJS().data[0];
      expect(row.count, 'totals honour the filter').to.equal(100);
      expect(row.SPLIT.data).to.deep.equal([
        { productName: 'P1', generated_image: IMG, count: 100 },
      ]);
    });
  });

  describe('negated filter keeps the left semantics', () => {
    it('main-side split still refuses loudly (an IN-list would drop orphans the user asked to keep)', () => {
      const ex = query(TIME.and($('generated_image').is(lit(IMG)).not()), {
        productName: $('productName'),
      });
      expect(() => planSqls(ex)).to.throw(/joinMode="left" cannot honour a linked-only filter/);
    });

    it('linked-only split keeps the orphan rows', async () => {
      const ex = query(TIME.and($('generated_image').is(lit(IMG)).not()), {
        productName: $('productName'),
        generated_image: $('generated_image'),
      });
      const druid = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        if (/GROUP BY \(\)/.test(sql)) return Promise.resolve([{ __VALUE__: 1000 }]);
        return Promise.resolve([
          { __join_imageUrl: 'u1', productName: 'P1', count: 100 },
          { __join_imageUrl: 'u2', productName: 'P2', count: 900 },
        ]);
      });
      const pg = promiseFnToStream(() =>
        Promise.resolve([{ __join_imageUrl: 'u1', generated_image: 'other' }]),
      );
      const ds = await ex.compute({ main: makeMain(druid, pg) });
      const names = ds
        .toJS()
        .data[0].SPLIT.data.map(r => r.productName)
        .sort();
      expect(names).to.deep.equal(['P1', 'P2']);
    });
  });

  describe('no linked filter — inert', () => {
    it('the left join keeps orphan rows and the mapping view is fetched for the result keys only', async () => {
      const ex = query(TIME, {
        productName: $('productName'),
        generated_image: $('generated_image'),
      });
      const sqls = planSqls(ex);
      // Enrichment by result keys (0.51.10): the mapped column is only
      // displayed, so main runs first and the Postgres view is asked for the
      // result's image URLs — `WHERE "imageUrl" IN (…)` — not scanned whole.
      // No linked clause reaches the view and the totals stay unrestricted.
      expect(mappingSql(sqls)).to.match(/WHERE \("imageUrl" (IN \(|IS NOT DISTINCT FROM )/);
      expect(mappingSql(sqls)).to.not.match(/"generated_image"\s*(=|IN |IS NOT DISTINCT)/);
      expect(totalsSql(sqls)).to.not.match(/"imageUrl"/);
      const druid = promiseFnToStream(rq => {
        const sql = sqlOf(rq);
        if (/GROUP BY \(\)/.test(sql)) return Promise.resolve([{ __VALUE__: 1000 }]);
        return Promise.resolve([
          { __join_imageUrl: 'u1', productName: 'P1', count: 100 },
          { __join_imageUrl: 'u2', productName: 'P2', count: 900 },
        ]);
      });
      const pg = promiseFnToStream(() =>
        Promise.resolve([{ __join_imageUrl: 'u1', generated_image: IMG }]),
      );
      const ds = await ex.compute({ main: makeMain(druid, pg) });
      const rows = ds.toJS().data[0].SPLIT.data;
      expect(rows.map(r => r.productName).sort()).to.deep.equal(['P1', 'P2']);
      expect(
        rows.find(r => r.productName === 'P2').generated_image,
        'orphan keeps the column undefined',
      ).to.equal(undefined);
    });
  });
});

describe('INNER magic dimension: filter and split on the same linked column', () => {
  it('the totals are restricted too (they never decompose, so they need the IN-list)', () => {
    const ex = query(TIME.and($('tienda').is(lit('Farmacia'))), { tienda: $('tienda') });
    const sqls = planSqls(ex);
    expect(lookupSql(sqls), 'lookup filtered').to.include('Farmacia');
    // Same-engine magic lookup: the totals restriction is an IN sub-query the
    // engine resolves itself (no key list through plywood).
    expect(totalsSql(sqls), 'totals restricted by a competitor IN sub-query').to.match(
      /"competitor" IN \(SELECT "competitor"[\s\S]*"lookup_c717fdfa_rev1"[\s\S]*'Farmacia'/,
    );
    // …and the split by the linked column is ONE native JOIN, not a second
    // IN-list on the main leaves.
    const split = mainSplitSql(sqls);
    expect(split, 'split is the native JOIN').to.match(
      /INNER JOIN "lookup_c717fdfa_rev1" AS lookup/,
    );
    expect(split).to.not.match(/WHERE[\s\S]*"competitor"\s*IN/);
  });
});
