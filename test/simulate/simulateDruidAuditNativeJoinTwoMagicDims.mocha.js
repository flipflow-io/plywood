/*
 * AUDIT test — TWO magic dimensions in the SAME panel on the native-JOIN route.
 *
 * Capability under examination: a panel that splits by TWO linked-only
 * dimensions (e.g. `brand_country` AND `brand_segment`, both resolvable ONLY in
 * a lookup and NOT in main) while carrying a NON-decomposable measure
 * (`countDistinct`, trait 'none'). The 'none' leaf diverts the whole panel to
 * the native-JOIN path (getNativeJoinDecomposition). The question this file
 * answers, with real SQL/row evidence:
 *
 *   Does the native-JOIN emit TWO inner joins (one per lookup) and GROUP BY
 *   BOTH linked keys — i.e. a genuine [country × segment] grid? Or does it only
 *   support ONE linked key (length-1 guard) and either (B) FAIL LOUD with a
 *   clear PlywoodUnsupportedNativeJoinShape, or (C) SILENTLY drop one dimension
 *   / ignore one lookup / collapse the grid (wrong numbers, no error)?
 *
 * Two sub-shapes are exercised:
 *   - TWO lookups (brand_country from magic_bc, brand_segment from magic_seg),
 *     both joinKey 'brand'. This hits BOTH the length-1 split guard AND the
 *     single-lookup guard.
 *   - ONE lookup carrying TWO linked-only columns (brand_country + brand_segment
 *     in the same lookup), split by both. This isolates the length-1 SPLIT guard
 *     from the single-LOOKUP guard.
 *
 * Classification: this is DIAGNOSIS, not a fix. Each test pins observed behavior
 * so a future multi-magic-dim native-JOIN implementation flips it loudly.
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
  end: new Date('2026-05-02T00:00:00Z'),
});

const UNIQ = '$main.countDistinct($product_id.concat($competitor))';

// ---------------------------------------------------------------------------
// SHAPE 1 — TWO distinct lookups, each contributing one linked-only dim.
// magic_bc → brand_country (lookup_bc_rev1); magic_seg → brand_segment
// (lookup_seg_rev1). Both joinKey 'brand'.
// ---------------------------------------------------------------------------
function makeMainTwoLookups(requester) {
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
        magic_seg: {
          source: 'lookup_seg_rev1',
          joinKeys: ['brand'],
          autoInjectJoinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: 'brand', type: 'STRING' },
            { name: 'brand_segment', type: 'STRING' },
          ],
        },
      },
      filter: timeFilter,
    },
    requester,
  );
}

// ---------------------------------------------------------------------------
// SHAPE 2 — ONE lookup carrying TWO linked-only columns. Isolates the length-1
// SPLIT guard from the single-LOOKUP guard.
// ---------------------------------------------------------------------------
function makeMainOneLookupTwoCols(requester) {
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
            { name: 'brand_segment', type: 'STRING' },
          ],
        },
      },
      filter: timeFilter,
    },
    requester,
  );
}

// Build a top-level expr that splits by TWO linked-only dims + value applies.
function buildTwoSplitExpr(scopeNames, splitDims, valueApplies, sortName) {
  let split = $('main').split({
    [splitDims[0]]: '$' + splitDims[0],
    [splitDims[1]]: '$' + splitDims[1],
  });
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(100);
  let ex = ply();
  for (const sn of scopeNames) ex = ex.apply(sn, $(sn).filter(timeFilter));
  ex = ex.apply('SPLIT', split);
  return ex;
}

function planSql(makeMain, scopeNames, splitDims, valueApplies, sortName) {
  return buildTwoSplitExpr(scopeNames, splitDims, valueApplies, sortName)
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('AUDIT: two magic dims in one panel + countDistinct (native-JOIN route)', () => {
  describe('SHAPE 1 — two linked-only dims from TWO distinct lookups', () => {
    it('OBSERVE: split by brand_country × brand_segment + countDistinct — throw or SQL?', () => {
      let threw = null;
      let sqls = null;
      try {
        sqls = planSql(
          makeMainTwoLookups,
          ['main', 'magic_bc', 'magic_seg'],
          ['brand_country', 'brand_segment'],
          [['uniq', UNIQ]],
          'uniq',
        );
      } catch (e) {
        threw = e;
      }
      // Print exactly what happened for the audit record.
      // eslint-disable-next-line no-console
      console.log(
        '\n[SHAPE1 two-lookups] threw=',
        threw && threw.name,
        '| msg=',
        threw && threw.message,
        '| sqls=',
        sqls && sqls.length,
      );
      if (sqls) for (const s of sqls) console.log('  SQL>', s);

      // The two-magic-dim shape MUST NOT silently produce a single SQL that
      // groups by only ONE of the two linked keys. Either it throws a clear
      // PlywoodUnsupportedNativeJoinShape (B), or — if it ever emits SQL — that
      // SQL must contain BOTH linked columns projected AND both grouped + both
      // INNER JOINs (a genuine grid). Anything in between is the silent bug (C).
      if (threw) {
        expect(threw.name, 'fail-loud is the documented v1 behavior').to.equal(
          'PlywoodUnsupportedNativeJoinShape',
        );
        // The message must name the multi-alias OR multi-lookup reason — not a
        // generic crash. With two lookups, the length-1 SPLIT guard fires first.
        expect(threw.message, 'reason names multi-alias or multi-lookup').to.match(
          /multi-alias linked-only split|native-JOIN v1 supports exactly 1|multiple linkedSources/,
        );
      } else {
        // No throw → assert it is a CORRECT grid, else this is bug C.
        expect(sqls.length, 'single combined SQL').to.equal(1);
        const sql = sqls[0];
        const joins = (sql.match(/INNER JOIN/gi) || []).length;
        expect(joins, 'TWO inner joins (one per lookup) for a real grid').to.equal(2);
        expect(sql, 'brand_country projected').to.match(/AS "brand_country"/);
        expect(sql, 'brand_segment projected').to.match(/AS "brand_segment"/);
        expect(sql, 'GROUP BY both keys, not GROUP BY 1').to.match(/GROUP BY 1, 2/);
      }
    });
  });

  describe('SHAPE 2 — two linked-only dims from ONE lookup (isolates split guard)', () => {
    it('OBSERVE: split by brand_country × brand_segment (same lookup) + countDistinct', () => {
      let threw = null;
      let sqls = null;
      try {
        sqls = planSql(
          makeMainOneLookupTwoCols,
          ['main', 'magic_bc'],
          ['brand_country', 'brand_segment'],
          [['uniq', UNIQ]],
          'uniq',
        );
      } catch (e) {
        threw = e;
      }
      // eslint-disable-next-line no-console
      console.log(
        '\n[SHAPE2 one-lookup-two-cols] threw=',
        threw && threw.name,
        '| msg=',
        threw && threw.message,
        '| sqls=',
        sqls && sqls.length,
      );
      if (sqls) for (const s of sqls) console.log('  SQL>', s);

      if (threw) {
        expect(threw.name).to.equal('PlywoodUnsupportedNativeJoinShape');
        // Here only ONE lookup is involved → the SPLIT length-1 guard must be
        // what fires (not the lookup-count guard).
        expect(threw.message, 'multi-alias split guard fires').to.match(
          /multi-alias linked-only split/,
        );
        expect(threw.message, 'names both aliases + count=2').to.match(/count=2/);
      } else {
        // If it ever emits SQL, both linked columns must be present + grouped.
        expect(sqls.length).to.equal(1);
        const sql = sqls[0];
        expect(sql, 'brand_country projected').to.match(/AS "brand_country"/);
        expect(sql, 'brand_segment projected').to.match(/AS "brand_segment"/);
        expect(sql, 'GROUP BY both keys').to.match(/GROUP BY 1, 2/);
      }
    });
  });

  describe('SANITY — single linked-only dim STILL works (guard is not over-broad)', () => {
    it('split by brand_country ALONE + countDistinct → one well-formed native-JOIN SQL', () => {
      // Orthogonality: the two-dim guard must not break the supported one-dim
      // case. Reuse the two-lookup external but split by ONLY brand_country.
      let split = $('main').split('$brand_country', 'brand_country');
      split = split.apply('uniq', UNIQ).sort('$uniq', 'descending').limit(100);
      const ex = ply()
        .apply('main', $('main').filter(timeFilter))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('magic_seg', $('magic_seg').filter(timeFilter))
        .apply('SPLIT', split);
      const sqls = ex
        .simulateQueryPlan({ main: makeMainTwoLookups() })
        .flat()
        .filter(q => typeof q.query === 'string')
        .map(q => q.query);
      expect(sqls.length, 'one native-JOIN SQL').to.equal(1);
      const sql = sqls[0];
      expect(sql, 'exactly ONE inner join (only magic_bc involved)').to.match(/INNER JOIN/i);
      expect((sql.match(/INNER JOIN/gi) || []).length, 'one join').to.equal(1);
      expect(sql, 'only lookup_bc joined, not lookup_seg').to.not.match(/lookup_seg_rev1/);
      expect(sql, 'brand_country projected').to.match(/AS "brand_country"/);
      expect(sql, 'GROUP BY 1').to.match(/GROUP BY 1\b/);
    });
  });

  describe('ROW-LEVEL — if a throw is the contract, computing the panel surfaces it (no silent wrong rows)', () => {
    it('compute() on the two-lookup two-dim panel rejects loudly, never returns collapsed rows', async () => {
      // The grave failure mode would be: native-JOIN silently drops brand_segment,
      // groups by brand_country only, and the engine returns rows keyed on ONE
      // dim — the user sees a [country]-only chart mislabeled as [country×segment],
      // with countDistinct collapsed across all segments. Prove compute() does NOT
      // do that: it must reject (the planner throws before any SQL dispatch).
      const req = promiseFnToStream(rq => {
        const sql = (rq && rq.query && rq.query.query) || '';
        // Should never be reached for the two-dim shape if the planner throws.
        if (sql.includes('INNER JOIN')) {
          return Promise.resolve([{ brand_country: 'Spain', uniq: 999 }]);
        }
        return Promise.resolve([]);
      });
      const ex = buildTwoSplitExpr(
        ['main', 'magic_bc', 'magic_seg'],
        ['brand_country', 'brand_segment'],
        [['uniq', UNIQ]],
        'uniq',
      );
      let rejected = null;
      let value = null;
      try {
        value = await ex.compute({ main: makeMainTwoLookups(req) });
      } catch (e) {
        rejected = e;
      }
      // eslint-disable-next-line no-console
      console.log(
        '\n[ROW-LEVEL] rejected=',
        rejected && rejected.name,
        '| msg=',
        rejected && rejected.message,
        '| value=',
        value ? JSON.stringify(value.toJS().data && value.toJS().data[0]) : null,
      );
      // EITHER it rejects loudly (acceptable, B) OR it returns a genuine grid.
      // It must NOT return a single-dim collapsed dataset silently.
      if (rejected) {
        expect(rejected, 'reject is an Error').to.be.an('error');
      } else {
        // If it resolved, the SPLIT must carry BOTH dims on every row.
        const rows = value.toJS().data[0].SPLIT.data;
        for (const r of rows) {
          expect(r, 'row carries brand_country').to.have.property('brand_country');
          expect(r, 'row carries brand_segment (NOT silently dropped)').to.have.property(
            'brand_segment',
          );
        }
      }
    });
  });
});
