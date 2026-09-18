/*
 * Raw-SQL filter predicate (custom dimension) × linked sources.
 *
 * Bug (Flipflow rc, 2026-09-17): a research panel filtered on a custom
 * dimension whose expression is a raw-SQL fragment (`s${ CASE WHEN
 * LOWER("competitor") LIKE '%amazon%' THEN 'Amazon' … END }`, plywood's
 * `SqlRefExpression`) on a cube with ten magic-attribute lookups. The front
 * stamps the main filter on `$main.filter()` and on every linked-source apply.
 * `pruneFilterToSchema` classifies a predicate by the RefExpression it
 * targets; a raw-SQL leaf has none, so the clause survived pruning to every
 * lookup's linked-only schema, `pruneLinkedFilterRefsInTree` harvested it as
 * a linked-only clause for all ten lookups and the totals/main-split
 * semijoin-to-root rescue threw:
 *   "Cross-source native-JOIN cannot emit SQL: semijoin-to-root: 10
 *    linkedSources [...] carry a linked-only filter clause at once".
 *
 * Correct semantics: raw SQL is authored against the PRIMARY table, so the
 * predicate belongs to main only. Pruning for a linked schema must drop it
 * (identity TRUE); pruning for the primary's own schema must keep it. The
 * emitted SQL then carries the CASE WHEN on the main WHERE and no lookup
 * ever sees it.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, Expression, $, ply } = plywood;

const timeFilter = $('__time').overlap({
  start: new Date('2026-09-17T00:00:00Z'),
  end: new Date('2026-09-18T00:00:00Z'),
});

const RETAILER_GROUP_SQL =
  " CASE WHEN LOWER(\"competitor\") LIKE '%amazon%' THEN 'Amazon' WHEN LOWER(\"competitor\") LIKE '%farma%' THEN 'Farmacias' ELSE 'Otros' END ";
const retailerGroup = Expression.fromJS({ op: 'sqlRef', sql: RETAILER_GROUP_SQL });
// The panel's "retailer_group is not null" clause, exactly as the front emits it.
const NOT_NULL = retailerGroup.overlap([null]).not();
const AMAZON_ONLY = retailerGroup.overlap(['Amazon']);

function lookup(name, key, column) {
  return {
    source: `lookup_${name}_rev1`,
    joinKeys: [key],
    autoInjectJoinKeys: [key],
    sharedDimensions: [key],
    joinMode: 'inner',
    timeAlignment: 'eternal',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: key, type: 'STRING' },
      { name: column, type: 'STRING' },
      { name: 'confidence', type: 'NUMBER' },
    ],
  };
}

const LINKED = {
  magic_a: lookup('a', 'category', 'qaSolar14'),
  magic_b: lookup('b', 'brand', 'brand_country'),
  magic_c: lookup('c', 'competitor', 'store_kind'),
};

function makeMain() {
  return External.fromJS({
    engine: 'druidsql',
    version: '30.0.0',
    source: 'histories',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'category', type: 'STRING' },
      { name: 'brand', type: 'STRING' },
      { name: 'productId', type: 'STRING' },
      { name: 'availability', type: 'NUMBER' },
    ],
    allowEternity: true,
    allowSelectQueries: true,
    linkedSources: LINKED,
  });
}

// `stampLinked`: the front's shape (full main filter on every linked apply).
function buildExpr(mainFilter, stampLinked, withSplit) {
  let ex = ply().apply('main', $('main').filter(mainFilter));
  if (stampLinked)
    for (const name of Object.keys(LINKED)) ex = ex.apply(name, $(name).filter(mainFilter));
  ex = ex.apply('pairs', '$main.countDistinct($productId.concat($competitor))');
  if (withSplit) {
    ex = ex.apply(
      'SPLIT',
      $('main')
        .split(retailerGroup, 'retailer_group')
        .apply('pairs', '$main.countDistinct($productId.concat($competitor))')
        .sort('$pairs', 'descending')
        .limit(25),
    );
  }
  return ex;
}

function planSql(ex) {
  return ex
    .simulateQueryPlan({ main: makeMain() })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

describe('Raw-SQL (custom dimension) filter predicate with linked sources', () => {
  describe('pruneFilterToSchema', () => {
    const schema = { category: true, qaSolar14: true };
    it('keeps a raw-SQL predicate when raw SQL is not foreign (primary schema)', () => {
      const f = timeFilter.and(NOT_NULL);
      expect(External.pruneFilterToSchema(f, { __time: true }, false).equals(f)).to.equal(true);
    });
    it('drops a raw-SQL predicate for a linked schema, keeps the rest', () => {
      const f = $('qaSolar14').overlap(['x']).and(NOT_NULL);
      const pruned = External.pruneFilterToSchema(f, schema, true);
      expect(External.containsRawSql(pruned)).to.equal(false);
      expect(pruned.equals($('qaSolar14').overlap(['x']).simplify())).to.equal(true);
    });
    it('a negated raw-SQL predicate becomes identity TRUE for a linked schema, never FALSE', () => {
      expect(External.pruneFilterToSchema(NOT_NULL, schema, true).equals(Expression.TRUE)).to.equal(
        true,
      );
    });
    it('an OR that mixes a raw-SQL predicate with a linked column is unevaluable as a whole', () => {
      const f = AMAZON_ONLY.or($('qaSolar14').overlap(['x']));
      expect(External.pruneFilterToSchema(f, schema, true).equals(Expression.TRUE)).to.equal(true);
    });
    it('containsRawSql sees a sqlRef nested anywhere', () => {
      expect(External.containsRawSql(timeFilter.and(NOT_NULL))).to.equal(true);
      expect(External.containsRawSql(timeFilter)).to.equal(false);
    });
  });

  describe('SIMULATE — the front stamps the raw-SQL clause on main AND every linked apply', () => {
    it('totals: plans without throwing; the CASE WHEN reaches main only', () => {
      const sqls = planSql(buildExpr(timeFilter.and(NOT_NULL), true, false));
      expect(sqls.length).to.be.greaterThan(0);
      const onMain = sqls.filter(q => q.includes('histories') && q.includes('CASE WHEN'));
      const onLookup = sqls.filter(q => q.includes('lookup_') && q.includes('CASE WHEN'));
      expect(onMain.length, 'main WHERE keeps the raw-SQL predicate').to.be.greaterThan(0);
      expect(onLookup, 'no lookup query carries the raw-SQL predicate').to.deep.equal([]);
      expect(
        sqls.some(q => q.includes('lookup_')),
        'no lookup is queried at all for a main-only filter',
      ).to.equal(false);
    });
    it('split by the raw-SQL dimension with a raw-SQL filter: plans, CASE WHEN on main only', () => {
      const sqls = planSql(buildExpr(timeFilter.and(NOT_NULL), true, true));
      expect(sqls.some(q => q.includes('CASE WHEN'))).to.equal(true);
      expect(sqls.filter(q => q.includes('lookup_') && q.includes('CASE WHEN'))).to.deep.equal([]);
    });
    it('a raw-SQL IN filter (Amazon only) is kept on main', () => {
      const sqls = planSql(buildExpr(timeFilter.and(AMAZON_ONLY), true, false));
      expect(sqls.some(q => q.includes('histories') && q.includes("'Amazon'"))).to.equal(true);
      expect(sqls.filter(q => q.includes('lookup_') && q.includes("'Amazon'"))).to.deep.equal([]);
    });
  });

  describe('SIMULATE — the raw-SQL clause only on main (a client that already prunes its linked copies)', () => {
    it('still plans: the harvest of $main.filter() no longer treats the clause as linked-only', () => {
      const sqls = planSql(buildExpr(timeFilter.and(NOT_NULL), false, false));
      expect(sqls.some(q => q.includes('CASE WHEN'))).to.equal(true);
      expect(sqls.filter(q => q.includes('lookup_'))).to.deep.equal([]);
    });
  });

  describe('ORTHOGONALITY — a genuine linked-only clause still reaches its lookup', () => {
    it('brand_country = Francia on the lookup side, raw-SQL clause on main', () => {
      const f = timeFilter.and($('brand_country').overlap(['Francia'])).and(NOT_NULL);
      const sqls = planSql(
        ply()
          .apply('main', $('main').filter(f))
          .apply('magic_b', $('magic_b').filter(f))
          .apply('pairs', '$main.countDistinct($productId.concat($competitor))'),
      );
      const joined = sqls.join('\n');
      expect(joined).to.match(/brand_country/);
      expect(joined).to.match(/'Francia'/);
      expect(joined).to.match(/CASE WHEN/);
      // Same-engine lookup: the totals semijoin is ONE statement whose WHERE
      // carries `"brand" IN (SELECT … FROM "lookup_b_rev1" …)`. The raw-SQL
      // clause must stay on the OUTER main WHERE, never inside that sub-query.
      const totals = sqls.find(q => q.includes('lookup_b_rev1'));
      expect(totals, 'one statement carries the lookup sub-query').to.exist;
      const subStart = totals.indexOf('IN (SELECT');
      expect(subStart, 'IN sub-query present').to.be.greaterThan(-1);
      const sub = totals.slice(subStart, totals.indexOf('GROUP BY 1)', subStart));
      expect(sub).to.match(/brand_country/);
      expect(sub).to.not.match(/CASE WHEN/);
    });
  });
});
