/*
 * Regression test for the resplit aggregation path.
 *
 * Reproduces the exact shape of the query that the Turnilo frontend
 * sends for Nestle (cube 425): an outer split by `productName` with
 * four "resplit" measures (avg_price / avg_pvp / min_price /
 * diff_with_pvp). Each measure aggregates over an inner split keyed
 * on a concat of 8 dimensions (time, competitor, marketing tags…).
 *
 * Symptom in production (and against a fresh @flipflow/plywood@0.44.1
 * built from a CLEAN bundle that includes buildResplitAggregationSQL):
 *
 *   POST /plywood
 *   {"error":"Could not compute",
 *    "message":"can not convert split expression to SQL directly"}
 *
 * The stack trace points at SQLExternal.getQueryAndPostTransform —
 * specifically the `case 'total'` branch that iterates over applies
 * and calls `apply.getSQL(dialect)`. Walking the chain reaches the
 * inner SplitExpression and throws.
 *
 * Note: nestedGroupByIfNeeded() has *already* detected the resplit
 * by the time we reach the switch, and `selectExpressions` /
 * `groupByExpressions` / `fromClause` already point at a CTE that
 * pre-aggregates the inner split. The bug is that both 'total' and
 * 'split' branches *ignore* nestedGroupByResult and overwrite/append
 * via apply.getSQL().
 */

const { expect } = require('chai');
const plywood = require('../plywood');

const { External, Expression, $, ply } = plywood;

const ATTRIBUTES = [
  { name: 'time', type: 'TIME' },
  { name: 'competitor', type: 'STRING' },
  { name: 'productName', type: 'STRING' },
  { name: 'mktLineItem', type: 'STRING' },
  { name: 'adType', type: 'STRING' },
  { name: 'mktCampaignName', type: 'STRING' },
  { name: 'mktProductName', type: 'STRING' },
  { name: 'adGroupName', type: 'STRING' },
  { name: 'mktSearchTerm', type: 'STRING' },
  { name: 'marketId', type: 'STRING' },
  { name: 'price', type: 'NUMBER' },
  { name: 'pvp', type: 'NUMBER' },
];

const baseExternal = External.fromJS({
  engine: 'druidsql',
  version: '30.0.0',
  source: 'reviews',
  attributes: ATTRIBUTES,
});

const context = { wiki: baseExternal };

// Concat keyed on (day, competitor, mktLineItem, adType, …) is what
// the Nestle measures use. Plywood's type-check rejects fallback('0')
// when the ref hasn't been resolved yet, so we cast and use a strict
// string literal. Functionally the same resplit pattern.
function innerConcat() {
  return $('time')
    .timeBucket('P1D', 'Etc/UTC')
    .cast('STRING')
    .concat($('competitor'))
    .concat($('mktLineItem'))
    .concat($('adType'))
    .concat($('mktCampaignName'))
    .concat($('mktProductName'))
    .concat($('adGroupName'))
    .concat($('mktSearchTerm'))
    .concat($('marketId'));
}

// avg_price = $wiki.split(<concat>).apply('B', $wiki.filter($price>0).average($price)).average($B)
function resplitAvg(metric) {
  return $('wiki')
    .split(innerConcat(), 'split')
    .apply('B', $('wiki').filter($(metric).greaterThan(0)).average($(metric)))
    .average($('B'));
}
function resplitMin(metric) {
  return $('wiki')
    .split(innerConcat(), 'split')
    .apply('B', $('wiki').filter($(metric).greaterThan(0)).min($(metric)))
    .min($('B'));
}

// The four measures Cube 425 sends, identical shape to production
function nestleMeasures(plyExpr) {
  return plyExpr
    .apply('avg_price', resplitAvg('price'))
    .apply('avg_pvp', resplitAvg('pvp'))
    .apply('min_price', resplitMin('price'))
    .apply(
      'diff_with_pvp',
      $('wiki')
        .average($('price'))
        .subtract($('wiki').average($('pvp')))
        .divide($('wiki').average($('pvp'))),
    );
}

function getQueryAndPostTransformSafely(external) {
  let err = null;
  let result = null;
  try {
    result = external.getQueryAndPostTransform();
  } catch (e) {
    err = e;
  }
  return { result, err };
}

describe('SQLExternal — resplit aggregation in non-split branches', () => {
  it("totals over 4 Nestle resplit measures must NOT throw 'can not convert split expression to SQL directly'", () => {
    // Same totals decomposition Plywood builds for the user's query:
    // ply()
    //   .apply('avg_price', $wiki.split(concat).apply('B', $wiki.filter(...).avg($price)).average($B))
    //   .apply('avg_pvp',   ...)
    //   .apply('min_price', ...)
    //   .apply('diff_with_pvp', ...)
    let ex = ply();
    ex = nestleMeasures(ex);
    ex = ex.referenceCheck(context).resolve(context).simplify();
    expect(ex.op).to.equal('literal');
    const external = ex.value.getReadyExternals()[0].external;
    expect(external.mode).to.equal('total');

    const { result, err } = getQueryAndPostTransformSafely(external);
    if (err) {
      expect(err.message).to.not.match(
        /can not convert split expression to SQL directly/,
        'Bug: total-mode branch ignores nestedGroupByResult and calls ' +
          'apply.getSQL() on a resplit apply. Stack should show ' +
          'ChainableUnaryExpression.getSQL → SplitExpression.getSQL.',
      );
    }
    expect(result).to.be.an('object');
    const sql = typeof result.query === 'string' ? result.query : result.query && result.query.query;
    expect(sql).to.be.a('string');
    expect(sql).to.include('cte_subsplit'); // the CTE produced by the resplit builder
  });

  it("'split' mode external with resplit applies must NOT throw", () => {
    // Build a totals external (the resolved tree's external) and then
    // attach an outer split + the same resplit applies, mimicking the
    // sub-query Plywood produces for the per-productName grid.
    let ex = ply();
    ex = nestleMeasures(ex);
    ex = ex.referenceCheck(context).resolve(context).simplify();
    const totalsExternal = ex.value.getReadyExternals()[0].external;
    const splitValue = totalsExternal.valueOf();
    splitValue.mode = 'split';
    splitValue.split = $('main').split($('productName'), 'productName');
    const splitExternal = External.fromValue(splitValue);
    expect(splitExternal.mode).to.equal('split');

    const { err } = getQueryAndPostTransformSafely(splitExternal);
    if (err) {
      expect(err.message).to.not.match(
        /can not convert split expression to SQL directly/,
        'Bug: split-mode branch also ignores nestedGroupByResult and ' +
          'overwrites selectExpressions with split.getSelectSQL().',
      );
    }
  });
});
