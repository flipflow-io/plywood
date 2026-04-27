/*
 * Cross-source with MULTIPLE linkedSources — split aliases belonging to
 * different linked sources must each route to their own iteration.
 *
 * Regression: before this patch, classifySplitAliases was called per-
 * linkedSource with the WHOLE split. If the query split on [aliasA,
 * aliasB] where A lived in linkedSource La and B in linkedSource Lb,
 * the iteration for La would throw "neither main nor La" on aliasB,
 * taking down the query — even though aliasB was perfectly valid on
 * Lb's iteration. User symptom: "Query error: Request failed with
 * status code 500" as soon as a cube accumulated two magic-attr
 * linkedSources and a grid tried to split by both.
 *
 * The fix: the classifier reports "foreignLinked" for aliases whose
 * refs resolve in neither main nor THIS linkedSource — the outer loop
 * treats them as sibling-owned, not as an error. A post-loop guard
 * still catches aliases that are truly dangling (not in main, not in
 * ANY declared linkedSource).
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $ } = plywood;

const timeFilter = $('time').overlap({
  start: new Date('2026-04-01T00:00:00Z'),
  end: new Date('2026-04-02T00:00:00Z'),
});

/** Main with TWO magic-attr-style linkedSources — brand_tier owns one,
 *  competitor_size owns the other. Both carry the matching join key,
 *  both declare timeAlignment=eternal (mirrors real magic-attr MSQ
 *  lookups). */
const makeMainWithTwoMagicLookups = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'brand', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      magic_brand_tier: {
        source: 'lookup_brand_tier',
        joinKeys: ['brand'],
        autoInjectJoinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: 'brand', type: 'STRING' },
          { name: 'brand_tier', type: 'STRING' },
          { name: 'confidence', type: 'NUMBER' },
        ],
      },
      magic_competitor_size: {
        source: 'lookup_competitor_size',
        joinKeys: ['competitor'],
        autoInjectJoinKeys: ['competitor'],
        sharedDimensions: ['competitor'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: 'competitor', type: 'STRING' },
          { name: 'competitor_size', type: 'STRING' },
          { name: 'confidence', type: 'NUMBER' },
        ],
      },
    },
    filter: timeFilter,
  });

describe('External cross-source — multiple linkedSources on one query', () => {
  it('split on two aliases owned by different linkedSources — each routes to its own iteration', () => {
    // Split on brand_tier (lives in magic_brand_tier) AND
    // competitor_size (lives in magic_competitor_size), plus a main
    // measure. The classifier for magic_brand_tier must NOT throw
    // when it sees competitor_size; instead it should report it as
    // foreignLinked and let the sibling iteration handle it.
    const ex = $('main')
      .split({ brand_tier: '$brand_tier', competitor_size: '$competitor_size' })
      .apply('AvgPrice', '$main.average($price)');

    expect(() => ex.simulateQueryPlan({ main: makeMainWithTwoMagicLookups() })).to.not.throw();

    const plan = ex.simulateQueryPlan({ main: makeMainWithTwoMagicLookups() });
    const queries = plan.flat().filter(q => typeof q.query === 'string');
    // Expect three Druid queries: main + two linked lookups.
    const mainQ = queries.find(
      q => q.query.includes('"histories"') && !q.query.includes('lookup_'),
    );
    const brandLookupQ = queries.find(q => q.query.includes('lookup_brand_tier'));
    const competitorLookupQ = queries.find(q => q.query.includes('lookup_competitor_size'));
    expect(mainQ, 'main query emitted').to.exist;
    expect(brandLookupQ, 'brand_tier lookup query emitted').to.exist;
    expect(competitorLookupQ, 'competitor_size lookup query emitted').to.exist;
  });

  it('truly dangling alias — not in main, not in any linkedSource — still errors with a clear message', () => {
    const ex = $('main')
      .split({ ghost: '$does_not_exist_anywhere' })
      .apply('AvgPrice', '$main.average($price)');
    expect(() => ex.simulateQueryPlan({ main: makeMainWithTwoMagicLookups() })).to.throw();
  });

  it('three linkedSources, query splits on two — third iteration is untouched', () => {
    // Add a third magic lookup that the query never references. Its
    // iteration must not trip on aliases owned by the other two.
    const mainWithThree = () => {
      const base = makeMainWithTwoMagicLookups().valueOf();
      base.linkedSources = {
        ...base.linkedSources,
        magic_region: {
          source: 'lookup_region',
          joinKeys: ['brand'],
          autoInjectJoinKeys: ['brand'],
          sharedDimensions: ['brand'],
          joinMode: 'inner',
          timeAlignment: 'eternal',
          attributes: [
            { name: 'brand', type: 'STRING' },
            { name: 'brand_region', type: 'STRING' },
            { name: 'confidence', type: 'NUMBER' },
          ],
        },
      };
      return External.fromValue(base);
    };
    const ex = $('main')
      .split({ brand_tier: '$brand_tier', competitor_size: '$competitor_size' })
      .apply('AvgPrice', '$main.average($price)');
    expect(() => ex.simulateQueryPlan({ main: mainWithThree() })).to.not.throw();
  });
});
