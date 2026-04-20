/*
 * RED spec for linked-source auto-decomposition in External.
 *
 * Today the caller (turnilo) manually splits an expression into main + linked
 * sub-queries and wraps them in a ply().apply('main', ...).apply('reviews', ...).
 * The target is: the caller writes a SINGLE unified expression rooted at $main
 * with $reviews refs, and plywood's External reshapes the tree into two
 * sub-queries + join internally.
 *
 * This file pins the target behavior. It will go RED until phases 2 and 3 land.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

const timeFilter = $('time').overlap({
  start: new Date('2026-04-01T00:00:00Z'),
  end: new Date('2026-04-02T00:00:00Z'),
});

/**
 * Main external that declares 'reviews' as a linked source. The linked source
 * config carries enough info for plywood to synthesize a sub-external on its
 * own — no need for the caller to register 'reviews' separately in context.
 */
const makeMainWithLinkedReviews = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'isOwn', type: 'BOOLEAN' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      reviews: {
        source: 'main_ds-reviews',
        joinKeys: ['competitor'],
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'competitor', type: 'STRING' },
          { name: 'reviewContent', type: 'STRING' },
          { name: 'reviewsRating', type: 'NUMBER', unsplitable: true },
        ],
      },
    },
    filter: timeFilter,
  });

describe('External auto-decomposition — cross-source expressions', () => {
  // ----------------------------------------------------------------------
  // Happy path — unified expression decomposes into two queries + join
  // ----------------------------------------------------------------------
  it('split on main dim + main series + linked series → two queries on shared key', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });

    const all = plan
      .flat()
      .map(q => q.query || '')
      .join('\n');
    expect(all).to.include('"main_ds"');
    expect(all).to.include('"main_ds-reviews"');
    expect(all).to.include('AVG("price")');
    expect(all).to.include('AVG("reviewsRating")');
    // Both queries group by the shared join key
    expect(all.match(/"competitor"/g).length).to.be.at.least(2);
  });

  it('single-source query (no linked refs) stays a single query — no decomposition overhead', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });

    const realQueries = plan.flat().filter(q => typeof q.query === 'string');
    expect(realQueries).to.have.length(1);
    expect(realQueries[0].query).to.include('"main_ds"');
    expect(realQueries[0].query).to.not.include('main_ds-reviews');
  });

  // ----------------------------------------------------------------------
  // Failure 1 — sort by linked dimension, splits are main-only.
  // Engine must promote an implicit first($dim) on the linked side and
  // apply the sort after the join. The sub-query against the linked source
  // must include the linked dim in its projection.
  // ----------------------------------------------------------------------
  it('sort by a linked-source dimension with main-only splits promotes an implicit first() on the linked side', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('MinPrice', '$main.min($price)')
      .sort('$reviews.reviewContent', 'ascending')
      .limit(50);

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });

    const all = plan
      .flat()
      .map(q => q.query || '')
      .join('\n');
    // The linked query exists
    expect(all).to.include('"main_ds-reviews"');
    // It projects reviewContent (either via LATEST/FIRST or as a group-by key)
    expect(all).to.match(/reviewContent|review_content/i);
    // Main query still exists against main_ds
    expect(all).to.include('"main_ds"');
  });

  // ----------------------------------------------------------------------
  // Failure 2 — degenerate shape. User splits by linked-only dim with
  // a main-only series but NO shared key in the splits. Ambiguous: do we
  // broadcast min_price as a global total? silently inject competitor as a
  // hidden join column? Neither is obvious — reject with a clear reason,
  // do NOT crash downstream with "planner bug".
  // ----------------------------------------------------------------------
  it('linked-only split with main-only series and no shared split key → clear error, no crash', () => {
    const ex = $('main')
      .split('$reviews.reviewContent', 'Review')
      .apply('MinPrice', '$main.min($price)');

    expect(() => ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() })).to.throw(
      /linked|shared|cross-source/i,
    );
  });

  it('linked-only split with both main and linked series, no shared key → clear error', () => {
    const ex = $('main')
      .split('$reviews.reviewContent', 'Review')
      .apply('MinPrice', '$main.min($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    expect(() => ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() })).to.throw(
      /linked|shared|cross-source/i,
    );
  });

  // ----------------------------------------------------------------------
  // Filter partitioning — a filter on a linked-only dim applied inside a
  // cross-source expression must end up on the linked sub-query's WHERE,
  // and must not leak to the main query (which cannot resolve it).
  // ----------------------------------------------------------------------
  it('filter on a linked-only dimension routes to the linked sub-query, not main', () => {
    const ex = $('main')
      .filter('$reviewContent.is("great")')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });

    const mainQuery = plan
      .flat()
      .find(
        q => (q.query || '').includes('"main_ds"') && !(q.query || '').includes('main_ds-reviews'),
      );
    const linkedQuery = plan.flat().find(q => (q.query || '').includes('"main_ds-reviews"'));

    expect(linkedQuery, 'linked query must exist').to.exist;
    expect(linkedQuery.query).to.match(/reviewContent|review_content/);

    expect(mainQuery, 'main query must exist').to.exist;
    // Main query must NOT reference reviewContent — it does not own that column
    expect(mainQuery.query).to.not.match(/reviewContent|review_content/);
  });

  // ----------------------------------------------------------------------
  // Symmetry — a main dim whose name happens to match a linked attribute
  // does not accidentally resolve on the wrong side.
  // ----------------------------------------------------------------------
  it('shared attribute names (time, competitor) resolve on the owning side only', () => {
    // isOwn is main-only. reviewContent is linked-only.
    const ex = $('main')
      .split({ Competitor: '$competitor', IsOwn: '$isOwn' })
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });

    const all = plan
      .flat()
      .map(q => q.query || '')
      .join('\n');
    expect(all).to.include('"main_ds"');
    expect(all).to.include('"main_ds-reviews"');

    const linkedQuery = plan.flat().find(q => (q.query || '').includes('main_ds-reviews'));
    // Linked side must not try to group by isOwn (main-only column)
    expect(linkedQuery.query).to.not.match(/isOwn|is_own/i);
  });
});
