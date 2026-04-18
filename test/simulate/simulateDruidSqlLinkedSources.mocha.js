const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, ply, r } = plywood;

const timeFilter = $('time').overlap({
  start: new Date('2026-04-01T00:00:00Z'),
  end: new Date('2026-04-02T00:00:00Z'),
});

const makeMainExternal = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'ean', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    filter: timeFilter,
  });

const makeReviewsExternal = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'main_ds-reviews',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'ean', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'reviewsRating', type: 'NUMBER', unsplitable: true },
    ],
    filter: timeFilter,
  });

describe('simulate Druid SQL with linked sources (multi-external join)', () => {
  it('produces two separate queries when joining split results from different externals', () => {
    const mainSplit = $('main')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)')
      .sort('$AvgPrice', 'descending')
      .limit(10);

    const reviewsSplit = $('reviews')
      .split('$competitor', 'Competitor')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const ex = ply()
      .apply('main', $('main').filter(timeFilter))
      .apply('reviews', $('reviews').filter(timeFilter))
      .apply('data', mainSplit.join(reviewsSplit));

    const queryPlan = ex.simulateQueryPlan({
      main: makeMainExternal(),
      reviews: makeReviewsExternal(),
    });

    // Cycle 1: main query
    expect(queryPlan[0].length).to.equal(1);
    expect(queryPlan[0][0].query).to.include('"main_ds"');
    expect(queryPlan[0][0].query).to.include('AVG("price")');
    expect(queryPlan[0][0].query).to.include('"competitor"');

    // Cycle 2: reviews query
    expect(queryPlan[1].length).to.equal(1);
    expect(queryPlan[1][0].query).to.include('"main_ds-reviews"');
    expect(queryPlan[1][0].query).to.include('AVG("reviewsRating")');
    expect(queryPlan[1][0].query).to.include('"competitor"');
  });

  it('applies time filters to both externals', () => {
    const mainSplit = $('main').split('$ean', 'EAN').apply('AvgPrice', '$main.average($price)');

    const reviewsSplit = $('reviews')
      .split('$ean', 'EAN')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const ex = ply()
      .apply('main', $('main').filter(timeFilter))
      .apply('reviews', $('reviews').filter(timeFilter))
      .apply('data', mainSplit.join(reviewsSplit));

    const queryPlan = ex.simulateQueryPlan({
      main: makeMainExternal(),
      reviews: makeReviewsExternal(),
    });

    // Both queries must have time filter
    expect(queryPlan[0][0].query).to.include("'2026-04-01");
    expect(queryPlan[1][0].query).to.include("'2026-04-01");
  });

  it('joins on multiple shared dimensions', () => {
    const mainSplit = $('main')
      .split({ Competitor: '$competitor', EAN: '$ean' })
      .apply('AvgPrice', '$main.average($price)');

    const reviewsSplit = $('reviews')
      .split({ Competitor: '$competitor', EAN: '$ean' })
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const ex = ply()
      .apply('main', $('main').filter(timeFilter))
      .apply('reviews', $('reviews').filter(timeFilter))
      .apply('data', mainSplit.join(reviewsSplit));

    const queryPlan = ex.simulateQueryPlan({
      main: makeMainExternal(),
      reviews: makeReviewsExternal(),
    });

    // Both queries should GROUP BY both dimensions
    expect(queryPlan[0][0].query).to.include('"competitor"');
    expect(queryPlan[0][0].query).to.include('"ean"');
    expect(queryPlan[1][0].query).to.include('"competitor"');
    expect(queryPlan[1][0].query).to.include('"ean"');
  });

  // -----------------------------------------------------------
  // Heterogeneous externals in the same ply() — the core invariant:
  // each external owns its own schema (attributes + derivedAttributes),
  // and fusing them into a single total must NOT merge schemas.
  // -----------------------------------------------------------
  const makeMainWithDerived = () =>
    External.fromJS({
      engine: 'druidsql',
      source: 'main_ds',
      timeAttribute: 'time',
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'product_id', type: 'STRING' },
        { name: 'price', type: 'NUMBER', unsplitable: true },
      ],
      // derivedAttribute referencing a main column
      derivedAttributes: { productId: '$product_id' },
      filter: timeFilter,
    });

  const makeReviewsWithDerived = () =>
    External.fromJS({
      engine: 'druidsql',
      source: 'main_ds-reviews',
      timeAttribute: 'time',
      attributes: [
        { name: 'time', type: 'TIME' },
        { name: 'partition_id', type: 'STRING' },
        { name: 'product_name', type: 'STRING' },
        { name: 'rating', type: 'NUMBER', unsplitable: true },
      ],
      // derivedAttribute referencing a REVIEWS-only column
      // Before the fix this would crash uniteValueExternalsIntoTotal because it
      // tried to resolve $product_name against main's rawAttributes.
      derivedAttributes: {
        partitionId: '$partition_id',
        productName: '$product_name',
      },
      filter: timeFilter,
    });

  it('heterogeneous externals in one ply() total do not cross-fuse schemas', () => {
    const ex = ply()
      .apply('main', $('main').filter(timeFilter))
      .apply('reviews', $('reviews').filter(timeFilter))
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($rating)');

    // Simulating must not throw "could not resolve $product_name" —
    // each external resolves its own derivedAttributes against its own schema.
    const plan = ex.simulateQueryPlan({
      main: makeMainWithDerived(),
      reviews: makeReviewsWithDerived(),
    });

    // Both source queries present, each against its own datasource
    const allQueries = plan.flat().map(q => q.query || '').join('\n');
    expect(allQueries).to.include('"main_ds"');
    expect(allQueries).to.include('"main_ds-reviews"');
    // Each external keeps its OWN aggregations
    expect(allQueries).to.include('AVG("price")');
    expect(allQueries).to.include('AVG("rating")');
  });

  it('External construction rejects derivedAttributes that do not resolve in its own schema', () => {
    // No try/catch — this must throw early, loud and clear.
    expect(() =>
      External.fromJS({
        engine: 'druidsql',
        source: 'reviews_ds',
        timeAttribute: 'time',
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'rating', type: 'NUMBER' },
        ],
        // $nonexistent is not in this external's attributes
        derivedAttributes: { bogus: '$nonexistent' },
      }),
    ).to.throw(/could not resolve/);
  });
});
