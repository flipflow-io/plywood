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
    const mainSplit = $('main')
      .split('$ean', 'EAN')
      .apply('AvgPrice', '$main.average($price)');

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
});
