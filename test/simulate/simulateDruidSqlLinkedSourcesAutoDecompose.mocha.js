/*
 * Spec for linked-source auto-decomposition in External.
 *
 * The caller writes a single unified expression rooted at $main with
 * $reviews refs; plywood's External reshapes the tree into two sub-queries
 * + join internally. No manual ply().apply('main', …).apply('reviews', …)
 * wrapper needed.
 *
 * Algebraic note: split/sort/filter targets are VALUES. To project a
 * foreign-side attribute to the outer scope you need an aggregation
 * (first, max, avg, etc.), never a bare $reviews.attr — that's a scalar
 * selection from a DATASET, not a valid Plywood expression.
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $ } = plywood;

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
        joinKeys: ['competitor', 'time'],
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
  // Happy path — unified expression decomposes into two queries + join
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

  // Filtered aggregate on the linked side: the apply has its own .filter(…)
  // on top of $reviews — plywood absorbs the filter into the inner external.
  // When main tries to collapse the apply, filterDiff must recognize the
  // extra filter as a per-apply refinement (not a ctx-wide filter mismatch).
  it('linked apply with .filter(rating>0) and main apply without it', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.filter($reviewsRating > 0).average($reviewsRating)');

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });
    const all = plan
      .flat()
      .map(q => q.query || '')
      .join('\n');
    expect(all).to.include('"main_ds-reviews"');
    // Filtered aggregate: Druid SQL maps .filter(...).average(...) to a
    // conditional CASE. Either form is correct for the engine.
    expect(all).to.match(/AVG\(CASE WHEN.*"reviewsRating"|AVG\("reviewsRating"\)/);
  });

  // Cross-source split: a split key whose expression references a linked-only
  // attribute is valid — plywood should run it on the linked side, while any
  // shared split goes on both sides and becomes the join key. The main side
  // broadcasts its rows across the extra linked rows.
  it('split by a linked-only dim plus a shared time bucket → main/linked queries partition split keys', () => {
    // Mirrors what turnilo emits: bare refs to both main-compatible columns
    // (time → shared joinKey) and linked-only columns (reviewContent →
    // only in reviews). Plywood must type-check via flat-exposed linked
    // attrs in main's full-type and route split keys by source at execute.
    const ex = $('main')
      .split({
        review_title: '$reviewContent',
        time: '$time.timeBucket("P1D")',
      })
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    // With cross-source split support: this should decompose to
    //   main_ds       GROUP BY time_bucket           SELECT AVG(price)
    //   main_ds-reviews GROUP BY time_bucket, reviewContent SELECT AVG(reviewsRating)
    //   join on time_bucket (broadcast main per title)
    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });

    const all = plan
      .flat()
      .map(q => q.query || '')
      .join('\n');
    expect(all).to.include('"main_ds"');
    expect(all).to.include('"main_ds-reviews"');
    // Main side groups by time only — must NOT reference reviewContent
    const mainQuery = plan
      .flat()
      .find(
        q => (q.query || '').includes('"main_ds"') && !(q.query || '').includes('main_ds-reviews'),
      );
    expect(mainQuery, 'main query must exist').to.exist;
    expect(mainQuery.query).to.match(/AVG\("price"\)/);
    expect(mainQuery.query).to.not.match(/reviewContent/);
    // Linked side groups by BOTH time and reviewContent
    const linkedQuery = plan.flat().find(q => (q.query || '').includes('main_ds-reviews'));
    expect(linkedQuery, 'linked query must exist').to.exist;
    expect(linkedQuery.query).to.match(/reviewContent/);
    expect(linkedQuery.query).to.match(/AVG\("reviewsRating"\)/);
  });

  // Sort routing: when sort targets a linked apply the main side must NOT
  // emit ORDER BY on that column (it doesn't SELECT it), the sort applies
  // post-join. Same for limit — otherwise a pre-join limit drops the wrong
  // rows before post-join ordering.
  it('sort by a linked apply strips sort/limit from main, applies them post-join', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)')
      .sort('$AvgRating', 'descending')
      .limit(10);

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });
    const mainQuery = plan
      .flat()
      .find(
        q => (q.query || '').includes('"main_ds"') && !(q.query || '').includes('main_ds-reviews'),
      );
    expect(mainQuery, 'main query must exist').to.exist;
    // Main SQL must NOT try to ORDER BY AvgRating (doesn't select it)
    expect(mainQuery.query).to.not.match(/AvgRating/);
  });

  // Failure 1 in the UI: sorting by a linked measure. The algebraically
  // well-formed way to express this is an apply that produces the sort key
  // per group, then a sort on that apply — which is exactly what turnilo
  // must emit for this UI gesture (rather than a bare $reviewContent ref).
  it('sort on a linked aggregate apply decomposes and the sort targets the joined result', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('MinPrice', '$main.min($price)')
      .apply('MaxRating', '$reviews.max($reviewsRating)')
      .sort('$MaxRating', 'descending')
      .limit(50);

    const plan = ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() });

    const all = plan
      .flat()
      .map(q => q.query || '')
      .join('\n');
    expect(all).to.include('"main_ds"');
    expect(all).to.include('"main_ds-reviews"');
    // Linked sub-query must project the rating column via MAX
    expect(all).to.match(/MAX\("reviewsRating"\)/);
  });

  // A main dim whose name happens to match a linked attribute does not
  // accidentally resolve on the wrong side.
  it('shared attribute names (time, competitor) resolve on the owning side only', () => {
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

  // Degenerate shape: user has linked applies but no split alias maps to
  // a declared joinKey — there's nothing to join on. The engine must throw
  // a clear error naming the missing keys, never silently fall through
  // and pretend the foreign ref is a native column.
  it('throws a clear error when a linked apply is present but no split column matches any joinKey', () => {
    const ex = $('main')
      .split('$isOwn', 'IsOwn') // main-only, not a joinKey
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    expect(() => ex.simulateQueryPlan({ main: makeMainWithLinkedReviews() })).to.throw(
      /joinKey|shared/i,
    );
  });
});
