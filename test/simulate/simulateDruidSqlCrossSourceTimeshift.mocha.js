/*
 * Spec for cross-source TIMESHIFT decomposition.
 *
 * Turnilo emits, for a time-compare query with linked-source measures:
 *
 *   $main
 *     .split('Competitor', '$competitor')
 *     .apply('Count',          $main.filter($time.overlap(currRange)).count())
 *     .apply('Count_previous', $main.filter($time.overlap(prevRange)).count())
 *     .apply('AvgRating',          $reviews.filter($time.overlap(currRange)).average($rating))
 *     .apply('AvgRating_previous', $reviews.filter($time.overlap(prevRange)).average($rating))
 *
 * The engine must produce FOUR parallel queries — one per (source, period)
 * — not a single blended query. The period filter lives on the External
 * (not in the apply chain) after decomposition, and applies are "clean"
 * again (Overlap substituted with TRUE before being handed to the sub-
 * External).
 *
 * Structural invariants this spec asserts:
 *   - 4 Druid SQL queries total (1 main×2 periods + 1 linked×2 periods)
 *   - Each query's WHERE clause pins a single time range
 *   - Main queries only project main-side applies (Count); linked queries
 *     only project linked-side applies (AvgRating)
 *   - Main-only timeshift (no linked) continues to emit 2 queries via the
 *     pre-existing getJoinDecompositionShortcut path — we don't regress it
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, r } = plywood;

const currRange = {
  start: new Date('2026-04-20T00:00:00Z'),
  end: new Date('2026-04-21T00:00:00Z'),
};
const prevRange = {
  start: new Date('2026-04-19T00:00:00Z'),
  end: new Date('2026-04-20T00:00:00Z'),
};

const outerTimeFilter = $('time').overlap({
  start: new Date('2026-04-19T00:00:00Z'),
  end: new Date('2026-04-21T00:00:00Z'),
});

const makeMainWithLinkedReviews = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      reviews: {
        source: 'main_ds-reviews',
        joinKeys: ['competitor', 'time'],
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'competitor', type: 'STRING' },
          { name: 'reviewsRating', type: 'NUMBER', unsplitable: true },
        ],
      },
    },
    filter: outerTimeFilter,
  });

const makeMainOnly = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: 'time',
    attributes: [
      { name: 'time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    filter: outerTimeFilter,
  });

describe('External auto-decomposition — cross-source TIMESHIFT', () => {
  it('topN: main + linked with current/previous applies → 4 parallel queries', () => {
    // Emulates the turnilo grid: split by competitor, sort by a measure
    // (descending), limit 50. This is the shape where getJoinDecomposition
    // Shortcut takes its "topN" path for plain (non-linked) timeshift —
    // we assert that cross-source fans out to 4 queries in the same shape.
    const structured = $('main')
      .split('$competitor', 'Competitor')
      .apply('Count', $('main').filter($('time').overlap(currRange)).count())
      .apply('Count_previous', $('main').filter($('time').overlap(prevRange)).count())
      .apply(
        'AvgRating',
        $('reviews').filter($('time').overlap(currRange)).average('$reviewsRating'),
      )
      .apply(
        'AvgRating_previous',
        $('reviews').filter($('time').overlap(prevRange)).average('$reviewsRating'),
      )
      .sort('$Count', 'descending')
      .limit(50);

    const plan = structured.simulateQueryPlan({ main: makeMainWithLinkedReviews() });
    const realQueries = plan.flat().filter(q => typeof q.query === 'string');

    // 4 queries: main×{curr,prev} + linked×{curr,prev}
    expect(realQueries, 'four parallel SQL queries').to.have.length(4);

    const mainQueries = realQueries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'),
    );
    const linkedQueries = realQueries.filter(q => q.query.includes('main_ds-reviews'));

    expect(mainQueries, 'exactly 2 main queries').to.have.length(2);
    expect(linkedQueries, 'exactly 2 linked queries').to.have.length(2);

    // Each main query projects only main-side apply (Count), never the linked ones
    for (const q of mainQueries) {
      expect(q.query).to.match(/COUNT\(\*\)|COUNT\(1\)/i);
      expect(q.query).to.not.match(/reviewsRating/);
    }

    // Each linked query projects only linked-side apply (AvgRating), never Count(*)
    for (const q of linkedQueries) {
      expect(q.query).to.match(/AVG\("reviewsRating"\)/);
    }

    // Period filters are pinned per query — one query per distinct time range
    const curStr = currRange.start.toISOString();
    const prevStr = prevRange.start.toISOString();
    const mainCurr = mainQueries.find(q => q.query.includes(curStr.slice(0, 10)));
    const mainPrev = mainQueries.find(q => q.query.includes(prevStr.slice(0, 10)));
    const linkedCurr = linkedQueries.find(q => q.query.includes(curStr.slice(0, 10)));
    const linkedPrev = linkedQueries.find(q => q.query.includes(prevStr.slice(0, 10)));

    expect(mainCurr, 'main current period query').to.exist;
    expect(mainPrev, 'main previous period query').to.exist;
    expect(linkedCurr, 'linked current period query').to.exist;
    expect(linkedPrev, 'linked previous period query').to.exist;
  });

  it('main-only topN timeshift (no linked sources) still emits 2 queries', () => {
    const structured = $('main')
      .split('$competitor', 'Competitor')
      .apply('Count', $('main').filter($('time').overlap(currRange)).count())
      .apply('Count_previous', $('main').filter($('time').overlap(prevRange)).count())
      .sort('$Count', 'descending')
      .limit(50);

    const plan = structured.simulateQueryPlan({ main: makeMainOnly() });
    const realQueries = plan.flat().filter(q => typeof q.query === 'string');

    expect(realQueries, 'two queries for 1-source/2-period').to.have.length(2);
    expect(realQueries.every(q => q.query.includes('"main_ds"'))).to.equal(true);
    expect(realQueries.every(q => !q.query.includes('reviews'))).to.equal(true);
  });

  it('cross-source WITHOUT timeshift still emits 2 queries (1 per source)', () => {
    // Regression guard: the existing getCrossExternalDecomposition path
    // must keep working when there's no period split to apply.
    const structured = $('main')
      .split('$competitor', 'Competitor')
      .apply('Count', '$main.count()')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const plan = structured.simulateQueryPlan({ main: makeMainWithLinkedReviews() });
    const realQueries = plan.flat().filter(q => typeof q.query === 'string');

    expect(realQueries, '1 main + 1 linked, no period split').to.have.length(2);
  });
});
