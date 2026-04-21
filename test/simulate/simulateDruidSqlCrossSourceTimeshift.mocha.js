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
 *   - Main queries only project main-side applies (Count); linked queries
 *     only project linked-side applies (AvgRating) — sources don't bleed
 *   - Each (source, period) pair is reachable in exactly one query's output
 *     (a Druid plain-groupBy can carry multiple periods via filtered-agg
 *     CASE WHEN; a topN must split into one query per period). Plywood
 *     should emit the minimal correct shape for each sub-External
 *   - Main-only timeshift (no linked) continues to emit 2 queries via the
 *     pre-existing getJoinDecompositionShortcut path — we don't regress it
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, r } = plywood;

// Non-contiguous on purpose: if prev.end === curr.start, both queries carry
// the shared boundary timestamp, which makes WHERE-clause string matching
// ambiguous. Two clear gap days guarantee distinct date strings per query.
const currRange = {
  start: new Date('2026-04-20T00:00:00Z'),
  end: new Date('2026-04-21T00:00:00Z'),
};
const prevRange = {
  start: new Date('2026-04-13T00:00:00Z'),
  end: new Date('2026-04-14T00:00:00Z'),
};

const outerTimeFilter = $('__time').overlap({
  start: new Date('2026-04-13T00:00:00Z'),
  end: new Date('2026-04-21T00:00:00Z'),
});

const makeMainWithLinkedReviews = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      reviews: {
        source: 'main_ds-reviews',
        joinKeys: ['competitor', '__time'],
        attributes: [
          { name: '__time', type: 'TIME' },
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
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    filter: outerTimeFilter,
  });

describe('External auto-decomposition — cross-source TIMESHIFT', () => {
  it('topN sort on main apply: main splits per period, linked uses filtered-aggs', () => {
    // Emulates the turnilo grid: split by competitor, sort by main measure,
    // limit 50. Main takes the topN path (2 queries, one per period), linked
    // has no sort so stays in 1 query with two filtered aggregates. Sources
    // never bleed applies across their queries.
    const structured = $('main')
      .split('$competitor', 'Competitor')
      .apply('Count', $('main').filter($('__time').overlap(currRange)).count())
      .apply('Count_previous', $('main').filter($('__time').overlap(prevRange)).count())
      .apply(
        'AvgRating',
        $('reviews').filter($('__time').overlap(currRange)).average('$reviewsRating'),
      )
      .apply(
        'AvgRating_previous',
        $('reviews').filter($('__time').overlap(prevRange)).average('$reviewsRating'),
      )
      .sort('$Count', 'descending')
      .limit(50);

    const plan = structured.simulateQueryPlan({ main: makeMainWithLinkedReviews() });
    const realQueries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQueries = realQueries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'),
    );
    const linkedQueries = realQueries.filter(q => q.query.includes('main_ds-reviews'));

    // Main has topN (sort+limit) → 1 query per period
    expect(mainQueries, 'main splits per period for topN').to.have.length(2);
    // Linked has no sort/limit → filtered-agg CASE WHEN in a single query
    expect(linkedQueries, 'linked fits in one query with filtered aggs').to.have.length(1);

    // Source non-bleed: no main query references linked columns, no linked
    // query references main columns.
    for (const q of mainQueries) {
      expect(q.query).to.not.match(/reviewsRating/);
    }
    for (const q of linkedQueries) {
      expect(q.query).to.not.match(/COUNT\(\*\)/i);
      expect(q.query).to.match(/AVG\(CASE WHEN.*"reviewsRating"/);
    }

    // Each main query pins exactly one period range in its WHERE clause
    const curDay = currRange.start.toISOString().slice(0, 10);
    const prevDay = prevRange.start.toISOString().slice(0, 10);
    const mainCurr = mainQueries.find(q => q.query.includes(curDay) && !q.query.includes(prevDay));
    const mainPrev = mainQueries.find(q => q.query.includes(prevDay) && !q.query.includes(curDay));
    expect(mainCurr, 'main-current period query with only curr filter').to.exist;
    expect(mainPrev, 'main-previous period query with only prev filter').to.exist;

    // Linked query carries BOTH periods (one per CASE WHEN)
    expect(linkedQueries[0].query).to.include(curDay);
    expect(linkedQueries[0].query).to.include(prevDay);
  });

  it('sort on a linked apply strips sort/limit from both sides → one query per source', () => {
    // When the sort targets a linked apply, neither side can serve a
    // per-period topN: doing so on the linked side would pre-limit to
    // "top 50 by curr AvgRating" which is an approximation, not the
    // true top 50 by AvgRating after the join. The correct shape is:
    // each source emits one query with both periods as CASE WHEN aggs,
    // and sort+limit are applied AFTER the join. Plywood emits this
    // shape today via postJoinSort / postJoinLimit.
    const structured = $('main')
      .split('$competitor', 'Competitor')
      .apply('Count', $('main').filter($('__time').overlap(currRange)).count())
      .apply('Count_previous', $('main').filter($('__time').overlap(prevRange)).count())
      .apply(
        'AvgRating',
        $('reviews').filter($('__time').overlap(currRange)).average('$reviewsRating'),
      )
      .apply(
        'AvgRating_previous',
        $('reviews').filter($('__time').overlap(prevRange)).average('$reviewsRating'),
      )
      .sort('$AvgRating', 'descending')
      .limit(50);

    const plan = structured.simulateQueryPlan({ main: makeMainWithLinkedReviews() });
    const realQueries = plan.flat().filter(q => typeof q.query === 'string');

    const mainQueries = realQueries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'),
    );
    const linkedQueries = realQueries.filter(q => q.query.includes('main_ds-reviews'));

    expect(mainQueries, 'main: one query with both periods as CASE WHEN').to.have.length(1);
    expect(linkedQueries, 'linked: one query with both periods as CASE WHEN').to.have.length(1);

    // Each sub-query carries BOTH periods (the filtered-agg pattern)
    expect(mainQueries[0].query).to.match(/CASE WHEN.*"__time".*CASE WHEN.*"__time"/s);
    expect(linkedQueries[0].query).to.match(/CASE WHEN.*"__time".*CASE WHEN.*"__time"/s);

    // Neither sub-query pre-limits (sort+limit must run post-join)
    expect(mainQueries[0].query).to.not.match(/LIMIT\s+50/i);
    expect(linkedQueries[0].query).to.not.match(/LIMIT\s+50/i);
  });

  it('main-only topN timeshift (no linked sources) still emits 2 queries', () => {
    const structured = $('main')
      .split('$competitor', 'Competitor')
      .apply('Count', $('main').filter($('__time').overlap(currRange)).count())
      .apply('Count_previous', $('main').filter($('__time').overlap(prevRange)).count())
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
