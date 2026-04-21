/*
 * Cross-source matrix — the canonical truth table.
 *
 * One spec per row of the 12-shape matrix:
 *
 *   # | MainSplit | SharedSplit | LinkedSplit | MainAgg | LinkedAgg | TimeShift | Expected outcome
 *   ---------------------------------------------------------------------------------------------
 *   1 |    -      |     -       |     -       |    Y    |    -      |     -     | totals main, 1 q
 *   2 |    Y      |     -       |     -       |    Y    |    -      |     -     | topN main, 1 q
 *   3 |    Y      |     -       |     -       |    Y    |    -      |     Y     | main x2 periods
 *   4 |    Y      |     -       |     -       |    Y    |    Y      |     -     | ERROR: needs shared
 *   5 |    -      |     Y       |     -       |    Y    |    Y      |     -     | 2 q, fullJoin
 *   6 |    -      |     Y       |     -       |    Y    |    Y      |     Y     | 3–4 q, timeshift
 *   7 |    Y      |     Y       |     -       |    Y    |    Y      |     -     | 2 q, main carries both splits
 *   8 |    -      |     -       |     Y       |    Y    |    Y      |     -     | main totals broadcast + linked split
 *   9 |    Y      |     -       |     Y       |    Y    |    -      |     -     | (validator layer; not Plywood's job)
 *  10 |    Y      |     Y       |     Y       |    Y    |    Y      |     -     | full combo
 *  11 |    Y      |     -       |     -       |    Y    |    -      |     -     | linked DECLARED but unused → 1 q, no decomp
 *  12 |    -      |     -       |     -       |    Y    |    Y      |     -     | totals cross-source, 2 q
 *
 * Each spec asserts: query count, table source of each query, applies projected
 * per query (sources don't bleed), sort/limit routing, and period filter pinning
 * where relevant. Shape 4 asserts the Plywood error; shape 9 lives in turnilo's
 * validator layer (tested separately).
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

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

// Main cube with reviews linkedSource declared. `competitor` and `__time` are
// joinKeys (shared). `ean`, `brand` are main-only. `review_title`, `review_content`
// are linked-only (exist in reviews.attributes, NOT in main.attributes).
function makeMainWithLinkedReviews() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'ean', type: 'STRING' },
      { name: 'brand', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      reviews: {
        source: 'main_ds-reviews',
        joinKeys: ['competitor', '__time'],
        // Only `competitor` is auto-injected as a synthetic split when
        // the user picks no shared dimension. __time is a valid join
        // anchor only when the user explicitly splits on timeBucket;
        // auto-adding it would explode results by snapshot count.
        autoInjectJoinKeys: ['competitor'],
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'competitor', type: 'STRING' },
          { name: 'review_title', type: 'STRING' },
          { name: 'review_content', type: 'STRING' },
          { name: 'reviewsRating', type: 'NUMBER', unsplitable: true },
        ],
      },
    },
    filter: outerTimeFilter,
  });
}

// Cube with NO linkedSources — for baseline main-only regressions (shapes 1-3).
function makeMainOnly() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'competitor', type: 'STRING' },
      { name: 'ean', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    filter: outerTimeFilter,
  });
}

function runPlan(expr, external) {
  const plan = expr.simulateQueryPlan({ main: external });
  return plan.flat().filter(q => typeof q.query === 'string');
}

function mainQueriesOf(queries) {
  return queries.filter(q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'));
}
function linkedQueriesOf(queries) {
  return queries.filter(q => q.query.includes('main_ds-reviews'));
}

describe('Cross-source matrix — canonical shapes', () => {
  it('Shape 1: totals, main-only aggregate, no linked cube → 1 query', () => {
    const ex = ply().apply('Count', '$main.count()');
    const queries = runPlan(ex, makeMainOnly());

    expect(queries, '1 query').to.have.length(1);
    expect(queries[0].query).to.include('"main_ds"');
    expect(queries[0].query).to.match(/COUNT\(\*\)/i);
  });

  it('Shape 2: topN main, main-only aggregate, no linked cube → 1 query', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)')
      .sort('$AvgPrice', 'descending')
      .limit(50);
    const queries = runPlan(ex, makeMainOnly());

    expect(queries, '1 query').to.have.length(1);
    expect(queries[0].query).to.include('GROUP BY');
    expect(queries[0].query).to.match(/ORDER BY .*"AvgPrice".*DESC/i);
    expect(queries[0].query).to.match(/LIMIT 50/i);
  });

  it('Shape 3: topN main + timeshift, no linked → 2 queries (curr/prev)', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('Count', $('main').filter($('__time').overlap(currRange)).count())
      .apply('Count_previous', $('main').filter($('__time').overlap(prevRange)).count())
      .sort('$Count', 'descending')
      .limit(50);
    const queries = runPlan(ex, makeMainOnly());

    expect(queries, '2 queries (curr+prev)').to.have.length(2);
    expect(queries.every(q => q.query.includes('"main_ds"'))).to.equal(true);
    expect(queries.every(q => !q.query.includes('reviews'))).to.equal(true);
  });

  it('Shape 4: main split + both aggregates, no user-shared split → auto-inject joinKeys', () => {
    // When a cross-source query has no shared split, the engine auto-injects
    // every non-time declared joinKey as a synthetic split (`__join_<key>`)
    // on both sides so the in-memory join has something to align on. The
    // synthetic columns are projected away from the final dataset, so the
    // caller sees only what they asked for: (Ean, AvgPrice, AvgRating).
    //
    // Time-typed joinKeys are intentionally NOT auto-injected: doing so
    // would force both sides to group by snapshot timestamp, producing
    // one output row per (user-split, snapshot) tuple — a product with N
    // scrapes in the filter window would appear N times in the grid. The
    // user's time filter already constrains both sides to the same
    // window; the identity-key join aggregates across it.
    const ex = $('main')
      .split('$ean', 'Ean')
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');

    const queries = runPlan(ex, makeMainWithLinkedReviews());
    const mains = mainQueriesOf(queries);
    const links = linkedQueriesOf(queries);

    expect(mains, 'main side').to.have.length(1);
    expect(links, 'linked side').to.have.length(1);
    // Identity joinKey injected on both sides; __time is NOT.
    expect(mains[0].query).to.match(/AS "__join_competitor"/);
    expect(mains[0].query).to.not.match(/AS "__join___time"/);
    expect(mains[0].query).to.match(/"ean" AS "Ean"/); // user's split stays
    expect(links[0].query).to.match(/AS "__join_competitor"/);
    expect(links[0].query).to.not.match(/AS "__join___time"/);
    // Sources don't bleed
    expect(mains[0].query).to.not.match(/reviewsRating/);
    expect(links[0].query).to.not.match(/AVG\("price"\)/);
  });

  it('Shape 5: shared split + both aggregates → 2 queries joined on shared key', () => {
    const ex = $('main')
      .split('$competitor', 'Competitor')
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    const mains = mainQueriesOf(queries);
    const links = linkedQueriesOf(queries);
    expect(mains, 'one main query').to.have.length(1);
    expect(links, 'one linked query').to.have.length(1);
    // Both sides group by the shared joinKey
    expect(mains[0].query).to.match(/"competitor" AS "Competitor"/);
    expect(links[0].query).to.match(/"competitor" AS "Competitor"/);
    // Sources don't bleed
    expect(mains[0].query).to.not.match(/reviewsRating/);
    expect(links[0].query).to.not.match(/AVG\("price"\)/);
  });

  it('Shape 6: shared split + both aggregates + timeshift → fan-out per source', () => {
    const ex = $('main')
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
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    const mains = mainQueriesOf(queries);
    const links = linkedQueriesOf(queries);
    // Main has topN → 2 queries per period; linked has no sort → 1 query with CASE WHEN
    expect(mains, 'main fans out per period').to.have.length(2);
    expect(links, 'linked fits in one query').to.have.length(1);
    for (const q of mains) expect(q.query).to.not.match(/reviewsRating/);
    for (const q of links) expect(q.query).to.match(/AVG\(CASE WHEN.*"reviewsRating"/);
  });

  it('Shape 7: main split + shared split + both aggregates → main carries both', () => {
    const ex = $('main')
      .split({ Ean: '$ean', Competitor: '$competitor' })
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    const mains = mainQueriesOf(queries);
    const links = linkedQueriesOf(queries);
    expect(mains).to.have.length(1);
    expect(links).to.have.length(1);
    // Main groups by both ean and competitor
    expect(mains[0].query).to.match(/"ean" AS "Ean"/);
    expect(mains[0].query).to.match(/"competitor" AS "Competitor"/);
    // Linked side groups ONLY by the shared joinKey, not ean (it doesn't have it)
    expect(links[0].query).to.match(/"competitor" AS "Competitor"/);
    expect(links[0].query).to.not.match(/"ean"/);
  });

  it('Shape 8: linked-only split + both aggregates → auto-inject joinKeys on both sides', () => {
    // User's only split is linked-only. The engine auto-injects the declared
    // non-time joinKeys on both sides so the main aggregate is computed per
    // (joinKey-tuple) context and joined to the linked rows. Post-join, the
    // synthetic columns are dropped and the caller sees
    // (ReviewTitle, AvgPrice, AvgRating).
    //
    // Time-typed joinKeys are skipped — see Shape 4 for the rationale.
    const ex = $('main')
      .split('$review_title', 'ReviewTitle')
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    const mains = mainQueriesOf(queries);
    const links = linkedQueriesOf(queries);
    expect(mains).to.have.length(1);
    expect(links).to.have.length(1);
    // Identity joinKey injected on both sides; __time is NOT.
    expect(mains[0].query).to.match(/AS "__join_competitor"/);
    expect(mains[0].query).to.not.match(/AS "__join___time"/);
    // Linked carries the linked-only user split + identity synthetic joinKey
    expect(links[0].query).to.match(/"review_title"/);
    expect(links[0].query).to.match(/"__join_competitor"/);
    expect(links[0].query).to.not.match(/AS "__join___time"/);
  });

  it('Shape 10: main split + shared split + linked split + both aggregates', () => {
    const ex = $('main')
      .split({
        Ean: '$ean',
        Competitor: '$competitor',
        ReviewTitle: '$review_title',
      })
      .apply('AvgPrice', '$main.average($price)')
      .apply('AvgRating', '$reviews.average($reviewsRating)');
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    const mains = mainQueriesOf(queries);
    const links = linkedQueriesOf(queries);
    expect(mains).to.have.length(1);
    expect(links).to.have.length(1);
    // Main has its own + shared, not the linked-only
    expect(mains[0].query).to.match(/"ean"/);
    expect(mains[0].query).to.match(/"competitor"/);
    expect(mains[0].query).to.not.match(/"review_title"/);
    // Linked has shared + linked-only, not the main-only
    expect(links[0].query).to.match(/"competitor"/);
    expect(links[0].query).to.match(/"review_title"/);
    expect(links[0].query).to.not.match(/"ean"/);
  });

  it('Shape 11a: linked DECLARED on cube but not referenced at all → 1 query', () => {
    // Minimal reproducer: linkedSources on the cube, but NO ply-level
    // registration and no linked aggregate either.
    const ex = $('main').split('$ean', 'Ean').apply('AvgPrice', '$main.average($price)');
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    expect(queries, 'exactly 1 query, no fan-out').to.have.length(1);
    expect(queries[0].query).to.include('"main_ds"');
    expect(queries[0].query).to.not.include('main_ds-reviews');
  });

  it('Shape 11b: linked REGISTERED at ply level but no linked aggregate → no linked query', () => {
    // This reproduces the actual user bug. Turnilo's visualization-query.ts
    // injects `.apply(lsName, $lsName.filter(mainFilter))` at the top-level
    // ply — always, regardless of whether the query uses the linked source.
    // That registration creates a foreign ExternalExpression in the top
    // ply's applies after resolve. Without a linked aggregate in the actual
    // output path, decomposition is inappropriate: Plywood must recognize
    // ply-level scope registration is not a value contribution.
    const innerTimeFilter = $('__time').overlap(currRange);
    const ex = ply()
      .apply('main', $('main').filter(innerTimeFilter))
      .apply('reviews', $('reviews').filter(innerTimeFilter))
      .apply(
        'SPLIT',
        $('main')
          .split('$ean', 'Ean')
          .apply('AvgPrice', '$main.average($price)')
          .sort('$AvgPrice', 'descending')
          .limit(50),
      );
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    const links = linkedQueriesOf(queries);
    const mains = mainQueriesOf(queries);
    expect(links, 'no linked query when no linked aggregate in output').to.have.length(0);
    expect(mains, 'at least one main query').to.have.length.at.least(1);
  });

  it('Shape 11c: user flow — linked-only split refs without linked agg → auto-inject + projection', () => {
    // The canonical repro: a grid query where the user picked review-side
    // dimensions (`review_title`, `review_content`) alongside a main
    // dimension (`ean`) and a main aggregate (`avg_price`). No shared
    // split, no linked aggregate.
    //
    // The schema-overlap trigger fires on `review_content` / `review_title`
    // (columns only in reviews.attributes) → decomposition engages → no
    // user-shared split → auto-inject the declared joinKeys [partitionId,
    // __time-analog on this fixture: `competitor`, `__time`] on both sides.
    // Main's SQL groups by (Ean, __join_competitor, __join___time) and
    // aggregates AvgPrice; linked's SQL groups by (review_title,
    // review_content, __join_*). The in-memory join aligns on the synthetic
    // keys, post-join drops them, and the caller sees exactly
    // (review_title, review_content, Ean, avg_price).
    const innerTimeFilter = $('__time').overlap(currRange);
    const ex = ply()
      .apply('main', $('main').filter(innerTimeFilter))
      .apply('reviews', $('reviews').filter(innerTimeFilter))
      .apply('avg_price', '$main.average($price)')
      .apply(
        'SPLIT',
        $('main')
          .split({
            review_title: '$review_title',
            review_content: '$review_content',
            ean: '$ean',
          })
          .apply('avg_price', '$main.average($price)')
          .sort('$avg_price', 'descending')
          .limit(50),
      );

    const queries = runPlan(ex, makeMainWithLinkedReviews());
    const links = linkedQueriesOf(queries);
    const mains = mainQueriesOf(queries);

    expect(mains, 'main side present').to.have.length.at.least(1);
    expect(links, 'linked side present').to.have.length.at.least(1);
    // No main query references the linked-only columns — those are routed
    // exclusively to the linked side.
    for (const q of mains) {
      expect(q.query).to.not.match(/review_content|review_title/);
    }
    // The SPLIT sub-query decomposes with synthetic joinKeys: there's a
    // main query that groups by "Ean" alongside "__join_" keys.
    const mainSplitQuery = mains.find(q => /"ean" AS "ean"/.test(q.query));
    expect(mainSplitQuery, 'main side of the SPLIT decomposition').to.exist;
    expect(mainSplitQuery.query).to.match(/"__join_/);
    // Linked groups by the linked-only user splits + synthetic joinKeys,
    // projects no main aggregate.
    const linkedSplitQuery = links.find(q => /review_content/.test(q.query));
    expect(linkedSplitQuery, 'linked side carries review_content split').to.exist;
    expect(linkedSplitQuery.query).to.match(/"__join_/);
  });

  it('Shape 12: totals cross-source (no splits) → 2 queries, both totals mode', () => {
    const ex = ply()
      .apply('Count', '$main.count()')
      .apply('AvgRating', '$reviews.average($reviewsRating)');
    const queries = runPlan(ex, makeMainWithLinkedReviews());

    const mains = mainQueriesOf(queries);
    const links = linkedQueriesOf(queries);
    expect(mains).to.have.length(1);
    expect(links).to.have.length(1);
    // Both totals-mode: GROUP BY () is Druid SQL for aggregate-over-all,
    // one row out (no grouping column).
    expect(mains[0].query).to.match(/GROUP BY \(\)/);
    expect(links[0].query).to.match(/GROUP BY \(\)/);
  });
});
