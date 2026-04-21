/*
 * Filter prune correctness — autonomous Plywood test that replays the
 * JSON shape turnilo's grid emits and pins the engine's behavior. No
 * server, no Druid, no client bundle required — Expression.fromJS +
 * simulateQueryPlan is enough to validate the whole algebra.
 *
 * Scenario: cross-source grid query with
 *   - filter: $__time.overlap(curr∪prev) AND $productName.overlap("X")
 *             AND $reference.overlap("REF1")       (main-only column)
 *   - timeshift: avg_price + _previous_avg_price + _delta (main)
 *                avg_rating + _previous + _delta (linked with .filter(rating>0))
 *   - SPLIT by review_title, review_content, productName + sort + limit
 *
 * What must hold:
 *   1. Query succeeds (no "could not resolve $reference" from linked refcheck)
 *   2. Main side applies the full filter (incl. $reference)
 *   3. Linked side applies the PRUNED filter ($__time + $productName,
 *      dropping $reference which isn't in its schema)
 *   4. The join on synthetic __join_<key> aliases carries main's extra
 *      narrowing over to linked rows.
 *
 * Current engine state (before the fix): the linked side inherits the
 * raw main filter from the ply-level registration, hits reference-check
 * on $reference, and throws. The test PINS this failure mode — when the
 * fix lands, the `expect(() => ...).to.throw(...)` flips to `expect(...).
 * to.have.length.at.least(N)` in commit B.
 */

const { expect } = require('chai');
const plywood = require('../plywood');
const { Expression, External } = plywood;

// Fixture: cube with reviews linkedSource. Attributes are populated as
// they would be after server-side introspection (the shape the server
// actually holds when a real query arrives; the CLIENT-side copy can
// be missing attributes, but this test operates at server-equivalent
// fidelity, which is where the fix lives).
function makeMain() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'partitionId', type: 'STRING' },
      { name: 'productName', type: 'STRING' },
      { name: 'reference', type: 'STRING' }, // main-only
      { name: 'ean', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      reviews: {
        source: 'main_ds-reviews',
        joinKeys: ['partitionId', '__time'],
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'partition_id', type: 'STRING' },
          { name: 'title', type: 'STRING' },
          { name: 'content', type: 'STRING' },
          { name: 'rating', type: 'NUMBER', unsplitable: true },
          { name: 'product_name', type: 'STRING' },
        ],
        derivedAttributes: { partitionId: '$partition_id', productName: '$product_name' },
      },
    },
  });
}

// Build the exact JSON shape turnilo's grid make-query emits for this
// scenario. Kept as a separate builder so we can reuse and vary it
// across related specs.
function buildGridRequestExpression() {
  const curr = { start: '2026-04-14T00:00:00.000Z', end: '2026-04-21T00:00:00.000Z' };
  const prev = { start: '2026-04-07T00:00:00.000Z', end: '2026-04-14T00:00:00.000Z' };

  const timeFilter = {
    op: 'overlap',
    operand: { op: 'ref', name: '__time' },
    expression: {
      op: 'literal',
      value: { setType: 'TIME_RANGE', elements: [curr, prev] },
      type: 'SET',
    },
  };
  const productNameFilter = {
    op: 'overlap',
    operand: { op: 'ref', name: 'productName' },
    expression: {
      op: 'literal',
      value: { setType: 'STRING', elements: ['AMASA-20'] },
      type: 'SET',
    },
  };
  const referenceFilter = {
    op: 'overlap',
    operand: { op: 'ref', name: 'reference' },
    expression: {
      op: 'literal',
      value: { setType: 'STRING', elements: ['REF1'] },
      type: 'SET',
    },
  };
  const mainFilter = {
    op: 'and',
    operand: { op: 'and', operand: timeFilter, expression: productNameFilter },
    expression: referenceFilter,
  };
  const overlapTime = r => ({
    op: 'overlap',
    operand: { op: 'ref', name: '__time' },
    expression: {
      op: 'literal',
      value: { setType: 'TIME_RANGE', elements: [r] },
      type: 'SET',
    },
  });
  const filter_ = (operand, expression) => ({ op: 'filter', operand, expression });
  const apply_ = (operand, expression, name) => ({ op: 'apply', operand, expression, name });

  const base = { op: 'literal', value: { attributes: [], data: [{}] }, type: 'DATASET' };

  // Top ply: main registration + linked registration (raw filter, unpruned;
  // this is what turnilo currently emits and what the server must handle).
  let e = apply_(base, filter_({ op: 'ref', name: 'main' }, mainFilter), 'main');
  e = apply_(e, filter_({ op: 'ref', name: 'reviews' }, mainFilter), 'reviews');

  // Totals-level measures (grid shows these above the row list)
  e = apply_(
    e,
    {
      op: 'average',
      operand: filter_({ op: 'ref', name: 'main' }, overlapTime(curr)),
      expression: { op: 'ref', name: 'price' },
    },
    'avg_price',
  );
  e = apply_(
    e,
    {
      op: 'average',
      operand: filter_({ op: 'ref', name: 'main' }, overlapTime(prev)),
      expression: { op: 'ref', name: 'price' },
    },
    '_previous__avg_price',
  );
  e = apply_(
    e,
    {
      op: 'subtract',
      operand: { op: 'ref', name: 'avg_price' },
      expression: { op: 'ref', name: '_previous__avg_price' },
    },
    '_delta__avg_price',
  );
  const reviewsRating = r => ({
    op: 'average',
    operand: filter_(filter_({ op: 'ref', name: 'reviews' }, overlapTime(r)), {
      op: 'greaterThan',
      operand: { op: 'ref', name: 'rating' },
      expression: { op: 'literal', value: 0 },
    }),
    expression: { op: 'ref', name: 'rating' },
  });
  e = apply_(e, reviewsRating(curr), 'avg_rating');
  e = apply_(e, reviewsRating(prev), '_previous__avg_rating');
  e = apply_(
    e,
    {
      op: 'subtract',
      operand: { op: 'ref', name: 'avg_rating' },
      expression: { op: 'ref', name: '_previous__avg_rating' },
    },
    '_delta__avg_rating',
  );

  // SPLIT sub-query — per-row data
  let s = {
    op: 'split',
    operand: { op: 'ref', name: 'main' },
    splits: {
      review_title: { op: 'ref', name: 'title' },
      review_content: { op: 'ref', name: 'content' },
      productName: { op: 'ref', name: 'productName' },
    },
    dataName: 'main',
  };
  s = apply_(
    s,
    {
      op: 'average',
      operand: filter_({ op: 'ref', name: 'main' }, overlapTime(curr)),
      expression: { op: 'ref', name: 'price' },
    },
    'avg_price',
  );
  s = apply_(s, reviewsRating(curr), 'avg_rating');
  s = {
    op: 'sort',
    operand: s,
    expression: { op: 'ref', name: 'avg_rating' },
    direction: 'descending',
  };
  s = { op: 'limit', operand: s, value: 50 };
  e = apply_(e, s, 'SPLIT');

  return e;
}

describe('Cross-source filter prune — autonomous plywood-level validation', () => {
  it('RED: unpruned main-only filter ref on linked side throws before fix', () => {
    // The turnilo-emitted shape routes the raw main filter (with $reference
    // — a main-only column) into .apply("reviews", $reviews.filter(F)).
    // Without auto-prune at absorb time, refcheck on the linked external
    // fails because $reference doesn't exist in reviews.
    //
    // This spec PINS the pre-fix behavior; commit B flips the assertion
    // to the correct SQL shape.
    const exprJS = buildGridRequestExpression();
    const ex = Expression.fromJS(exprJS);

    expect(() => ex.simulateQueryPlan({ main: makeMain() })).to.throw(
      /could not resolve \$reference|could not resolve \$__time|not within the segment/i,
    );
  });
});
