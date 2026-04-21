/*
 * Bug B: post-join HAVING decomposition for cross-source.
 *
 * Scenario: grid query with SPLIT on main, an apply aggregating from a
 * linked source (avg_rating_linked), and a HAVING filter on that linked
 * apply — e.g. `.filter($avg_rating_linked > 0)` applied to the split
 * result. Without the fix, the havingFilter rides along into the main
 * sub-External's SQL and Druid rejects the query ("column not found").
 *
 * With the fix:
 *   1. Main's emitted SQL contains NO reference to `avg_rating_linked`
 *      in its HAVING clause (the predicate was moved to post-join).
 *   2. Linked's emitted SQL also has no such predicate (it references
 *      the apply NAME, not the underlying column — only the post-join
 *      Dataset knows that name).
 *   3. The decomposition exposes the predicate as `postJoinHavingFilter`
 *      so the execution layer applies it against the joined Dataset.
 *
 * This mirrors Ogievetsky's 0f9cbf3 comment: "the correct thing to do
 * would be to decompose the havingFilter ... and assign them accordingly".
 */

const { expect } = require('chai');
const plywood = require('../plywood');
const { $, Expression, External } = plywood;

function makeMain() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'main_ds',
    timeAttribute: '__time',
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'partitionId', type: 'STRING' },
      { name: 'productName', type: 'STRING' },
      { name: 'ean', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    linkedSources: {
      reviews: {
        source: 'main_ds-reviews',
        joinKeys: ['partitionId'],
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'partition_id', type: 'STRING' },
          { name: 'rating', type: 'NUMBER', unsplitable: true },
          { name: 'product_name', type: 'STRING' },
        ],
        derivedAttributes: { partitionId: '$partition_id', productName: '$product_name' },
      },
    },
  });
}

describe('Cross-source post-join HAVING decomposition', () => {
  it('splits a linked-apply HAVING out of main SQL into a post-join filter', () => {
    // Build: split on productName, apply avg_price (main) + avg_rating
    // (linked), sort desc by avg_price, limit 50, then HAVING avg_rating > 0.
    const main = makeMain();
    const reviews = External.fromJS({
      engine: 'druidsql',
      source: 'main_ds-reviews',
      timeAttribute: '__time',
      attributes: [
        { name: '__time', type: 'TIME' },
        { name: 'partition_id', type: 'STRING' },
        { name: 'rating', type: 'NUMBER', unsplitable: true },
        { name: 'product_name', type: 'STRING' },
      ],
      derivedAttributes: { partitionId: '$partition_id', productName: '$product_name' },
    });

    const ex = $('main')
      .split('$productName', 'productName')
      .apply('avg_price', '$main.average($price)')
      .apply('avg_rating', $('reviews').average('$rating'))
      .filter('$avg_rating > 0')
      .sort('$avg_price', 'descending')
      .limit(50);

    // Use simulateQueryPlan with main in context; linked is auto-expanded.
    const plan = ex.simulateQueryPlan({ main });
    const queries = plan.flat().filter(q => typeof q.query === 'string');
    expect(queries.length, 'at least main + linked query emitted').to.be.at.least(2);

    const mainQueries = queries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'),
    );
    const linkedQueries = queries.filter(q => q.query.includes('main_ds-reviews'));
    expect(mainQueries, 'main side present').to.have.length.at.least(1);
    expect(linkedQueries, 'linked side present').to.have.length.at.least(1);

    // Neither side emits SQL that references avg_rating in HAVING. The
    // post-join filter keeps that predicate out of the Druid payload.
    for (const q of mainQueries) {
      expect(q.query, 'main SQL must not HAVING on linked apply').to.not.match(
        /HAVING[\s\S]*avg_rating/i,
      );
    }
    for (const q of linkedQueries) {
      expect(q.query, 'linked SQL must not HAVING on avg_rating apply name').to.not.match(
        /HAVING[\s\S]*"?avg_rating"?\s*>/i,
      );
    }
  });

  it('keeps main-scope HAVING on main and moves linked-scope to post-join (mixed AND)', () => {
    // `(avg_price > 10) AND (avg_rating > 0)` — split cleanly: main HAVING
    // keeps avg_price clause, post-join applies avg_rating clause.
    const main = makeMain();
    const ex = $('main')
      .split('$productName', 'productName')
      .apply('avg_price', '$main.average($price)')
      .apply('avg_rating', $('reviews').average('$rating'))
      .filter(
        $('avg_price')
          .greaterThan(10)
          .and($('avg_rating').greaterThan(0)),
      );

    const plan = ex.simulateQueryPlan({ main });
    const queries = plan.flat().filter(q => typeof q.query === 'string');
    const mainQueries = queries.filter(
      q => q.query.includes('"main_ds"') && !q.query.includes('main_ds-reviews'),
    );

    // Main keeps avg_price HAVING — that clause is its own apply. Rendering
    // varies (column on either side of the comparator depending on the
    // expression direction), so just assert the apply name appears inside
    // the HAVING clause at all.
    const mainKeepsAvgPrice = mainQueries.some(q => /HAVING[\s\S]*avg_price/i.test(q.query));
    expect(mainKeepsAvgPrice, 'main HAVING retains avg_price clause').to.equal(true);

    // Main drops avg_rating HAVING — it's a linked apply, goes post-join.
    for (const q of mainQueries) {
      expect(q.query, 'main HAVING drops avg_rating clause').to.not.match(
        /HAVING[\s\S]*avg_rating/i,
      );
    }
  });
});
