/*
 * Pin: a cube-level filter with a NEGATED clause over main-only columns
 * must prune to identity (no WHERE) on the linked sub-query — never to
 * `WHERE FALSE`.
 *
 * Live regression (2026-06-02, GrupoIfa dev): the cube carries
 * `NOT($url.in([...12KB allowlist])) AND 0 < $price`. pruneFilterToSchema
 * rewrote the inner `$url.in(...)` to TRUE and then negated it, emitting
 * `WHERE FALSE` on the lookup sub-query. Zero lookup rows + inner join
 * dropped every main row: the magic-dim split returned one naked total
 * with the linked column missing. Counterfactual: reverting the NOT
 * branch to `inner.not().simplify()` makes these tests fail with the
 * FALSE filter / empty join again.
 */

const { expect } = require('chai');
const plywood = require('../plywood');
const { External, Expression, $, ply, r } = plywood;

function mkMain(filterExpr) {
  return External.fromJS({
    engine: 'druidsql',
    source: 'histories',
    timeAttribute: '__time',
    allowEternity: true,
    attributes: [
      { name: '__time', type: 'TIME' },
      { name: 'url', type: 'STRING' },
      { name: 'competitor', type: 'STRING' },
      { name: 'price', type: 'NUMBER', unsplitable: true },
    ],
    filter: filterExpr,
    linkedSources: {
      magic_cc: {
        source: 'lookup_cc_rev1',
        joinKeys: ['competitor'],
        autoInjectJoinKeys: ['competitor'],
        sharedDimensions: ['competitor'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'competitor', type: 'STRING' },
          { name: 'competitor_country', type: 'STRING' },
        ],
      },
    },
  });
}

function linkedQueries(plan) {
  return plan
    .flat()
    .map(q => (typeof q === 'string' ? q : q && q.query))
    .filter(q => typeof q === 'string' && q.includes('lookup_cc_rev1'));
}

function splitByCountry() {
  return ply()
    .apply('main', $('main'))
    .apply(
      'data',
      $('main')
        .split({ competitor_country: $('competitor_country') }, 'main')
        .apply('cnt', $('main').count()),
    );
}

describe('Cross-source: negated cube filter prunes to identity on the linked side', () => {
  it('NOT(url IN set) AND 0 < price — the GrupoIfa dev shape — emits no FALSE on the lookup', () => {
    const filter = $('url')
      .in(['https://a/p1', 'https://b/p2'])
      .not()
      .and(r(0).lessThan($('price')))
      .toJS();

    const plan = splitByCountry().simulateQueryPlan({ main: mkMain(filter) });
    const linked = linkedQueries(plan);
    expect(linked.length, 'lookup sub-query emitted').to.be.greaterThan(0);
    for (const q of linked) {
      expect(q, `lookup SQL must not be emptied:\n${q}`).to.not.match(/WHERE\s+FALSE/i);
      expect(q, `main-only columns must not leak into lookup SQL:\n${q}`).to.not.match(
        /"url"|"price"/,
      );
    }
  });

  it('bare NOT over a main-only column prunes to identity, not FALSE', () => {
    const filter = $('url').in(['https://a/p1']).not().toJS();
    const plan = splitByCountry().simulateQueryPlan({ main: mkMain(filter) });
    for (const q of linkedQueries(plan)) {
      expect(q).to.not.match(/WHERE\s+FALSE/i);
    }
  });

  it('literal-first predicate (0 < price) prunes instead of leaking an unknown column', () => {
    const filter = r(0).lessThan($('price')).toJS();
    const plan = splitByCountry().simulateQueryPlan({ main: mkMain(filter) });
    for (const q of linkedQueries(plan)) {
      expect(q).to.not.match(/"price"/);
      expect(q).to.not.match(/WHERE\s+FALSE/i);
    }
  });

  it('a NOT over a column the lookup DOES have survives the prune', () => {
    const filter = $('competitor').in(['Excluido']).not().toJS();
    const plan = splitByCountry().simulateQueryPlan({ main: mkMain(filter) });
    const linked = linkedQueries(plan);
    expect(linked.length).to.be.greaterThan(0);
    // competitor exists on both sides — the exclusion must reach the lookup.
    expect(linked.some(q => /competitor/.test(q) && /Excluido/.test(q))).to.equal(true);
  });
});
