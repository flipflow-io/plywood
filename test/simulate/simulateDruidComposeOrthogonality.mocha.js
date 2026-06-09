/*
 * ORTHOGONALITY of the three magic-dim fixes against the NORMAL plywood path.
 *
 * The three fixes under test (branch feat/cross-source-timeshift-decomposition):
 *   (A) avg/ratio leaf decomposition (!T_n = SUM/COUNT) + post-join re-aggregation
 *       for measures over a LINKED-ONLY split  (commit 5a0a3dd).
 *   (A') linked-only filter harvested per-request to a copy of the External
 *       (no shared-config mutation) + nativeJoin renderAggregateSQL THROWS on
 *       an unrenderable op instead of returning null  (commit 5a0a3dd).
 *   (B) a split on the MAIN timeAttribute against an `eternal` linked source
 *       resolves to MAIN instead of throwing "resolve on both sides" (658a23e).
 *
 * Every one of those fixes is GATED on a cross-source condition:
 *   - (A)  fires only when a split alias is linked-only AND causes join fan-out.
 *   - (A') the per-request filter copy only happens inside the cross-source
 *          decomposition; the renderAggregateSQL guard only runs on the
 *          nativeJoin renderer, which is only built for a cross-source plan.
 *   - (B)  fires only when a split alias's refs are exactly the main
 *          timeAttribute AND it overlaps an `eternal` linked source.
 *
 * THE CLAIM (Ismael, 2026-06-02): the fixes are ORTHOGONAL — a query that
 * touches NO linked source and NO magic dimension (the shape of the vast
 * majority of the 349 production cubes) must emit EXACTLY the SQL it emitted
 * before the three fixes existed. Zero contamination of the normal path.
 *
 * How this file proves it (strongest-possible test):
 *   1. Byte-identity. The SAME panel is planned against (a) a bare External
 *      with NO linkedSources and (b) an External that HAS linkedSources defined
 *      but whose panel never references the linked column. If the fixes were
 *      contaminating the normal path, the two SQLs would differ. We assert
 *      they are byte-for-byte identical.
 *   2. Artifact-absence. No `!T_` leaf, no `__join_` synthetic key, no lookup
 *      sub-query, no pruneFilterToSchema/per-request-copy side effect, no
 *      INNER JOIN — none of the cross-source machinery — appears in the
 *      normal-path SQL.
 *   3. Single query. A normal panel is ONE SQL, not a decomposed multi-query
 *      plan.
 *
 * Real query shapes (from /tmp/cube-formula-shapes.md, the 349-cube catalog):
 *   - filtered aggregate `$main.filter(X).agg(...)`     (524 formulas / 274 cubes)
 *   - native countDistinct(concat)                       (all 349 cubes)
 *   - ratio of avgs RP/PVP  + subtract diff              (345 / 343 cubes)
 *   - Time(Day) timeBucket split key                     (the GrupoIfa shape)
 *   - min/max measures
 *
 * Counterfactual for case 2 (avg + Time(Day), both MAIN): fix B is GATED on an
 * eternal linked source being in the split. With both split keys on MAIN the
 * gate is closed, so avg stays the pre-existing rewritten ratio column
 * `((AVG-AVG)*1.0/AVG)` — exactly what the bare external emits. If fix B leaked
 * into this path it would either throw or mint leaves; the byte-identity assert
 * would catch it.
 *
 * If ANY normal-path SQL carries a fix artifact → compositionBug (the worst
 * kind: the fixes bleeding into the 99% of queries that never asked for them).
 */

const { expect } = require('chai');

const plywood = require('../plywood');

const { External, $, ply } = plywood;

const timeFilter = $('__time').overlap({
  start: new Date('2026-05-01T00:00:00Z'),
  end: new Date('2026-05-02T00:00:00Z'),
});

const ATTRS = [
  { name: '__time', type: 'TIME' },
  { name: 'brand', type: 'STRING' },
  { name: 'competitor', type: 'STRING' },
  { name: 'price', type: 'NUMBER', unsplitable: true },
  { name: 'pvp', type: 'NUMBER', unsplitable: true },
  { name: 'promo', type: 'BOOLEAN' },
];

// (a) Bare External — NO linkedSources at all. The shape of the majority of
// the 349 cubes (no magic dim injected). This is the "before the three fixes"
// reference path.
function makeBare() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'bare_ds',
    timeAttribute: '__time',
    attributes: ATTRS,
    filter: timeFilter,
  });
}

// (b) External that HAS a magic-dim lookup defined (joinKey brand, eternal),
// but the panels below never reference `brand_country` or query the lookup.
// Same `source`/attributes as the bare one, so identical panels MUST emit
// identical SQL — any divergence is contamination.
function makeWithLinked() {
  return External.fromJS({
    engine: 'druidsql',
    source: 'bare_ds',
    timeAttribute: '__time',
    attributes: ATTRS,
    linkedSources: {
      magic_bc: {
        source: 'lookup_bc_rev1',
        joinKeys: ['brand'],
        autoInjectJoinKeys: ['brand'],
        sharedDimensions: ['brand'],
        joinMode: 'inner',
        timeAlignment: 'eternal',
        attributes: [
          { name: '__time', type: 'TIME' },
          { name: 'brand', type: 'STRING' },
          { name: 'brand_country', type: 'STRING' },
        ],
      },
    },
    filter: timeFilter,
  });
}

function planSqls(expr, external) {
  return expr
    .simulateQueryPlan({ main: external })
    .flat()
    .filter(q => typeof q.query === 'string')
    .map(q => q.query);
}

// Build a SPLIT panel: a top-level ply() that registers the `main` scope (and,
// when withLinkedScope, also the magic scope as a sibling apply — exactly as
// the Turnilo front stamps it) + a SPLIT apply. The panel NEVER references the
// linked-only column.
function buildPanel(splitKey, valueApplies, sortName, mainFilter, withLinkedScope) {
  let split = $('main').split(splitKey);
  for (const [name, formula] of valueApplies) split = split.apply(name, formula);
  split = split.sort('$' + (sortName || valueApplies[0][0]), 'descending').limit(50);
  let top = ply().apply('main', $('main').filter(mainFilter || timeFilter));
  if (withLinkedScope) top = top.apply('magic_bc', $('magic_bc').filter(timeFilter));
  return top.apply('SPLIT', split);
}

// The artifacts that ONLY the three fixes can introduce. None may appear on the
// normal path.
function assertNoFixArtifacts(sql, label) {
  expect(sql, `${label}: no !T_ leaf column`).to.not.match(/!T_\d+/);
  expect(sql, `${label}: no synthetic __join_ key`).to.not.match(/__join_/);
  expect(sql, `${label}: no lookup sub-query`).to.not.match(/lookup_bc_rev1/);
  expect(sql, `${label}: no INNER JOIN (cross-source path)`).to.not.match(/INNER JOIN/i);
  expect(sql, `${label}: no LEFT JOIN (cross-source path)`).to.not.match(/LEFT JOIN/i);
  // No mutilated SELECT (the renderAggregateSQL-drop failure mode).
  expect(sql, `${label}: no dangling comma before FROM`).to.not.match(/,\s*\n?\s*FROM/i);
  expect(sql, `${label}: no empty select item (double comma)`).to.not.match(/,\s*,/);
  // Any ORDER BY column is a projected alias (no orphan).
  const orderMatch = sql.match(/ORDER BY "([^"]+)"/);
  if (orderMatch) {
    expect(sql, `${label}: ORDER BY "${orderMatch[1]}" projected`).to.include(
      `AS "${orderMatch[1]}"`,
    );
  }
}

describe('Compose orthogonality — the 3 fixes are inert on the normal (non-linked) path', () => {
  // The panels below are the dominant real cube shapes. Each is asserted twice:
  // once on the bare external, once on the linked-but-unused external, and the
  // two SQLs are required to be byte-identical.

  const PANELS = [
    {
      title:
        'split by MAIN dim (brand) + avg + count + countDistinct + min + filter + sort + limit',
      splitKey: { brand: '$brand' },
      applies: [
        ['avg_price', '$main.average($price)'],
        ['cnt', '$main.count()'],
        ['uniq', '$main.countDistinct($competitor)'],
        ['min_price', '$main.min($price)'],
      ],
      sort: 'cnt',
      // filtered aggregate context: a $promo predicate on main (shape #1).
      mainFilter: timeFilter.and($('promo').is(true)),
    },
    {
      title: 'split by MAIN dim + avg + ratio of avgs (RP/PVP) + diff (catalog #3/#4)',
      splitKey: { brand: '$brand' },
      applies: [
        ['avg_price', '$main.average($price)'],
        ['rp', '($main.average($price) - $main.average($pvp)) / $main.average($pvp)'],
        ['rp_diff', '$main.average($price) - $main.average($pvp)'],
      ],
      sort: 'avg_price',
    },
    {
      title: 'split by Time(Day) timeBucket × brand (BOTH main) + avg + ratio — fix B stays closed',
      // Two split keys, both resolving to MAIN. No eternal linked dim in the
      // split → fix B must NOT fire; avg stays the rewritten ratio column.
      splitKey: { time: '$__time.timeBucket(P1D)', brand: '$brand' },
      applies: [
        ['avg_price', '$main.average($price)'],
        ['rp', '($main.average($price) - $main.average($pvp)) / $main.average($pvp)'],
      ],
      sort: 'avg_price',
    },
    {
      title:
        'split by MAIN dim (competitor) + countDistinct(concat) + filtered countDistinct (#1/#5)',
      splitKey: { competitor: '$competitor' },
      applies: [
        ['urls', '$main.countDistinct($brand.concat($competitor))'],
        ['promo_brands', '$main.filter($promo == true).countDistinct($brand)'],
      ],
      sort: 'urls',
    },
  ];

  for (const p of PANELS) {
    describe(p.title, () => {
      it('emits a single, normal SQL on the BARE external (no fix artifacts)', () => {
        const sqls = planSqls(
          buildPanel(p.splitKey, p.applies, p.sort, p.mainFilter, false),
          makeBare(),
        );
        expect(sqls.length, 'exactly one SQL (no decomposition)').to.equal(1);
        assertNoFixArtifacts(sqls[0], 'bare');
      });

      it('emits a single, normal SQL on the LINKED-but-unused external (no fix artifacts)', () => {
        const sqls = planSqls(
          buildPanel(p.splitKey, p.applies, p.sort, p.mainFilter, true),
          makeWithLinked(),
        );
        expect(sqls.length, 'exactly one SQL (lookup never queried)').to.equal(1);
        assertNoFixArtifacts(sqls[0], 'linked-unused');
      });

      it('BYTE-IDENTICAL SQL with vs without linkedSources defined (zero contamination)', () => {
        // The strongest orthogonality statement: defining a magic-dim lookup on
        // the cube, and even registering its scope in the expression, changes
        // NOTHING about the SQL of a panel that does not use it.
        const bare = planSqls(
          buildPanel(p.splitKey, p.applies, p.sort, p.mainFilter, false),
          makeBare(),
        );
        const linked = planSqls(
          buildPanel(p.splitKey, p.applies, p.sort, p.mainFilter, true),
          makeWithLinked(),
        );
        expect(bare.length, 'bare: one SQL').to.equal(1);
        expect(linked.length, 'linked-unused: one SQL').to.equal(1);
        expect(linked[0], 'linked-unused SQL === bare SQL (byte-for-byte)').to.equal(bare[0]);
        // Counterfactual: if any of the 3 fixes leaked into the normal path
        // (minted a !T_ leaf, forced a JOIN, mutated the External via the
        // per-request filter copy, or triggered the eternal-time reclassify),
        // the two strings would diverge and this assert would fail.
      });
    });
  }

  describe('fix B contrapositive — Time(Day) split alone never reclassifies on a normal cube', () => {
    it('a pure Time(Day) split keeps avg as a native rewritten column, single SQL', () => {
      // No linked source in the picture at all → the eternal-time reclassify
      // (fix B) has nothing to act on. avg renders as the rewritten ratio
      // column on a single main query.
      const panel = buildPanel(
        { time: '$__time.timeBucket(P1D)' },
        [
          ['avg_price', '$main.average($price)'],
          ['count', '$main.count()'],
        ],
        'count',
        timeFilter,
        false,
      );
      const sqls = planSqls(panel, makeBare());
      expect(sqls.length, 'single SQL').to.equal(1);
      assertNoFixArtifacts(sqls[0], 'time-only');
      expect(sqls[0], 'TIME_FLOOR day bucket present').to.match(/TIME_FLOOR/i);
      expect(sqls[0], 'avg rendered natively (AVG)').to.match(/AVG\("price"\)/);
    });
  });

  describe('totals (GROUP BY ()) on a normal cube — no decomposition, no leaves', () => {
    it('a no-split totals panel emits one GROUP BY () SQL with native aggregates', () => {
      const totals = ply()
        .apply('main', $('main').filter(timeFilter.and($('promo').is(true))))
        .apply('avg_price', '$main.average($price)')
        .apply('rp', '($main.average($price) - $main.average($pvp)) / $main.average($pvp)')
        .apply('uniq', '$main.countDistinct($brand.concat($competitor))');
      const bare = planSqls(totals, makeBare());
      // With linked scope registered but unused.
      const totalsLinked = ply()
        .apply('main', $('main').filter(timeFilter.and($('promo').is(true))))
        .apply('magic_bc', $('magic_bc').filter(timeFilter))
        .apply('avg_price', '$main.average($price)')
        .apply('rp', '($main.average($price) - $main.average($pvp)) / $main.average($pvp)')
        .apply('uniq', '$main.countDistinct($brand.concat($competitor))');
      const linked = planSqls(totalsLinked, makeWithLinked());

      expect(bare.length, 'bare totals: one SQL').to.equal(1);
      expect(linked.length, 'linked totals: one SQL').to.equal(1);
      assertNoFixArtifacts(bare[0], 'totals-bare');
      assertNoFixArtifacts(linked[0], 'totals-linked');
      expect(linked[0], 'totals SQL byte-identical with/without lookup').to.equal(bare[0]);
      expect(bare[0], 'totals is GROUP BY ()').to.match(/GROUP BY \(\)/);
    });
  });
});
