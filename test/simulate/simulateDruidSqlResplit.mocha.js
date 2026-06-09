const { expect } = require('chai');
const plywood = require('../plywood');
const { External, $, ply } = plywood;

const attributes = [
  { name: '__time', type: 'TIME' },
  { name: 'pvp', type: 'NUMBER', unsplitable: true },
  { name: 'qty', type: 'NUMBER', unsplitable: true },
  { name: 'reference', type: 'STRING' },
  { name: 'userIdProduct', type: 'STRING' },
];

function ext() {
  return External.fromJS({
    engine: 'druidsql',
    version: '0.20.0',
    source: 'histories',
    timeAttribute: '__time',
    attributes,
    allowSelectQueries: true,
    filter: $('__time').overlap({
      start: new Date('2026-06-07T00:00:00Z'),
      end: new Date('2026-06-08T00:00:00Z'),
    }),
  });
}

// $main.split($reference).apply('B', $main.average($pvp)).average($B)
function simpleResplit() {
  return $('main').split('$reference', 'ref').apply('B', $('main').average('$pvp')).average('$B');
}

// $main.filter($pvp>0).split(day || COALESCE(reference,userIdProduct)).apply('B',$main.max($pvp)).average($B)
function realResplit() {
  return $('main')
    .filter('$pvp > 0')
    .split(
      $('__time')
        .timeBucket('P1D', 'Etc/UTC')
        .cast('STRING')
        .concat($('reference').fallback('$userIdProduct')),
      'key',
    )
    .apply('B', $('main').max('$pvp'))
    .average('$B');
}

function queriesOf(plan) {
  return plan.flat().filter(function (q) {
    return q && typeof q.query === 'string';
  });
}

describe('simulate DruidSql resplit (nested aggregation)', function () {
  describe('[GREEN] nested CTE after fix', function () {
    it('VALUE/TOTAL mode (simple) -> single nested-CTE query', function () {
      var ex = ply().apply('Resplit', simpleResplit());
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(1);
      expect(queries[0].query).to.equal(
        [
          'WITH',
          '  cte_subsplit AS (',
          'SELECT "reference" AS "s0",',
          'AVG("pvp") AS "B_main_0"',
          'FROM "histories" AS t',
          'WHERE (TIMESTAMP \'2026-06-07 00:00:00\'<="__time" AND "__time"<TIMESTAMP \'2026-06-08 00:00:00\')',
          'GROUP BY "reference"',
          ')',
          'SELECT',
          'AVG("B_main_0") AS "__VALUE__"',
          'FROM cte_subsplit AS t',
          'GROUP BY ()',
        ].join('\n'),
      );
    });

    it('OUTER-SPLIT mode (simple) -> nested CTE grouped by the outer dimension', function () {
      var ex = $('main').split('$reference', 'Reference').apply('Resplit', simpleResplit());
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(1);
      expect(queries[0].query).to.equal(
        [
          'WITH',
          '  cte_subsplit AS (',
          'SELECT "reference" AS "s0",',
          'AVG("pvp") AS "B_main_0"',
          'FROM "histories" AS t',
          'WHERE (TIMESTAMP \'2026-06-07 00:00:00\'<="__time" AND "__time"<TIMESTAMP \'2026-06-08 00:00:00\')',
          'GROUP BY "reference"',
          ')',
          'SELECT',
          '"s0" AS "Reference",',
          'AVG("B_main_0") AS "Resplit"',
          'FROM cte_subsplit AS t',
          'GROUP BY "s0"',
        ].join('\n'),
      );
    });

    it('VALUE/TOTAL (real ISDIN) -> nested CTE with MAX and composite key', function () {
      var ex = ply().apply('Resplit', realResplit());
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(1);
      expect(queries[0].query).to.equal(
        [
          'WITH',
          '  cte_subsplit AS (',
          'SELECT (CAST(TIME_FLOOR("__time", \'P1D\', NULL, \'Etc/UTC\') AS VARCHAR)||COALESCE("reference", "userIdProduct")) AS "s0",',
          'MAX("pvp") AS "B_main_0"',
          'FROM "histories" AS t',
          'WHERE ((TIMESTAMP \'2026-06-07 00:00:00\'<="__time" AND "__time"<TIMESTAMP \'2026-06-08 00:00:00\') AND 0<"pvp")',
          'GROUP BY (CAST(TIME_FLOOR("__time", \'P1D\', NULL, \'Etc/UTC\') AS VARCHAR)||COALESCE("reference", "userIdProduct"))',
          ')',
          'SELECT',
          'AVG("B_main_0") AS "__VALUE__"',
          'FROM cte_subsplit AS t',
          'GROUP BY ()',
        ].join('\n'),
      );
    });

    it('OUTER-SPLIT (real ISDIN) -> nested CTE grouped by the outer dimension', function () {
      var ex = $('main').split('$reference', 'Reference').apply('Resplit', realResplit());
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(1);
      expect(queries[0].query).to.equal(
        [
          'WITH',
          '  cte_subsplit AS (',
          'SELECT (CAST(TIME_FLOOR("__time", \'P1D\', NULL, \'Etc/UTC\') AS VARCHAR)||COALESCE("reference", "userIdProduct")) AS "s0",',
          '"reference" AS "s1",',
          'MAX("pvp") AS "B_main_0"',
          'FROM "histories" AS t',
          // FILTER-PARITY FIX (Concern 1, I2/I3): the resplit measure's inner
          // .filter($pvp>0) now scopes the inner CTE in OUTER-SPLIT mode too,
          // mirroring value/total mode (line 112). Pre-fix this WHERE was time-only,
          // which silently changed the per-key MAX("pvp") row set under a split.
          'WHERE ((TIMESTAMP \'2026-06-07 00:00:00\'<="__time" AND "__time"<TIMESTAMP \'2026-06-08 00:00:00\') AND 0<"pvp")',
          'GROUP BY (CAST(TIME_FLOOR("__time", \'P1D\', NULL, \'Etc/UTC\') AS VARCHAR)||COALESCE("reference", "userIdProduct")), "reference"',
          ')',
          'SELECT',
          '"s1" AS "Reference",',
          'AVG("B_main_0") AS "Resplit"',
          'FROM cte_subsplit AS t',
          'GROUP BY "s1"',
        ].join('\n'),
      );
    });
  });

  // Ogievetsky composability critique: a resplit measure + a plain COMPANION
  // measure under an outer split must not double-emit the companion. The pre-revise
  // split branch only skipped split-bearing applies, so SUM(qty) was rendered twice
  // (once from the CTE via nestedGroupByResult, once raw referencing base "qty").
  describe('[composability] resplit measure + plain companion measure', function () {
    it('OUTER-SPLIT: resplit + SUM(qty) -> exactly one TotalQty column, no raw base-column ref in outer SELECT', function () {
      var ex = $('main')
        .split('$reference', 'Reference')
        .apply('Resplit', simpleResplit())
        .apply('TotalQty', $('main').sum('$qty'));
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(1);
      var q = queries[0].query;
      // exactly one TotalQty output column (pre-revise emitted two)
      expect((q.match(/AS "TotalQty"/g) || []).length, 'one TotalQty column').to.equal(1);
      // the outer SELECT (after the CTE close ')\nSELECT') must not reference the base column "qty"
      var outerSelect = q.slice(q.lastIndexOf('\nSELECT'));
      expect(outerSelect.indexOf('"qty"'), 'no raw base "qty" in outer SELECT').to.equal(-1);
    });
  });
});
