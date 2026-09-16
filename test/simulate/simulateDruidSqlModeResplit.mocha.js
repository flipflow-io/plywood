const { expect } = require('chai');
const plywood = require('../plywood');
const { External, DruidDialect, $, ply } = plywood;

// MODE over a RESPLIT measure: the operand of the mode is the per-sub-key
// aggregate of an inner split, e.g. "the most frequent per-product average price":
//
//   $main.split($reference).apply('B', $main.average($pvp)).mode($B)
//
// The mode field is NOT a base column — it only exists inside cte_subsplit — so the
// ROW_NUMBER / scalar-subquery patterns of the simple MODE must read FROM the CTE and
// mode the inner alias (B_main_0), never a raw "B".

const attributes = [
  { name: '__time', type: 'TIME' },
  { name: 'pvp', type: 'NUMBER', unsplitable: true },
  { name: 'reference', type: 'STRING' },
  { name: 'userIdProduct', type: 'STRING' },
  { name: 'competitor', type: 'STRING' },
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

const timeFilter =
  '(TIMESTAMP \'2026-06-07 00:00:00\'<="__time" AND "__time"<TIMESTAMP \'2026-06-08 00:00:00\')';

// $main.split($reference).apply('B', $main.average($pvp)).mode($B)
function modeOfAvgResplit() {
  return $('main').split('$reference', 'ref').apply('B', $('main').average('$pvp')).mode('$B');
}

// $main.split($reference).apply('B', $main.max($pvp)).mode($B)
function modeOfMaxResplit() {
  return $('main').split('$reference', 'ref').apply('B', $('main').max('$pvp')).mode('$B');
}

// $main.filter($pvp>0).split($reference).apply('B', $main.max($pvp)).mode($B)
function modeOfFilteredMaxResplit() {
  return $('main')
    .filter('$pvp > 0')
    .split('$reference', 'ref')
    .apply('B', $('main').max('$pvp'))
    .mode('$B');
}

function queriesOf(plan) {
  return plan.flat().filter(function (q) {
    return q && typeof q.query === 'string';
  });
}

// Shared cte_subsplit for the OUTER-SPLIT cases: inner key s0, outer dim s1.
function cteOuterSplit(aggSQL, whereSQL) {
  return [
    'WITH',
    '  cte_subsplit AS (',
    'SELECT "reference" AS "s0",',
    '"competitor" AS "s1",',
    aggSQL + ' AS "B_main_0"',
    'FROM "histories" AS t',
    'WHERE ' + whereSQL,
    'GROUP BY "reference", "competitor"',
    ')',
  ];
}

// ROW_NUMBER mode query over the CTE, partitioned by the outer dim.
function modeOverCteOuterSplit(applyName) {
  return [
    'SELECT',
    '"Comp",',
    '"__val" AS "' + applyName + '"',
    'FROM (SELECT "s1" AS "Comp",',
    '"B_main_0" AS "__val",',
    'ROW_NUMBER() OVER (PARTITION BY "s1" ORDER BY COUNT(*) DESC) AS "__rn" FROM cte_subsplit AS t WHERE "B_main_0" IS NOT NULL GROUP BY "s1","B_main_0")',
    'WHERE "__rn" = 1',
  ];
}

describe('simulate DruidSql MODE over resplit', function () {
  describe('VALUE / TOTAL mode', function () {
    it('VALUE: mode of avg-resplit -> scalar subquery over cte_subsplit (no MODE_INLINE, no ANY_VALUE)', function () {
      var ex = ply().apply('R', modeOfAvgResplit());
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(1);
      expect(queries[0].query).to.equal(
        [
          'WITH',
          '  cte_subsplit AS (',
          'SELECT "reference" AS "s0",',
          'AVG("pvp") AS "B_main_0"',
          'FROM "histories" AS t',
          'WHERE ' + timeFilter,
          'GROUP BY "reference"',
          ')',
          'SELECT',
          '(SELECT "B_main_0" FROM cte_subsplit WHERE "B_main_0" IS NOT NULL GROUP BY "B_main_0" ORDER BY COUNT(*) DESC LIMIT 1) AS "__VALUE__"',
          'FROM cte_subsplit AS t',
          'GROUP BY ()',
        ].join('\n'),
      );
    });

    it('TOTAL: mode of max-resplit alongside an avg-resplit companion -> one query, mode as scalar subquery', function () {
      var ex = ply()
        .apply('R', modeOfMaxResplit())
        .apply(
          'AvgB',
          $('main').split('$reference', 'ref').apply('B', $('main').max('$pvp')).average('$B'),
        );
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(1);
      expect(queries[0].query).to.equal(
        [
          'WITH',
          '  cte_subsplit AS (',
          'SELECT "reference" AS "s0",',
          'MAX("pvp") AS "B_main_0",',
          'MAX("pvp") AS "B_main_1"',
          'FROM "histories" AS t',
          'WHERE ' + timeFilter,
          'GROUP BY "reference"',
          ')',
          'SELECT',
          '(SELECT "B_main_0" FROM cte_subsplit WHERE "B_main_0" IS NOT NULL GROUP BY "B_main_0" ORDER BY COUNT(*) DESC LIMIT 1) AS "R",',
          'AVG("B_main_1") AS "AvgB"',
          'FROM cte_subsplit AS t',
          'GROUP BY ()',
        ].join('\n'),
      );
    });
  });

  describe('OUTER SPLIT mode (decomposed: normal query + one mode query over the CTE)', function () {
    it('mode of avg-resplit under split($competitor) -> ROW_NUMBER over cte_subsplit partitioned by the outer dim', function () {
      var ex = $('main').split('$competitor', 'Comp').apply('R', modeOfAvgResplit()).limit(10);
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(2);
      // q0: the normal query, clean (no CTE, no ROW_NUMBER)
      expect(queries[0].query).to.equal(
        [
          'SELECT',
          '"competitor" AS "Comp"',
          'FROM "histories" AS t',
          'WHERE ' + timeFilter,
          'GROUP BY 1',
          'LIMIT 10',
        ].join('\n'),
      );
      // q1: the mode query, FROM cte_subsplit
      expect(queries[1].query).to.equal(
        cteOuterSplit('AVG("pvp")', timeFilter).concat(modeOverCteOuterSplit('R')).join('\n'),
      );
    });

    it('mode of avg-resplit alongside COUNT in the same split -> COUNT stays in the normal query', function () {
      var ex = $('main')
        .split('$competitor', 'Comp')
        .apply('Count', $('main').count())
        .apply('R', modeOfAvgResplit())
        .limit(10);
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(2);
      expect(queries[0].query).to.equal(
        [
          'SELECT',
          '"competitor" AS "Comp",',
          'COUNT(*) AS "Count"',
          'FROM "histories" AS t',
          'WHERE ' + timeFilter,
          'GROUP BY 1',
          'LIMIT 10',
        ].join('\n'),
      );
      expect(queries[1].query).to.equal(
        cteOuterSplit('AVG("pvp")', timeFilter).concat(modeOverCteOuterSplit('R')).join('\n'),
      );
    });

    it('mode of filtered max-resplit ($main.filter($pvp>0).split(...)) -> inner filter lands in the CTE WHERE', function () {
      var ex = $('main')
        .split('$competitor', 'Comp')
        .apply('R', modeOfFilteredMaxResplit())
        .limit(10);
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(2);
      expect(queries[1].query).to.equal(
        cteOuterSplit('MAX("pvp")', '(' + timeFilter + ' AND 0<"pvp")')
          .concat(modeOverCteOuterSplit('R'))
          .join('\n'),
      );
    });

    it('sort by the mode-resplit measure -> sort/limit stripped from the normal query (applied post-join)', function () {
      var ex = $('main')
        .split('$competitor', 'Comp')
        .apply('R', modeOfAvgResplit())
        .sort('$R', 'descending')
        .limit(5);
      var queries = queriesOf(ex.simulateQueryPlan({ main: ext() }));
      expect(queries).to.have.length(2);
      expect(queries[0].query).to.equal(
        [
          'SELECT',
          '"competitor" AS "Comp"',
          'FROM "histories" AS t',
          'WHERE ' + timeFilter,
          'GROUP BY 1',
        ].join('\n'),
      );
      expect(queries[1].query).to.equal(
        cteOuterSplit('AVG("pvp")', timeFilter).concat(modeOverCteOuterSplit('R')).join('\n'),
      );
    });
  });

  describe('[guard] never emit MODE_INLINE, a spurious ANY_VALUE, or the inner alias "B" as a column', function () {
    var cases = {
      'VALUE avg': ply().apply('R', modeOfAvgResplit()),
      'VALUE max': ply().apply('R', modeOfMaxResplit()),
      'VALUE filtered max': ply().apply('R', modeOfFilteredMaxResplit()),
      'TOTAL with companion': ply()
        .apply('R', modeOfMaxResplit())
        .apply(
          'AvgB',
          $('main').split('$reference', 'ref').apply('B', $('main').max('$pvp')).average('$B'),
        ),
      'SPLIT avg': $('main').split('$competitor', 'Comp').apply('R', modeOfAvgResplit()),
      'SPLIT max': $('main').split('$competitor', 'Comp').apply('R', modeOfMaxResplit()),
      'SPLIT filtered max': $('main')
        .split('$competitor', 'Comp')
        .apply('R', modeOfFilteredMaxResplit()),
      'SPLIT with COUNT': $('main')
        .split('$competitor', 'Comp')
        .apply('Count', $('main').count())
        .apply('R', modeOfAvgResplit()),
    };

    Object.keys(cases).forEach(function (name) {
      it(name, function () {
        var queries = queriesOf(cases[name].simulateQueryPlan({ main: ext() }));
        expect(queries.length).to.be.greaterThan(0);
        queries.forEach(function (q) {
          expect(q.query, 'MODE_INLINE placeholder').to.not.contain('MODE_INLINE');
          expect(q.query, 'spurious ANY_VALUE').to.not.contain('ANY_VALUE(');
          expect(q.query, 'inner alias "B" leaked as a column').to.not.match(/"B"/);
          // the mode field must always be the CTE alias, and the CTE must exist
          expect(
            q.query.indexOf('"B_main_0"') === -1 || q.query.indexOf('cte_subsplit') !== -1,
          ).to.equal(true);
        });
      });
    });
  });

  describe('[fail-loud] ModeExpression has no inline SQL form', function () {
    it('DruidDialect.modeExpression throws instead of emitting MODE_INLINE', function () {
      var dialect = new DruidDialect();
      expect(function () {
        $('main').mode('$reference').getSQL(dialect);
      }).to.throw(/mode/i);
    });
  });
});
