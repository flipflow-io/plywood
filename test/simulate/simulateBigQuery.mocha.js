/*
 * Copyright 2015-2020 Imply Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const { expect } = require('chai');
const { sane } = require('../utils');

const plywood = require('../plywood');

const { External, $, ply, r, Expression } = plywood;

// Every SQL below was dry-run on BigQuery against the 507 copy (etl_lab.dbq__507_histories) and is valid there
const context = {
  main: External.fromJS({
    engine: 'bigquery',
    source: 'histories',
    attributes: [
      { name: '__time', type: 'TIME', nativeType: 'TIMESTAMP' },
      { name: 'competitor', type: 'STRING', nativeType: 'STRING' },
      { name: 'brand', type: 'STRING', nativeType: 'STRING' },
      { name: 'webBrand', type: 'STRING', nativeType: 'STRING' },
      { name: 'isOwnProduct', type: 'STRING', nativeType: 'STRING' },
      { name: 'price', type: 'NUMBER', nativeType: 'FLOAT64' },
      { name: 'count', type: 'NUMBER', nativeType: 'INT64' },
      { name: 'matched_ifa_associates_mv', type: 'STRING', nativeType: 'ARRAY<STRUCT<v STRING>>' },
    ],
    context: { lookupTables: { ifa_mg_cleaner: 'lookup_ifa_mg_cleaner' } },
  }),
};

const day = $('__time').overlap(new Date('2026-10-25T00:00:00Z'), new Date('2026-10-26T00:00:00Z'));
const main = $('main').filter(day);
const mv = $('matched_ifa_associates_mv');
const cases = {
  advanced: ply()
    .apply('main', main.filter($('competitor').is(r('Carrefoures'))))
    .apply('Count', '$main.count()')
    .apply('AvgPrice', '$main.average($price)')
    .apply('Ratio', '$main.sum($price) / $main.sum($count)')
    .apply(
      'Own',
      $('main')
        .filter($('isOwnProduct').is(r('1')))
        .count(),
    )
    .apply('Products', '$main.countDistinct($brand)')
    .apply(
      'Brands',
      $('main')
        .split('$brand', 'Brand')
        .apply('Count', '$main.count()')
        .sort('$Count', 'descending')
        .limit(5),
    ),
  timeBuckets: ply()
    .apply('main', main)
    .apply(
      'Days',
      $('main')
        .split($('__time').timeBucket('P1D', 'Etc/UTC'), 'Day')
        .apply('Count', '$main.count()')
        .sort('$Day', 'ascending'),
    )
    .apply(
      'Weeks',
      $('main')
        .split($('__time').timeBucket('P1W', 'Europe/Madrid'), 'Week')
        .apply('Count', '$main.count()')
        .sort('$Week', 'ascending'),
    )
    .apply(
      'EightHours',
      $('main')
        .split($('__time').timeBucket('PT8H', 'Europe/Madrid'), 'Bucket')
        .apply('Count', '$main.count()')
        .sort('$Bucket', 'ascending'),
    )
    .apply(
      'HalfYears',
      $('main')
        .split($('__time').timeBucket('P6M', 'Etc/UTC'), 'Half')
        .apply('Count', '$main.count()')
        .sort('$Half', 'ascending'),
    ),
  stringOps: ply()
    .apply(
      'main',
      main.filter(
        $('brand')
          .contains('dove', 'ignoreCase')
          .and($('competitor').match('^Carre'))
          .and($('brand').substr(0, 3).is(r('Dov'))),
      ),
    )
    .apply('Count', '$main.count()'),
  lookup: ply()
    .apply('main', main)
    .apply(
      'Groups',
      $('main')
        .split($('brand').lookup('ifa_mg_cleaner').fallback('$brand'), 'Group')
        .apply('Count', '$main.count()')
        .sort('$Count', 'descending')
        .limit(5),
    ),
  rawSql: ply()
    .apply(
      'main',
      main.filter(
        Expression.parse(
          's${isOwnProduct = 1 AND LOOKUP("webBrand", \'ifa_mg_cleaner\') IS NOT NULL}',
        ),
      ),
    )
    .apply(
      'Weight',
      Expression.parse(
        "$main.average(s${CAST(JSON_VALUE(attributes, '$.Weight[0].WeightValue') AS DOUBLE) / price})",
      ),
    )
    .apply(
      'Days',
      $('main')
        .split(Expression.parse("s${TIME_FORMAT(__time, 'yyyy-MM-dd')}"), 'Day')
        .apply('Count', '$main.count()')
        .sort('$Day', 'ascending'),
    ),
  multiValueSplit: ply()
    .apply('main', main)
    .apply(
      'Associates',
      $('main')
        .split(mv, 'Associate')
        .apply('Count', '$main.count()')
        .sort('$Count', 'descending')
        .limit(5),
    ),
  multiValuePinned: ply()
    .apply('main', main.filter(mv.is(r('Ahorramascom'))))
    .apply(
      'Associates',
      $('main')
        .split(mv, 'Associate')
        .apply('Count', '$main.count()')
        .sort('$Count', 'descending')
        .limit(5),
    ),
  multiValueExclude: ply()
    .apply('main', main.filter(mv.in(['Ahorramascom', 'Pamplonaeleclerces']).not()))
    .apply(
      'Competitors',
      $('main')
        .split('$competitor', 'Competitor')
        .apply('Count', '$main.count()')
        .sort('$Count', 'descending')
        .limit(5),
    ),
  mode: ply()
    .apply('main', main)
    .apply(
      'Competitors',
      $('main')
        .split('$competitor', 'Competitor')
        .apply('Count', '$main.count()')
        .apply('ModePrice', '$main.mode($price)')
        .sort('$Count', 'descending')
        .limit(5),
    ),
};

describe('simulate BigQuery', () => {
  it('works in advanced case', () => {
    const queryPlan = cases.advanced.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          COUNT(*) AS \`Count\`,
          AVG(\`price\`) AS \`AvgPrice\`,
          IEEE_DIVIDE(SUM(\`price\`),SUM(\`count\`)) AS \`Ratio\`,
          SUM(CASE WHEN (\`isOwnProduct\`='1') THEN 1 ELSE 0 END) AS \`Own\`,
          COUNT(DISTINCT \`brand\`) AS \`Products\`
          FROM \`histories\` AS t
          WHERE ((TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND (\`competitor\`='Carrefoures'))
          GROUP BY ()
        `,
      },
      {
        query: sane`
          SELECT
          \`brand\` AS \`Brand\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE ((TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND (\`competitor\`='Carrefoures'))
          GROUP BY 1
          ORDER BY IS_NAN(\`Count\`) IS TRUE DESC, \`Count\` DESC
          LIMIT 5
        `,
      },
    ]);
  });

  it('works with time buckets (ISO week, fixed-length buckets from the epoch in the time zone, calendar multiples)', () => {
    const queryPlan = cases.timeBuckets.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          TIMESTAMP_TRUNC(\`__time\`, DAY) AS \`Day\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00')
          GROUP BY 1
          ORDER BY \`Day\` ASC
        `,
      },
      {
        query: sane`
          SELECT
          TIMESTAMP_TRUNC(\`__time\`, ISOWEEK, 'Europe/Madrid') AS \`Week\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00')
          GROUP BY 1
          ORDER BY \`Week\` ASC
        `,
      },
      {
        query: sane`
          SELECT
          TIMESTAMP_BUCKET(\`__time\`, INTERVAL 28800 SECOND, TIMESTAMP('1970-01-01 00:00:00', 'Europe/Madrid')) AS \`Bucket\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00')
          GROUP BY 1
          ORDER BY \`Bucket\` ASC
        `,
      },
      {
        query: sane`
          SELECT
          TIMESTAMP(DATETIME_ADD(DATETIME '1970-01-01 00:00:00', INTERVAL ((EXTRACT(YEAR FROM \`__time\`)-1970)*12+EXTRACT(MONTH FROM \`__time\`)-1)-MOD(((EXTRACT(YEAR FROM \`__time\`)-1970)*12+EXTRACT(MONTH FROM \`__time\`)-1),6) MONTH)) AS \`Half\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00')
          GROUP BY 1
          ORDER BY \`Half\` ASC
        `,
      },
    ]);
  });

  it('works with contains, match and substr', () => {
    const queryPlan = cases.stringOps.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          COUNT(*) AS \`__VALUE__\`
          FROM \`histories\` AS t
          WHERE ((((TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND STRPOS(LOWER(CAST(\`brand\` AS STRING)),LOWER('dove'))>0) AND REGEXP_CONTAINS(CAST(\`competitor\` AS STRING), '^Carre')) AND (IF(1>CHAR_LENGTH(\`brand\`),NULL,SUBSTR(\`brand\`,1,3))='Dov'))
          GROUP BY ()
        `,
      },
    ]);
  });

  it('works with a lookup as a (k, v) table', () => {
    const queryPlan = cases.lookup.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          COALESCE((SELECT v FROM \`lookup_ifa_mg_cleaner\` WHERE k = \`brand\`), \`brand\`) AS \`Group\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00')
          GROUP BY 1
          ORDER BY IS_NAN(\`Count\`) IS TRUE DESC, \`Count\` DESC
          LIMIT 5
        `,
      },
    ]);
  });

  it('translates raw Druid SQL formulas', () => {
    const queryPlan = cases.rawSql.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          AVG((IEEE_DIVIDE(SAFE_CAST(JSON_VALUE(\`attributes\`, '$.Weight[0].WeightValue') AS FLOAT64),\`price\`))) AS \`__VALUE__\`
          FROM \`histories\` AS t
          WHERE ((TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND ((SAFE_CAST(\`isOwnProduct\` AS FLOAT64) = 1) AND ((SELECT v FROM \`lookup_ifa_mg_cleaner\` WHERE k = \`webBrand\`) IS NOT NULL)))
          GROUP BY ()
        `,
      },
      {
        query: sane`
          SELECT
          (FORMAT_TIMESTAMP('%Y-%m-%d', \`__time\`)) AS \`Day\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE ((TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND ((SAFE_CAST(\`isOwnProduct\` AS FLOAT64) = 1) AND ((SELECT v FROM \`lookup_ifa_mg_cleaner\` WHERE k = \`webBrand\`) IS NOT NULL)))
          GROUP BY 1
          ORDER BY \`Day\` ASC
        `,
      },
    ]);
  });

  it('splits a multi-value column per element', () => {
    const queryPlan = cases.multiValueSplit.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          \`__mv__matched_ifa_associates_mv\` AS \`Associate\`,
          COUNT(*) AS \`Count\`
          FROM (SELECT t.*, __mvs_0.v AS \`__mv__matched_ifa_associates_mv\` FROM \`histories\` AS t LEFT JOIN UNNEST(t.\`matched_ifa_associates_mv\`) AS __mvs_0) AS t
          WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00')
          GROUP BY 1
          ORDER BY IS_NAN(\`Count\`) IS TRUE DESC, \`Count\` DESC
          LIMIT 5
        `,
      },
    ]);
  });

  it('pins a multi-value split to the value of a top level = filter, as Druid does', () => {
    const queryPlan = cases.multiValuePinned.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          \`__mv__matched_ifa_associates_mv\` AS \`Associate\`,
          COUNT(*) AS \`Count\`
          FROM (SELECT t.*, EXISTS(SELECT 1 FROM UNNEST(IF(ARRAY_LENGTH(\`matched_ifa_associates_mv\`)>0,\`matched_ifa_associates_mv\`,[STRUCT(CAST(NULL AS STRING) AS v)])) WHERE (\`v\`='Ahorramascom')) AS \`__mvp_0\`, 'Ahorramascom' AS \`__mv__matched_ifa_associates_mv\` FROM \`histories\` AS t) AS t
          WHERE ((TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND \`__mvp_0\`)
          GROUP BY 1
          ORDER BY IS_NAN(\`Count\`) IS TRUE DESC, \`Count\` DESC
          LIMIT 5
        `,
      },
    ]);
  });

  it('filters a multi-value column per row', () => {
    const queryPlan = cases.multiValueExclude.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          \`competitor\` AS \`Competitor\`,
          COUNT(*) AS \`Count\`
          FROM (SELECT t.*, EXISTS(SELECT 1 FROM UNNEST(IF(ARRAY_LENGTH(\`matched_ifa_associates_mv\`)>0,\`matched_ifa_associates_mv\`,[STRUCT(CAST(NULL AS STRING) AS v)])) WHERE \`v\` IN ('Ahorramascom','Pamplonaeleclerces')) AS \`__mvp_0\` FROM \`histories\` AS t) AS t
          WHERE ((TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND (\`__mvp_0\`) IS NOT TRUE)
          GROUP BY 1
          ORDER BY IS_NAN(\`Count\`) IS TRUE DESC, \`Count\` DESC
          LIMIT 5
        `,
      },
    ]);
  });

  it('works with mode (escaped helper columns)', () => {
    const queryPlan = cases.mode.simulateQueryPlan(context);
    expect(queryPlan).to.have.length(1);

    expect(queryPlan[0]).to.deep.equal([
      {
        query: sane`
          SELECT
          \`competitor\` AS \`Competitor\`,
          COUNT(*) AS \`Count\`
          FROM \`histories\` AS t
          WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00')
          GROUP BY 1
          ORDER BY IS_NAN(\`Count\`) IS TRUE DESC, \`Count\` DESC
          LIMIT 5
        `,
      },
      {
        query: sane`
          SELECT
          \`Competitor\`,
          \`__val\` AS \`ModePrice\`
          FROM (SELECT \`competitor\` AS \`Competitor\`,
          \`price\` AS \`__val\`,
          ROW_NUMBER() OVER (PARTITION BY \`competitor\` ORDER BY COUNT(*) DESC) AS \`__rn\` FROM \`histories\` AS t WHERE (TIMESTAMP '2026-10-25 00:00:00'<=\`__time\` AND \`__time\`<TIMESTAMP '2026-10-26 00:00:00') AND \`price\` IS NOT NULL GROUP BY \`competitor\`,\`price\`)
          WHERE \`__rn\` = 1
        `,
      },
    ]);
  });
});
