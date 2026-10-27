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
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External, BigQueryExternal, BigQueryDialect, $, ply, r, AttributeInfo } = plywood;

function rowsRequester(rows, seen = []) {
  return ({ query }) => {
    seen.push(query);
    const stream = new PassThrough({ objectMode: true });
    setTimeout(() => {
      for (const row of rows) stream.write(row);
      stream.end();
    }, 1);
    return stream;
  };
}

const attributes = [
  { name: '__time', type: 'TIME' },
  { name: 'competitor', type: 'STRING' },
  { name: 'price', type: 'NUMBER' },
  { name: 'tags', type: 'STRING', nativeType: 'ARRAY<STRUCT<v STRING>>' },
];

describe('BigQueryExternal', () => {
  describe('postProcessIntrospect', () => {
    it('maps BigQuery fields to Plywood types', () => {
      const fields = [
        { name: '__time', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'competitor', type: 'STRING', mode: 'NULLABLE' },
        { name: 'count', type: 'INTEGER', mode: 'NULLABLE' },
        { name: 'price', type: 'FLOAT', mode: 'NULLABLE' },
        { name: 'onSale', type: 'BOOLEAN', mode: 'NULLABLE' },
        { name: 'attributes', type: 'JSON', mode: 'NULLABLE' },
        {
          name: 'tags',
          type: 'RECORD',
          mode: 'REPEATED',
          fields: [{ name: 'v', type: 'STRING', mode: 'NULLABLE' }],
        },
        { name: 'scores', type: 'INTEGER', mode: 'REPEATED' },
        { name: 'shape', type: 'GEOGRAPHY', mode: 'NULLABLE' },
      ];

      expect(AttributeInfo.toJSs(BigQueryExternal.postProcessIntrospect(fields))).to.deep.equal([
        { name: '__time', type: 'TIME', nativeType: 'TIMESTAMP' },
        { name: 'competitor', type: 'STRING', nativeType: 'STRING' },
        { name: 'count', type: 'NUMBER', nativeType: 'INT64' },
        { name: 'price', type: 'NUMBER', nativeType: 'FLOAT64' },
        { name: 'onSale', type: 'BOOLEAN', nativeType: 'BOOL' },
        { name: 'attributes', type: 'STRING', nativeType: 'JSON' },
        { name: 'tags', type: 'STRING', nativeType: 'ARRAY<STRUCT<v STRING>>' },
      ]);
    });

    it('introspects with a schema only request (a free dry run)', () => {
      const seen = [];
      const external = External.fromJS(
        { engine: 'bigquery', source: 'project.dataset.histories' },
        rowsRequester(
          [
            { name: '__time', type: 'TIMESTAMP' },
            { name: 'competitor', type: 'STRING' },
          ],
          seen,
        ),
      );

      return external.introspect().then(introspected => {
        expect(seen).to.deep.equal([
          { query: 'SELECT * FROM `project.dataset.histories`', schemaOnly: true },
        ]);
        expect(AttributeInfo.toJSs(introspected.attributes)).to.deep.equal([
          { name: '__time', type: 'TIME', nativeType: 'TIMESTAMP' },
          { name: 'competitor', type: 'STRING', nativeType: 'STRING' },
        ]);
      });
    });
  });

  describe('queries', () => {
    it('returns 0 on a totals query with no data', () => {
      const external = External.fromJS(
        { engine: 'bigquery', source: 'histories', attributes },
        rowsRequester([]),
      );

      return ply()
        .apply('Count', '$wiki.count()')
        .compute({ wiki: external })
        .then(result => {
          expect(result.toJS().data).to.deep.equal([{ Count: 0 }]);
        });
    });

    it('sends only the BigQuery job settings of the context', () => {
      const seen = [];
      const external = External.fromJS(
        {
          engine: 'bigquery',
          source: 'histories',
          attributes,
          context: {
            maximumBytesBilled: '1000000000',
            jobTimeoutMs: 60000,
            labels: { cube: 'histories' },
            lookupTables: {},
          },
        },
        rowsRequester([{ __VALUE__: 3 }], seen),
      );

      return $('wiki')
        .count()
        .compute({ wiki: external })
        .then(result => {
          expect(result).to.equal(3);
          expect(seen).to.deep.equal([
            {
              query: 'SELECT\nCOUNT(*) AS `__VALUE__`\nFROM `histories` AS t\nGROUP BY ()',
              maximumBytesBilled: '1000000000',
              jobTimeoutMs: 60000,
              labels: { cube: 'histories' },
            },
          ]);
        });
    });

    it('reads several sources as one, a missing column being NULL (Druid TABLE(APPEND))', () => {
      const external = External.fromJS(
        { engine: 'bigquery', source: ['a', 'b'], attributes },
        rowsRequester([]),
      );
      expect(external.getFrom()).to.equal(
        'FROM (SELECT * FROM `a` FULL UNION ALL BY NAME SELECT * FROM `b`) AS t',
      );
    });

    it('finds lookup tables next to the source unless the context names them', () => {
      expect(
        BigQueryExternal.lookupTableFor({ source: 'project.dataset.histories' }, 'brands'),
      ).to.equal('project.dataset.lookup__brands');
      expect(
        BigQueryExternal.lookupTableFor(
          { source: 'histories', context: { lookupTables: { brands: 'other.brands' } } },
          'brands',
        ),
      ).to.equal('other.brands');
    });

    it('filters a measure on a multi-value column per row, as Druid does', () => {
      const external = External.fromJS(
        { engine: 'bigquery', source: 'histories', attributes },
        rowsRequester([]),
      );

      // A single filtered measure has its filter lifted into WHERE; next to another measure it stays inside
      const queryPlan = ply()
        .apply('All', $('wiki').count())
        .apply(
          'Tagged',
          $('wiki')
            .filter($('tags').is(r('a')))
            .sum('$price'),
        )
        .simulateQueryPlan({ wiki: external });

      expect(queryPlan.flat().map(q => q.query)).to.deep.equal([
        "SELECT\nCOUNT(*) AS `All`,\nSUM(CASE WHEN (`__mvm_0` = TRUE) THEN `price` ELSE 0 END) AS `Tagged`\nFROM (SELECT t.*, EXISTS(SELECT 1 FROM UNNEST(IF(ARRAY_LENGTH(`tags`)>0,`tags`,[STRUCT(CAST(NULL AS STRING) AS v)])) WHERE (`v`='a')) AS `__mvm_0` FROM `histories` AS t) AS t\nGROUP BY ()",
      ]);
    });

    it('keeps a measure filter on a multi-value column per row when splitting on that column', () => {
      const external = External.fromJS(
        { engine: 'bigquery', source: 'histories', attributes },
        rowsRequester([]),
      );

      const queryPlan = $('wiki')
        .split('$tags', 'Tag')
        .apply('All', $('wiki').count())
        .apply(
          'Tagged',
          $('wiki')
            .filter($('tags').is(r('a')))
            .count(),
        )
        .sort('$All', 'descending')
        .limit(5)
        .simulateQueryPlan({ wiki: external });

      expect(queryPlan.flat().map(q => q.query)).to.deep.equal([
        "SELECT\n`__mv__tags` AS `Tag`,\nCOUNT(*) AS `All`,\nSUM(CASE WHEN (`__mvm_0` = TRUE) THEN 1 ELSE 0 END) AS `Tagged`\nFROM (SELECT t.*, EXISTS(SELECT 1 FROM UNNEST(IF(ARRAY_LENGTH(`tags`)>0,`tags`,[STRUCT(CAST(NULL AS STRING) AS v)])) WHERE (`v`='a')) AS `__mvm_0`, __mvs_0.v AS `__mv__tags` FROM `histories` AS t LEFT JOIN UNNEST(t.`tags`) AS __mvs_0) AS t\nGROUP BY 1\nORDER BY IS_NAN(`All`) IS TRUE DESC, `All` DESC\nLIMIT 5",
      ]);
    });

    it('refuses a multi-value column inside a measure other than in a filter', () => {
      const external = External.fromJS(
        { engine: 'bigquery', source: 'histories', attributes },
        rowsRequester([]),
      );

      expect(() =>
        ply()
          .apply('All', $('wiki').count())
          .apply('Tags', $('wiki').countDistinct('$tags'))
          .simulateQueryPlan({ wiki: external }),
      ).to.throw("multi-value column in measure 'Tags' is only supported in a filter");
    });
  });

  describe('BigQueryDialect', () => {
    const dialect = new BigQueryDialect();

    it('quotes names with backticks and literals with backslash escapes', () => {
      expect(dialect.escapeName('a`b')).to.equal('`a\\`b`');
      expect(dialect.escapeLiteral("it's a \\ path\n")).to.equal("'it\\'s a \\\\ path\\n'");
    });

    it('orders NaN highest and NULL lowest, as Druid groupBy does', () => {
      expect(dialect.orderByExpression('`x`', 'ASC', 'NUMBER')).to.equal(
        'IS_NAN(`x`) IS TRUE ASC, `x` ASC',
      );
      expect(dialect.orderByExpression('`x`', 'DESC', 'NUMBER')).to.equal(
        'IS_NAN(`x`) IS TRUE DESC, `x` DESC',
      );
      expect(dialect.orderByExpression('`x`', 'DESC', 'STRING')).to.equal('`x` DESC');
    });

    it('rewrites JSON paths Druid reads into BigQuery syntax', () => {
      const raw = sql =>
        dialect.translateRawSql(plywood.Expression.parse(`s\${${sql}}`).parsedSql, sql);
      expect(raw("JSON_VALUE(attributes, '$.a[0][''Saturated Fat''][0].v')")).to.equal(
        'JSON_VALUE(`attributes`, \'$.a[0]."Saturated Fat"[0].v\')',
      );
      // Druid does not read this form and gives NULL
      expect(raw('JSON_VALUE(attributes, \'$."Volume optional"[0]\')')).to.equal(
        'CAST(NULL AS STRING)',
      );
    });
  });
});
