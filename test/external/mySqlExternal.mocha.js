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
const { sane } = require('../utils');

const plywood = require('../plywood');

const { External, TimeRange, $, ply, r, AttributeInfo } = plywood;

describe('MySQLExternal', () => {
  describe('should work when getting back no data', () => {
    const emptyExternal = External.fromJS(
      {
        engine: 'mysql',
        source: 'wikipedia',
        attributes: [
          { name: 'time', type: 'TIME' },
          { name: 'language', type: 'STRING' },
          { name: 'page', type: 'STRING' },
          { name: 'added', type: 'NUMBER' },
        ],
      },
      () => {
        const stream = new PassThrough({ objectMode: true });
        setTimeout(() => {
          stream.end();
        }, 1);
        return stream;
      },
    );

    it('should return null correctly on a totals query', () => {
      const ex = ply().apply('Count', '$wiki.count()');

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(result.toJS().data).to.deep.equal([{ Count: 0 }]);
      });
    });

    it('should return null correctly on a timeseries query', () => {
      const ex = $('wiki')
        .split("$time.timeBucket(P1D, 'Etc/UTC')", 'Time')
        .apply('Count', '$wiki.count()')
        .sort('$Time', 'ascending');

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(result.toJS().data).to.deep.equal([]);
      });
    });

    it('should return null correctly on a topN query', () => {
      const ex = $('wiki')
        .split('$page', 'Page')
        .apply('Count', '$wiki.count()')
        .apply('Added', '$wiki.sum($added)')
        .sort('$Count', 'descending')
        .limit(5);

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(result.toJS().data).to.deep.equal([]);
      });
    });

    it('should return null correctly on a select query', () => {
      const ex = $('wiki');

      return ex.compute({ wiki: emptyExternal }).then(result => {
        expect(AttributeInfo.toJSs(result.attributes)).to.deep.equal([
          { name: 'time', type: 'TIME' },
          { name: 'language', type: 'STRING' },
          { name: 'page', type: 'STRING' },
          { name: 'added', type: 'NUMBER' },
        ]);

        expect(result.toJS().data).to.deep.equal([]);
        expect(result.toCSV()).to.equal('time,language,page,added');
      });
    });
  });

  describe('getFrom (source as string vs string[])', () => {
    const attributes = [
      { name: 'time', type: 'TIME' },
      { name: 'page', type: 'STRING' },
      { name: 'added', type: 'NUMBER' },
    ];

    const noopRequester = () => {
      const stream = new PassThrough({ objectMode: true });
      setTimeout(() => stream.end(), 1);
      return stream;
    };

    it('emits FROM "table" AS t for a single source string', () => {
      const ext = External.fromJS({ engine: 'mysql', source: 'foo', attributes }, noopRequester);
      expect(ext.getFrom()).to.equal('FROM `foo` AS t');
    });

    it('emits UNION ALL subquery when source is an array of two', () => {
      const ext = External.fromJS(
        { engine: 'mysql', source: ['foo', 'bar'], attributes },
        noopRequester,
      );
      expect(ext.getFrom()).to.equal(
        'FROM (SELECT * FROM `foo` UNION ALL SELECT * FROM `bar`) AS t',
      );
    });

    it('escapes identifiers with backticks inside the union', () => {
      const ext = External.fromJS(
        { engine: 'mysql', source: ['weird`name', 'plain'], attributes },
        noopRequester,
      );
      expect(ext.getFrom()).to.equal(
        'FROM (SELECT * FROM `weird``name` UNION ALL SELECT * FROM `plain`) AS t',
      );
    });

    it('treats a single-element array uniformly as a subquery', () => {
      const ext = External.fromJS({ engine: 'mysql', source: ['only'], attributes }, noopRequester);
      expect(ext.getFrom()).to.equal('FROM (SELECT * FROM `only`) AS t');
    });

    it('throws a clear error on an empty source array', () => {
      const ext = External.fromJS({ engine: 'mysql', source: [], attributes }, noopRequester);
      expect(() => ext.getFrom()).to.throw('source array must not be empty');
    });

    it('withQuery takes precedence over an array source', () => {
      const ext = External.fromJS(
        {
          engine: 'mysql',
          source: ['foo', 'bar'],
          withQuery: 'SELECT 1',
          attributes,
        },
        noopRequester,
      );
      expect(ext.getFrom()).to.equal('FROM __with__ AS t');
    });
  });
});
