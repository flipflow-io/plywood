/*
 * Copyright 2015-2026 Imply Data, Inc.
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

/*
 * Tests for DruidSQLExternal.getFrom — the array-source override that emits
 * Druid-specific TABLE(APPEND(...)) instead of SELECT * UNION ALL.
 *
 * APPEND is schema-lenient (mirrors the native Druid `union` datasource used
 * by DruidExternal), so an array source can mix datasources with different
 * columns and missing fields become NULL.
 */

const { expect } = require('chai');
const { PassThrough } = require('readable-stream');

const plywood = require('../plywood');

const { External } = plywood;

const noopRequester = () => {
  const stream = new PassThrough({ objectMode: true });
  setTimeout(() => stream.end(), 1);
  return stream;
};

const attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'page', type: 'STRING' },
  { name: 'added', type: 'NUMBER' },
];

describe('DruidSQLExternal.getFrom', () => {
  it('emits a plain FROM clause for a single source string', () => {
    const ext = External.fromJS({ engine: 'druidsql', source: 'foo', attributes }, noopRequester);
    expect(ext.getFrom()).to.equal('FROM "foo" AS t');
  });

  it('emits TABLE(APPEND(...)) for an array source of two', () => {
    const ext = External.fromJS(
      {
        engine: 'druidsql',
        source: ['histories-online', 'histories-offline'],
        attributes,
      },
      noopRequester,
    );
    expect(ext.getFrom()).to.equal(
      "FROM TABLE(APPEND('histories-online', 'histories-offline')) AS t",
    );
  });

  it('escapes single quotes in datasource names inside APPEND', () => {
    const ext = External.fromJS(
      { engine: 'druidsql', source: ["weird'name", 'plain'], attributes },
      noopRequester,
    );
    expect(ext.getFrom()).to.equal("FROM TABLE(APPEND('weird''name', 'plain')) AS t");
  });

  it('treats a single-element array uniformly as TABLE(APPEND(...))', () => {
    const ext = External.fromJS(
      { engine: 'druidsql', source: ['only'], attributes },
      noopRequester,
    );
    expect(ext.getFrom()).to.equal("FROM TABLE(APPEND('only')) AS t");
  });

  it('throws a clear error on an empty source array', () => {
    const ext = External.fromJS({ engine: 'druidsql', source: [], attributes }, noopRequester);
    expect(() => ext.getFrom()).to.throw('source array must not be empty');
  });

  it('withQuery takes precedence over an array source', () => {
    const ext = External.fromJS(
      {
        engine: 'druidsql',
        source: ['histories-online', 'histories-offline'],
        withQuery: 'SELECT 1',
        attributes,
      },
      noopRequester,
    );
    expect(ext.getFrom()).to.equal('FROM __with__ AS t');
  });
});
