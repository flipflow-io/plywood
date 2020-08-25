/*
 * Copyright 2020-2020 Imply Data, Inc.
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
const axios = require('axios');
let { sane } = require('../utils');

let { druidRequesterFactory } = require('plywood-druid-requester');

let plywood = require('../plywood');
let { External, TimeRange, $, ply, basicExecutorFactory } = plywood;

let info = require('../info');

describe('Cancel Functional', function() {
  it('works', async () => {
    let cancelFn;
    let cancelToken = new axios.CancelToken(cFn => {
      cancelFn = cFn;
    });
    cancelFn('Test cancel.');

    let druidRequester = druidRequesterFactory({
      host: info.druidHost,
      cancelToken,
    });

    let wikiExternal = External.fromJS(
      {
        engine: 'druid',
        source: 'wikipedia',
        timeAttribute: 'time',
        filter: $('time').overlap(
          TimeRange.fromJS({
            start: new Date('2015-09-12T00:00:00Z'),
            end: new Date('2015-09-13T00:00:00Z'),
          }),
        ),
      },
      druidRequester,
    );

    let basicExecutor = basicExecutorFactory({
      datasets: {
        wiki: wikiExternal,
      },
    });

    let ex = ply()
      .apply('wiki', $('wiki').filter($('channel').is('en')))
      .apply(
        'Cities',
        $('wiki')
          .split('$cityName', 'City')
          .apply('TotalAdded', '$wiki.sum($added)')
          .sort('$TotalAdded', 'descending')
          .limit(2),
      );

    try {
      await basicExecutor(ex);
    } catch (e) {
      expect(axios.isCancel(e));
      expect(e.message).to.equal('Test cancel.');
      return;
    }
    throw new Error('did not error');
  });
});
