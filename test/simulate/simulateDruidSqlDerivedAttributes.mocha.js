/*
 * Copyright 2012-2015 Metamarkets Group Inc.
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

const plywood = require('../plywood');

const { External, $, ply, r } = plywood;

const attributes = [
  { name: 'time', type: 'TIME' },
  { name: 'isOwnProduct', type: 'STRING' },
  { name: 'productId', type: 'STRING' },
];

const makeExternal = () =>
  External.fromJS({
    engine: 'druidsql',
    source: 'catalog',
    timeAttribute: 'time',
    attributes,
    derivedAttributes: {
      isOwnProducta: '$isOwnProduct',
    },
  });

describe('simulate Druid SQL with derivedAttributes', () => {
  it('inlines derivedAttribute inside a filter within an apply (aggregate)', () => {
    // $main.filter($isOwnProducta == '1').countDistinct($productId)
    const ex = $('catalog')
      .split('$time.timeBucket(P1D)', 'Day')
      .apply(
        'OwnCount',
        $('catalog')
          .filter($('isOwnProducta', 'STRING').is(r('1')))
          .countDistinct('$productId'),
      );

    const queryPlan = ex.simulateQueryPlan({ catalog: makeExternal() });
    const sql = queryPlan[0][0].query;

    // The derived attribute must be inlined to the real column so Druid can execute it
    expect(sql).to.include('"isOwnProduct"');
    expect(sql).to.not.include('"isOwnProducta"');
  });

  it('inlines derivedAttribute inside the aggregated ref itself', () => {
    // $main.countDistinct($isOwnProducta)
    const ex = $('catalog')
      .split('$time.timeBucket(P1D)', 'Day')
      .apply('UniqueOwn', $('catalog').countDistinct('$isOwnProducta'));

    const queryPlan = ex.simulateQueryPlan({ catalog: makeExternal() });
    const sql = queryPlan[0][0].query;

    expect(sql).to.include('"isOwnProduct"');
    expect(sql).to.not.include('"isOwnProducta"');
  });

  it('inlines derivedAttribute in a global WHERE filter', () => {
    const ex = ply()
      .apply('catalog', $('catalog').filter($('isOwnProducta', 'STRING').is(r('1'))))
      .apply('Total', '$catalog.count()');

    const queryPlan = ex.simulateQueryPlan({ catalog: makeExternal() });
    const sql = queryPlan[0][0].query;

    expect(sql).to.include('"isOwnProduct"');
    expect(sql).to.not.include('"isOwnProducta"');
  });
});
