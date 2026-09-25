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

exports.druidVersion = process.env.DRUID_VERSION || '35.0.0';
// No default host: the Druid suites must never reach a production cluster by accident (on
// flipflow-dev, localhost:8182 is the SSH tunnel to the production router). Read lazily so the
// MySQL and PostgreSQL suites still load without it.
Object.defineProperty(exports, 'druidHost', {
  enumerable: true,
  get() {
    if (!process.env.DRUID_HOST) {
      throw new Error(
        'DRUID_HOST is not set: name the Druid to query (localhost:8182 is the production tunnel on flipflow-dev)',
      );
    }
    return process.env.DRUID_HOST;
  },
});
exports.druidContext = {
  timeout: 10000,
  useCache: false,
  populateCache: false,
};

exports.mySqlVersion = '5.7.41';
exports.mySqlHost = `localhost:3306`;
exports.mySqlDatabase = 'datazoo';
exports.mySqlUser = 'datazoo';
exports.mySqlPassword = 'datazoo';

exports.postgresVersion = '9.5.21';
exports.postgresHost = `localhost:5432`;
exports.postgresDatabase = 'datazoo';
exports.postgresUser = 'root';
exports.postgresPassword = 'datazoo';
