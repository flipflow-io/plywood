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

import { Readable } from 'readable-stream';

import { Datum } from '../datatypes';
import { ComputeOptions, Expression } from '../expressions/baseExpression';

export type Executor = (ex: Expression, opt?: ComputeOptions) => any;

export interface BasicExecutorParameters {
  datasets: Datum;
}

export function basicExecutorFactory(parameters: BasicExecutorParameters): Executor {
  const datasets = parameters.datasets;
  return (ex: Expression, opt: ComputeOptions = {}) => {
    if (opt.stream) {
      if (typeof ex.computeStream === 'function') {
        // If ex has a computeStream method, use it
        return ex.computeStream(datasets, opt);
      } else {
        // Otherwise, create a Readable stream and push the result manually
        const stream = new Readable({
          objectMode: true,
          read() {}, // This will be called when the consumer wants more data
        });
        ex.compute(datasets, opt)
          .then(result => {
            stream.push(result);
            stream.push(null); // Signal the end of the stream
          })
          .catch(err => {
            stream.emit('error', err);
          });
        return stream;
      }
    } else {
      // Otherwise, use regular compute
      return ex.compute(datasets, opt);
    }
  };
}
