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

import { ApplyExpression } from '../../expressions';

/**
 * Detects if there's derivationScope: inner by looking for __result_main_* columns
 * in innerApplies. These columns indicate that arithmetic operations are being
 * calculated inside the split before final aggregation.
 *
 * When derivationScope is "inner", the measure formula includes operations like:
 *   (delta * current) or (current - previous) * price
 * These are calculated at the split level (e.g., per URL) before being summed.
 *
 * @param innerApplies - Array of ApplyExpression objects from the inner CTE
 * @returns true if derivationScope: inner is detected, false otherwise
 */
export function detectInnerDerivation(innerApplies: ApplyExpression[]): boolean {
  for (const apply of innerApplies) {
    const applyName = apply.name;
    if (applyName && /^__result_main_\d+$/.test(applyName)) {
      return true;
    }
  }
  return false;
}

