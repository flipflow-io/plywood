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

import { AttributeInfo } from '../../datatypes/attributeInfo';
import { Expression, SplitExpression } from '../../expressions';

export interface DivvyResult {
  inner: Expression;
  outer: Expression;
}

export interface SplitClassificationResult {
  measureInternalSplits: Record<string, Expression>;
  additionalSplits: Record<string, Expression>;
  outerSplitNameToIntermediateName: Record<string, string>;
  innerSplits: Record<string, Expression>;
  outerSplits: Record<string, Expression>;
  outerAttributes: AttributeInfo[];
}

/**
 * Classifies splits into measure internal splits (from globalResplitSplit)
 * and additional splits (from UI that are not in globalResplitSplit).
 *
 * When derivationScope is "inner", we need to separate:
 * - Measure internal splits: These come from the measure's internal split
 *   (e.g., $url in estimated_revenue formula)
 * - Additional splits: These are added by the UI (e.g., seller, brand)
 *   and should NOT be included in the inner CTE GROUP BY
 *
 * @param globalResplitSplit - The split from the measure's internal formula
 * @param uiSplit - The split from the UI (may include additional dimensions)
 * @param hasInnerDerivation - Whether derivationScope: inner is detected
 * @param divvyUpNestedSplitExpression - Function to split expressions into inner/outer
 * @param outerAttributes - Array to populate with attribute info
 * @returns Classification result with separated splits and mappings
 */
export function classifySplits(
  globalResplitSplit: SplitExpression,
  uiSplit: SplitExpression | null,
  hasInnerDerivation: boolean,
  divvyUpNestedSplitExpression: (ex: Expression, intermediateName: string) => DivvyResult,
  outerAttributes: AttributeInfo[],
): SplitClassificationResult {
  const measureInternalSplits: Record<string, boolean> = {};
  const outerSplitNameToIntermediateName: Record<string, string> = {};
  const innerSplits: Record<string, Expression> = {};
  const outerSplits: Record<string, Expression> = {};
  let splitCount = 0;

  // Process splits from globalResplitSplit (ALWAYS measure internal)
  // These come from the measure's internal split (e.g., $url in the formula)
  globalResplitSplit.mapSplits((name, ex) => {
    let outerSplitName: string | null = null;

    // Check if this split is also in the UI split
    if (uiSplit) {
      uiSplit.mapSplits((sname, sex) => {
        if (ex.equals(sex)) {
          outerSplitName = sname;
        }
      });
    }

    const intermediateName = `s${splitCount++}`;
    const divvy = divvyUpNestedSplitExpression(ex, intermediateName);

    outerAttributes.push(
      AttributeInfo.fromJS({ name: intermediateName, type: divvy.inner.type }),
    );

    innerSplits[intermediateName] = divvy.inner;

    // ALL splits from globalResplitSplit are measure internal
    // (they come from the measure's internal split)
    measureInternalSplits[intermediateName] = true;

    if (outerSplitName) {
      outerSplits[outerSplitName] = divvy.outer;
      outerSplitNameToIntermediateName[outerSplitName] = intermediateName;
    }
  });

  // Process additional splits from UI that are NOT in globalResplitSplit
  // These are splits added by the UI that are not part of the measure's internal split
  if (uiSplit) {
    uiSplit.mapSplits((name, ex) => {
      // Skip if already processed (from globalResplitSplit)
      if (outerSplits[name]) return;

      const intermediateName = `s${splitCount++}`;
      const divvy = divvyUpNestedSplitExpression(ex, intermediateName);

      innerSplits[intermediateName] = divvy.inner;
      outerAttributes.push(
        AttributeInfo.fromJS({ name: intermediateName, type: divvy.inner.type }),
      );

      outerSplits[name] = divvy.outer;
      outerSplitNameToIntermediateName[name] = intermediateName;

      // These are NOT marked as measureInternalSplits - they are additional splits
      // (only relevant when hasInnerDerivation is true)
    });
  }

  // Separate additional splits from measure internal splits
  // This is only needed when hasInnerDerivation is true
  const finalAdditionalSplits: Record<string, Expression> = {};
  const finalInnerSplits: Record<string, Expression> = {};
  const finalMeasureInternalSplits: Record<string, Expression> = {};

  // Build mapping from intermediate name to outer name for additional splits
  const intermediateNameToOuterName: Record<string, string> = {};
  if (hasInnerDerivation && uiSplit) {
    uiSplit.mapSplits((sname, sex) => {
      // Find the intermediate name for this split
      for (const checkIntermediateName in innerSplits) {
        if (!innerSplits.hasOwnProperty(checkIntermediateName)) continue;

        const checkDivvy = divvyUpNestedSplitExpression(sex, checkIntermediateName);
        if (checkDivvy.inner.equals(innerSplits[checkIntermediateName])) {
          intermediateNameToOuterName[checkIntermediateName] = sname;
          break;
        }
      }
    });
  }

  // Separate splits: measure internal vs additional
  for (const intermediateName in innerSplits) {
    if (!innerSplits.hasOwnProperty(intermediateName)) continue;

    if (hasInnerDerivation && !measureInternalSplits[intermediateName]) {
      // This is an additional split - it should NOT go to inner CTE GROUP BY
      finalAdditionalSplits[intermediateName] = innerSplits[intermediateName];

      // Remove from outerSplits so we handle it separately via lookup CTE
      if (intermediateNameToOuterName[intermediateName]) {
        delete outerSplits[intermediateNameToOuterName[intermediateName]];
      }
    } else {
      // This is a measure internal split - it goes to inner CTE GROUP BY
      finalInnerSplits[intermediateName] = innerSplits[intermediateName];
      finalMeasureInternalSplits[intermediateName] = innerSplits[intermediateName];
    }
  }

  return {
    measureInternalSplits: finalMeasureInternalSplits,
    additionalSplits: finalAdditionalSplits,
    outerSplitNameToIntermediateName,
    innerSplits: finalInnerSplits,
    outerSplits,
    outerAttributes,
  };
}

