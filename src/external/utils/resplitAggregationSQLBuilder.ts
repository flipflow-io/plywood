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
import { Expression, ApplyExpression, SplitExpression } from '../../expressions';
import { SQLDialect } from '../../dialect/baseDialect';
import { detectInnerDerivation } from './innerDerivationDetector';
import { classifySplits, SplitClassificationResult, DivvyResult } from './splitClassifier';
import { buildLookupCTE, LookupCTEResult } from './lookupCTEBuilder';
import { qualifyColumns } from './columnQualifier';

export interface ResplitAggregationSQLResult {
  cteDefinitions: string[];
  selectExpressions: string[];
  fromClause: string;
  groupByExpressions: string[];
  outerAttributes: AttributeInfo[];
}

/**
 * Builds the complete SQL for resplit aggregation with support for
 * derivationScope: inner and additional splits lookup CTE.
 *
 * This function orchestrates the entire process:
 * 1. Detects if there's derivationScope: inner
 * 2. Classifies splits into measure internal vs additional
 * 3. Builds the inner CTE (cte_subsplit) with only measure internal splits
 * 4. Builds the lookup CTE (cte_split_lookup) for additional splits if needed
 * 5. Qualifies columns when there's a JOIN
 * 6. Adds columns for additional splits from lookup CTE
 *
 * @param globalResplitSplit - The split from the measure's internal formula
 * @param uiSplit - The split from the UI (may include additional dimensions)
 * @param innerApplies - Apply expressions for the inner CTE
 * @param outerApplies - Apply expressions for the outer query
 * @param fromClause - FROM clause function
 * @param whereClause - WHERE clause string
 * @param dialect - SQL dialect for generating SQL
 * @param divvyUpNestedSplitExpression - Function to split expressions into inner/outer
 * @param wrapCTEReferencesWithAnyValue - Function to wrap CTE references with ANY_VALUE
 * @returns Complete SQL structure for resplit aggregation
 */
export function buildResplitAggregationSQL(
  globalResplitSplit: SplitExpression,
  uiSplit: SplitExpression | null,
  innerApplies: ApplyExpression[],
  outerApplies: ApplyExpression[],
  getFrom: () => string,
  whereClause: string,
  dialect: SQLDialect,
  divvyUpNestedSplitExpression: (ex: Expression, intermediateName: string) => DivvyResult,
  wrapCTEReferencesWithAnyValue: (
    sql: string,
    cteColumnNames: string[],
    dialect: SQLDialect,
  ) => string,
): ResplitAggregationSQLResult {
  // Step 1: Detect inner derivation
  const hasInnerDerivation = detectInnerDerivation(innerApplies);

  // Step 2: Classify splits
  const outerAttributes: AttributeInfo[] = [];
  const classification = classifySplits(
    globalResplitSplit,
    uiSplit,
    hasInnerDerivation,
    divvyUpNestedSplitExpression,
    outerAttributes,
  );

  // Step 3: Build inner CTE (cte_subsplit)
  const innerSelectExpressions: string[] = [];
  const innerGroupByExpressions: string[] = [];

  for (const intermediateName in classification.innerSplits) {
    if (!classification.innerSplits.hasOwnProperty(intermediateName)) continue;
    const expression = classification.innerSplits[intermediateName];
    const sql = expression.getSQL(dialect);
    innerSelectExpressions.push(`${sql} AS ${dialect.escapeName(intermediateName)}`);
    innerGroupByExpressions.push(sql);
  }

  innerApplies.forEach(apply => {
    const sql = apply.getSQL(dialect);
    innerSelectExpressions.push(sql);
  });

  // Handle case when there are no GROUP BY expressions (no splits)
  const groupByClause =
    innerGroupByExpressions.length > 0
      ? `GROUP BY ${innerGroupByExpressions.join(', ')}`
      : '';

  const innerQueryParts = [
    'SELECT ' + innerSelectExpressions.join(',\n'),
    getFrom(),
    whereClause,
    groupByClause,
  ].filter(Boolean);

  const cteName = 'cte_subsplit';
  const innerQuery = innerQueryParts.join('\n');
  const cteDefinition = `${cteName} AS (\n${innerQuery}\n)`;

  // Step 4: Build lookup CTE if needed
  let lookupCTE: LookupCTEResult | null = null;
  const additionalSplitsKeys = Object.keys(classification.additionalSplits);

  if (
    hasInnerDerivation &&
    additionalSplitsKeys.length > 0 &&
    Object.keys(classification.measureInternalSplits).length > 0
  ) {
    lookupCTE = buildLookupCTE(
      classification.measureInternalSplits,
      classification.additionalSplits,
      getFrom(),
      whereClause,
      dialect,
    );
  }

  // Step 5: Build FROM clause with JOIN if needed
  let finalFromClause = `FROM ${cteName} AS t`;
  const cteDefinitions: string[] = [cteDefinition];

  if (lookupCTE && lookupCTE.joinConditions.length > 0) {
    finalFromClause += `\nLEFT JOIN ${lookupCTE.cteName} ON ${lookupCTE.joinConditions.join(' AND ')} AND ${lookupCTE.cteName}.rn = 1`;
    cteDefinitions.push(lookupCTE.cteDefinition);
  }

  // Step 6: Qualify columns if needed
  const needsQualification =
    hasInnerDerivation && additionalSplitsKeys.length > 0 && lookupCTE !== null;

  const qualified = qualifyColumns(
    classification.outerSplits,
    classification.outerSplitNameToIntermediateName,
    needsQualification,
    't',
    dialect,
  );

  const selectExpressions = [...qualified.selectExpressions];
  const groupByExpressions = [...qualified.groupByExpressions];

  // Step 7: Add columns for additional splits from lookup CTE
  if (lookupCTE && additionalSplitsKeys.length > 0) {
    // Map intermediate names to outer names
    for (const intermediateName in classification.additionalSplits) {
      if (!classification.additionalSplits.hasOwnProperty(intermediateName)) continue;

      // Find outer name for this intermediate name
      let outerName: string | null = null;
      for (const oname in classification.outerSplitNameToIntermediateName) {
        if (
          classification.outerSplitNameToIntermediateName[oname] === intermediateName
        ) {
          outerName = oname;
          break;
        }
      }

      if (outerName) {
        selectExpressions.push(
          `${lookupCTE.cteName}.${dialect.escapeName(intermediateName)} AS ${dialect.escapeName(outerName)}`,
        );
        groupByExpressions.push(
          `${lookupCTE.cteName}.${dialect.escapeName(intermediateName)}`,
        );
      }
    }
  }

  // Step 8: Add outer applies
  const cteColumnNames: string[] = [];
  for (const intermediateName in classification.innerSplits) {
    if (classification.innerSplits.hasOwnProperty(intermediateName)) {
      cteColumnNames.push(intermediateName);
    }
  }
  innerApplies.forEach(apply => {
    cteColumnNames.push(apply.name);
  });

  outerApplies.forEach(apply => {
    const sqlWithoutAlias = apply.getSQL(dialect).replace(/\s+AS\s+.*$/i, '');
    if (groupByExpressions.length > 0) {
      const processedSql = wrapCTEReferencesWithAnyValue(
        sqlWithoutAlias,
        cteColumnNames,
        dialect,
      );
      selectExpressions.push(`${processedSql} AS ${dialect.escapeName(apply.name)}`);
    } else {
      const processedSql = wrapCTEReferencesWithAnyValue(
        sqlWithoutAlias,
        cteColumnNames,
        dialect,
      );
      selectExpressions.push(`${processedSql} AS ${dialect.escapeName(apply.name)}`);
    }
  });

  return {
    cteDefinitions,
    selectExpressions,
    fromClause: finalFromClause,
    groupByExpressions,
    outerAttributes,
  };
}

