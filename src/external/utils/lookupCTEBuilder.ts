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

import { Expression } from '../../expressions';
import { SQLDialect } from '../../dialect/baseDialect';

export interface LookupCTEResult {
  cteDefinition: string | null;
  cteName: string;
  joinConditions: string[];
  additionalOuterSplits: Record<string, string>; // outerName -> intermediateName
}

/**
 * Builds a lookup CTE for additional splits when there's derivationScope: inner.
 *
 * The lookup CTE uses a subquery approach to handle Druid SQL limitations:
 * - Inner query: GROUP BY with MAX(__time) to get the most recent value
 * - Outer query: ROW_NUMBER() to select only the most recent record per partition
 *
 * This allows us to get the correct value for additional splits (like seller)
 * without including them in the inner CTE GROUP BY, which would cause data loss.
 *
 * @param measureInternalSplits - Splits that are part of the measure's internal split
 * @param additionalSplits - Splits added by UI that are not in measure internal
 * @param fromClause - FROM clause for the lookup query
 * @param whereClause - WHERE clause for the lookup query
 * @param dialect - SQL dialect for generating SQL
 * @returns Lookup CTE result or null if not needed
 */
export function buildLookupCTE(
  measureInternalSplits: Record<string, Expression>,
  additionalSplits: Record<string, Expression>,
  fromClause: string,
  whereClause: string,
  dialect: SQLDialect,
): LookupCTEResult | null {
  const additionalSplitsKeys = Object.keys(additionalSplits);
  if (additionalSplitsKeys.length === 0) {
    return null;
  }

  const measureInternalSplitsKeys = Object.keys(measureInternalSplits);
  if (measureInternalSplitsKeys.length === 0) {
    return null;
  }

  const lookupCTEName = 'cte_split_lookup';
  const lookupSelectExpressions: string[] = [];
  const lookupGroupByExpressions: string[] = [];
  const partitionByExpressions: string[] = [];
  const additionalOuterSplits: Record<string, string> = {};

  // Include measure internal splits for JOIN
  for (const intermediateName in measureInternalSplits) {
    if (!measureInternalSplits.hasOwnProperty(intermediateName)) continue;
    const expression = measureInternalSplits[intermediateName];
    const sql = expression.getSQL(dialect);
    lookupSelectExpressions.push(`${sql} AS ${dialect.escapeName(intermediateName)}`);
    lookupGroupByExpressions.push(sql);
    partitionByExpressions.push(sql);
  }

  // Include additional splits
  for (const intermediateName in additionalSplits) {
    if (!additionalSplits.hasOwnProperty(intermediateName)) continue;
    const expression = additionalSplits[intermediateName];
    const sql = expression.getSQL(dialect);
    lookupSelectExpressions.push(`${sql} AS ${dialect.escapeName(intermediateName)}`);
    lookupGroupByExpressions.push(sql);
    // Store mapping (we'll need to map this back to outer names later)
    additionalOuterSplits[intermediateName] = intermediateName; // TODO: Map to outer name
  }

  // Validate we have expressions
  if (lookupGroupByExpressions.length === 0 || partitionByExpressions.length === 0) {
    return null;
  }

  // Inner query: GROUP BY with MAX(__time)
  // This allows us to use MAX(__time) in the ORDER BY of ROW_NUMBER()
  lookupSelectExpressions.push('MAX("__time") AS "__time_max"');

  const innerLookupQueryParts = [
    'SELECT',
    lookupSelectExpressions.join(',\n'),
    fromClause,
    whereClause,
    'GROUP BY ' + lookupGroupByExpressions.join(', '),
  ].filter(Boolean);

  const innerLookupQuery = innerLookupQueryParts.join('\n');

  // Outer query: Apply ROW_NUMBER() using __time_max from inner query
  // Use a non-reserved word alias instead of "inner" (which is a SQL keyword)
  const lookupInnerAlias = 'lookup_inner';
  const outerLookupSelectExpressions: string[] = [];
  const outerPartitionByExpressions: string[] = [];

  // Add all columns from inner query
  for (const intermediateName in measureInternalSplits) {
    if (!measureInternalSplits.hasOwnProperty(intermediateName)) continue;
    outerLookupSelectExpressions.push(
      `${lookupInnerAlias}.${dialect.escapeName(intermediateName)}`,
    );
    outerPartitionByExpressions.push(
      `${lookupInnerAlias}.${dialect.escapeName(intermediateName)}`,
    );
  }

  for (const intermediateName in additionalSplits) {
    if (!additionalSplits.hasOwnProperty(intermediateName)) continue;
    outerLookupSelectExpressions.push(
      `${lookupInnerAlias}.${dialect.escapeName(intermediateName)}`,
    );
  }

  // Validate partition expressions
  if (outerPartitionByExpressions.length === 0) {
    return null;
  }

  const outerLookupQueryParts = [
    'SELECT',
    outerLookupSelectExpressions.join(',\n'),
    `, ROW_NUMBER() OVER (PARTITION BY ${outerPartitionByExpressions.join(', ')} ORDER BY ${lookupInnerAlias}."__time_max" DESC) AS rn`,
    'FROM (',
    innerLookupQuery,
    `) AS ${lookupInnerAlias}`,
  ].filter(Boolean);

  const lookupCTEQuery = outerLookupQueryParts.join('\n');
  const cteDefinition = `${lookupCTEName} AS (\n${lookupCTEQuery}\n)`;

  // Build join conditions based on measure internal splits
  const joinConditions: string[] = [];
  for (const intermediateName in measureInternalSplits) {
    if (!measureInternalSplits.hasOwnProperty(intermediateName)) continue;
    joinConditions.push(
      `t.${dialect.escapeName(intermediateName)} = ${lookupCTEName}.${dialect.escapeName(intermediateName)}`,
    );
  }

  return {
    cteDefinition,
    cteName: lookupCTEName,
    joinConditions,
    additionalOuterSplits,
  };
}

