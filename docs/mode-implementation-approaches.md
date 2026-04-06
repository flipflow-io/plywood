# MODE Expression — Druid SQL Implementation Approaches

## Context

Druid SQL has no native `MODE()` aggregate function. We need to implement the statistical mode (most frequent value) as a plywood expression that generates valid Druid SQL.

**Datasource tested**: `histories-826e27285307a923759de350de081d6218a04f4cff82b20c5ddaa8c60138c066`
**Date tested**: 2026-03-25 (434M total rows, ~2.8M for that day)

---

## Approach A: Scalar Subquery

### SQL Pattern

```sql
SELECT (
  SELECT "brand"
  FROM datasource
  WHERE <filters>
    AND "brand" IS NOT NULL AND "brand" <> ''
  GROUP BY "brand"
  ORDER BY COUNT(*) DESC
  LIMIT 1
) AS mode_brand
```

### Pros
- Very simple SQL generation
- Druid internally optimizes as TopN query (fast)
- Easy to implement in plywood — no CTE machinery needed

### Cons
- **Only works for global mode (no split)**
- Correlated subqueries (`WHERE t2."country" = t1."country"`) are NOT supported in Druid SQL
- Cannot be combined with splits — the main plywood use case

### Test Results
- **Global mode**: ✅ Returns `Isdinceutics` (correct)
- **Mode per split**: ❌ `UNSUPPORTED: Unhandled Query Planning Failure`

---

## Approach B: CTE + ROW_NUMBER + JOIN

### SQL Pattern

```sql
WITH field_counts AS (
  SELECT <split_dims>, "field", COUNT(*) AS cnt
  FROM datasource
  WHERE <filters>
    AND "field" IS NOT NULL AND "field" <> ''
  GROUP BY <split_dims>, "field"
),
ranked AS (
  SELECT <split_dims>, "field", cnt,
    ROW_NUMBER() OVER (PARTITION BY <split_dims> ORDER BY cnt DESC) AS rn
  FROM field_counts
),
mode_result AS (
  SELECT <split_dims>, "field" AS mode_field
  FROM ranked WHERE rn = 1
),
main_agg AS (
  -- Regular plywood query with splits + other aggregates
  SELECT <split_dims>, COUNT(*) AS ..., SUM(...) AS ...
  FROM datasource
  WHERE <filters>
  GROUP BY <split_dims>
)
SELECT m.*, r.mode_field
FROM main_agg m
LEFT JOIN mode_result r
  ON (m."dim1" IS NOT DISTINCT FROM r."dim1")
ORDER BY ...
LIMIT ...
```

### Pros
- **Works with splits** — the real plywood/Turnilo use case
- Works for global mode too (omit PARTITION BY)
- Composable: mode CTE can be JOINed with the main aggregation query
- `IS NOT DISTINCT FROM` handles NULL dimension values correctly
- Multiple mode columns can be added as additional CTEs + JOINs

### Cons
- Complex SQL generation — requires CTE injection into plywood's query builder
- JOIN adds overhead (though Druid handles broadcast joins well for grouped results)
- Need to replicate split dimensions and filters in the CTE
- Plywood's `sqlExternal.ts` already has CTE support (resplit/lookup), so this extends existing patterns

### Test Results
- **Mode per split (country)**: ✅ Returns correct mode_brand per country
- **Combined with other aggregates**: ✅ JOIN with main_agg works
- **NULL dimension handling**: ✅ `IS NOT DISTINCT FROM` resolves NULL join

---

## Recommendation

**Approach B (CTE + ROW_NUMBER + JOIN)** is the only viable option for production use.

Approach A is insufficient because plywood queries almost always involve splits. The scalar subquery pattern cannot express "mode of X within each group of Y".

### Implementation Strategy for Plywood

1. **DruidDialect.modeExpression()** — generates the CTE + window function SQL fragments
2. **sqlExternal.ts** — extend the existing CTE infrastructure (used by resplit/lookup) to inject mode CTEs and JOIN them with the main query
3. **DruidAggregationBuilder** — detect ModeExpression and route to CTE-based generation instead of inline aggregation
4. The mode CTE needs access to: split dimensions, filters, and the target field — all available in the expression context

### Key SQL Components Verified Against Druid
- `WITH ... AS (...)` — ✅ CTEs work
- `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` — ✅ Window functions work
- `IS NOT DISTINCT FROM` — ✅ NULL-safe equality works
- `LEFT JOIN` between CTEs — ✅ Works
- Multiple CTEs chained — ✅ Works
