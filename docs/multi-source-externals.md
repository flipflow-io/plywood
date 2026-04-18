# Multi-Source Externals — Plywood Architectural Notes

## The invariant

**Every External owns its schema.** An External's `derivedAttributes` must
resolve against its own `rawAttributes`. Period. Plywood never merges schemas
across different externals.

All the multi-source machinery (linked sources, cross-source joins) respects
this invariant. If a change in plywood violates it, it will be rejected by
design.

## How plywood handles multiple externals in one expression

A typical linked-source query looks like:

```ts
ply()
  .apply("main", $main.filter(timeFilter))
  .apply("reviews", $reviews.filter(timeFilter))
  .apply("avgPrice",  "$main.average($price)")
  .apply("avgRating", "$reviews.average($rating)")
```

Two externals, `$main` and `$reviews`, live side by side. When evaluating the
outer `ply()` total row, plywood collects value-mode externals via
`Dataset.computeValueExternalAlterations` (see `src/datatypes/dataset.ts`).
Before the fix, a naive `uniteValueExternalsIntoTotal` call fused both into a
single external and ran one query whose SQL incorrectly referenced `main_ds`
for both averages.

### The fix (in `Dataset.ts`)

Group value-mode alterations by `external.equalBase()` BEFORE calling
`uniteValueExternalsIntoTotal`:

```ts
if (valueExternalAlterations.length === 1) {
  externalAlterations.push(valueExternalAlterations[0]);
} else {
  const groups: DatasetExternalAlteration[][] = [];
  for (const alt of valueExternalAlterations) {
    const group = groups.find(g => g[0].external.equalBase(alt.external));
    if (group) group.push(alt);
    else groups.push([alt]);
  }
  for (const group of groups) {
    if (group.length === 1) externalAlterations.push(group[0]);
    else externalAlterations.push({
      index: i, key: "",
      external: External.uniteValueExternalsIntoTotal(group),
    });
  }
}
```

`uniteValueExternalsIntoTotal` is now always called with a group of
externals sharing the SAME base (engine + source + version + rollup + mode).
Its internal `getMergedDerivedAttributesFromExternals` is schema-safe because
every external in the group shares `rawAttributes`.

As a consequence, `makeTotal` never receives a mixed-schema merge. No
try-catch in `typeCheckDerivedAttributes` is needed.

## `Dataset.join` auto-dispatch

`Dataset.join(other)` picks the right join variant based on the relation
between the two key sets:

| Relation                                 | Variant               |
| ---------------------------------------- | --------------------- |
| `other.keys ⊂ this.keys`                 | `broadcastJoin`       |
| `this.keys == other.keys`                | `leftJoin`            |
| overlapping, each side has exclusives    | `crossJoinOnSharedKeys` |
| no overlap                               | `leftJoin` (no-op)    |

Each variant lives as an explicit method on `Dataset`. Consumers just call
`.join()` and plywood picks. Tests in `test/datatypes/dataset.mocha.js` cover
each branch.

## What is not supported

- **Merging schemas across heterogeneous externals.** An external cannot
  reference another external's columns via `derivedAttributes`.
- **More than one linked source per query.** Plywood models a single "main" +
  N linked externals in the same ply, but the turnilo planner currently only
  routes queries with one linked source at a time.
