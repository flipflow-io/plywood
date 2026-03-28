# Plywood

Plywood is a JavaScript library that simplifies building interactive
visualizations and applications for large data sets. Plywood acts as a
middle-layer between data visualizations and data stores.

Plywood is architected around the principles of nested
[Split-Apply-Combine](http://www.jstatsoft.org/article/view/v040i01/v40i01.pdf),
a powerful divide-and-conquer algorithm that can be used to construct all types
of data visualizations. Plywood comes with its own [expression
language](docs/expressions.md) where a single Plywood expression can
translate to multiple database queries, and where results are returned in a
nested data structure so they can be easily consumed by visualization libraries
such as [D3.js](http://d3js.org/). 

You can use Plywood in the browser and/or in node.js to easily create your own
visualizations and applications.

Plywood also acts as a very advanced query planner for Druid, and Plywood will
determine the most optimal way to execute Druid queries.

## Installation

To use Plywood from npm simply run: `npm install plywood`.

Plywood can also be used by the browser.

## Documentation

To learn more, see [http://plywood.imply.io](http://plywood.imply.io/)

# Git Workflow for Flipflow Developers

When working with the Plywood library in Flipflow projects, please follow these simple guidelines to ensure smooth collaboration and keep our codebase synchronized with the upstream repository.

## 1. Clone the Flipflow Repository

Clone the Flipflow version of the Plywood repository:

```bash
git clone https://github.com/flipflow-io/plywood.git
cd plywood
```

## 2. Configure the Upstream Remote

Add the original Plywood repository as the `upstream` remote. This allows you to pull in updates from the original project:

```bash
git remote add upstream https://github.com/implydata/plywood.git
```

Verify your remotes:

```bash
git remote -v
```

You should see:

```
origin    https://github.com/flipflow-io/plywood.git (fetch)
origin    https://github.com/flipflow-io/plywood.git (push)
upstream  https://github.com/implydata/plywood.git (fetch)
upstream  https://github.com/implydata/plywood.git (push)
```

## 3. Make Changes on `master` or Create a Branch

You can work directly on the `master` branch or create a new branch for your changes:

### To work on `master`:

```bash
git checkout master
```

### To create and switch to a new branch:

```bash
git checkout -b my-feature-branch
```

## 4. Commit Your Changes

After making changes, stage and commit them:

```bash
git add .
git commit -m "Brief description of your changes"
```

## 5. Push Changes to the Flipflow Repository

Push your commits to the Flipflow repository on GitHub:

### If working on `master`:

```bash
git push origin master
```

### If working on a branch:

```bash
git push origin my-feature-branch
```

## 6. Update Your Branches with the Latest Changes

If you're working on a branch, update it with the latest changes from `master`:

```bash
git checkout my-feature-branch
git merge master
```

Resolve any conflicts as needed, then push the updated branch:

```bash
git push origin my-feature-branch
```

## 8. Best Practices

- **Regularly Update from Upstream**: Frequently merge changes from the original repository to minimize conflicts and stay up-to-date.
- **Use Descriptive Commit Messages**: Clearly explain what changes you've made and why.
- **Test Your Changes**: Ensure your modifications work as expected and don't break existing functionality.
- **Communicate with the Team**: Keep team members informed about significant changes or issues.

---

# Testing

Requires **Node 20** (pinned via Volta in `package.json`).

```bash
npm install --legacy-peer-deps
```

## Compile

Every test command runs `./compile` first, which does: ESLint → Prettier check → PEG.js → TypeScript → rollup bundle. You can run it standalone:

```bash
./compile
```

If lint/format fails:

```bash
npm run eslint-fix
npm run prettify
```

## Unit + Simulate Tests (no external deps)

```bash
npm test
```

This runs `./travis-test`: compile + mocha on `test/{datatypes,expression,external,helper,overall,parser,simulate}/*` against 3 builds (build, package, package.min) + lite builds.

To run a single test file fast (skips compile, uses existing build):

```bash
npx mocha test/simulate/simulateDruidSqlMode.mocha.js
```

## Functional Tests (require live databases)

```bash
npm run full-test
```

This runs `./run-tests`: compile + mocha on `test/*/*` (includes `test/functional/*`).

### Prerequisites

Functional tests connect to real databases. Config is in `test/info.js`:

| Database   | Host            | Required by                                |
|------------|-----------------|--------------------------------------------|
| Druid      | `localhost:8182` | `druidSqlFunctional`, `druidFunctional`    |
| MySQL      | `localhost:3306` | `mySqlFunctional`                          |
| PostgreSQL | `localhost:5432` | `postgresFunctional`                       |

On `flipflow-dev`, Druid is available via SSH tunnel:

```bash
# Check if tunnel is already running
ss -tlnp | grep 8182

# If not, start it
systemctl --user start tunnel@druid-router
```

### Load the Wikipedia test dataset into Druid

The functional tests expect a `wikipedia` datasource with 39244 rows (Wikipedia edits from 2015-09-12):

```bash
./test/load-wikipedia.sh
```

This script loads the data via Druid MSQ, polls until ingestion completes, and verifies the row count. See the script for the full ingestion SQL.

Verify manually if needed:

```bash
curl -s 'http://localhost:8182/druid/v2/sql' \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) AS cnt FROM \"wikipedia\""}' | jq
# Should return 39244
```

### Run only Druid SQL functional tests

```bash
npx mocha test/functional/druidSqlFunctional.mocha.js
```

### Run only simulate tests (no DB needed)

```bash
npx mocha test/simulate/*
```

### Run a single test by name

```bash
npx mocha test/functional/druidSqlFunctional.mocha.js --grep "MODE"
```

## Test structure

```
test/
├── datatypes/       # Dataset, Set, TimeRange, etc.
├── expression/      # Expression parsing, serialization, free references
├── external/        # External query planning
├── helper/          # Utility tests
├── overall/         # Compute engine (in-memory evaluation)
├── parser/          # PEG.js expression parser
├── simulate/        # SQL generation tests (no real DB)
│   ├── simulateDruidSql.mocha.js
│   ├── simulateDruidSqlMode.mocha.js
│   ├── simulateDruid.mocha.js (native Druid JSON queries)
│   └── simulateMySQL.mocha.js
├── functional/      # Real database execution tests
│   ├── druidSqlFunctional.mocha.js  (Druid SQL — main)
│   ├── druidFunctional.mocha.js     (Druid native)
│   ├── mySqlFunctional.mocha.js
│   ├── postgresFunctional.mocha.js
│   └── ...
├── info.js          # Database connection config
├── utils.js         # Test helpers (sane template tag)
└── plywood.js       # PLYWOOD_PATH loader (build/plywood by default)
```