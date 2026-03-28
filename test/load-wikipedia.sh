#!/bin/bash
set -euo pipefail

# Load the Wikipedia test dataset into Druid for functional tests.
# Expects Druid at localhost:8182 (or override with DRUID_HOST env var).
# Dataset: 39244 rows of Wikipedia edits from 2015-09-12.

DRUID_HOST="${DRUID_HOST:-localhost:8182}"
DATA_URL="https://raw.githubusercontent.com/YahooArchive/swiv/768ebff8fd23f08fa480c35af200e9096cc6f414/assets/data/wikiticker-2015-09-12-sampled.json"

echo "==> Submitting wikipedia ingestion to Druid at $DRUID_HOST..."

RESPONSE=$(curl -s "http://$DRUID_HOST/druid/v2/sql/task" \
  -H 'Content-Type: application/json' \
  -d @- <<'PAYLOAD'
{
  "query": "REPLACE INTO \"wikipedia\" OVERWRITE ALL\nSELECT\n  TIME_PARSE(\"time\") AS \"__time\",\n  \"channel\",\n  \"cityName\",\n  \"comment\",\n  CAST(LENGTH(\"comment\") AS BIGINT) AS \"commentLength\",\n  CAST(CAST(LENGTH(\"comment\") AS BIGINT) AS VARCHAR) AS \"commentLengthStr\",\n  \"countryIsoCode\",\n  \"countryName\",\n  \"deltaBucket100\",\n  CAST(\"delta\" / 10 AS BIGINT) AS \"deltaByTen\",\n  \"isAnonymous\",\n  \"isMinor\",\n  \"isNew\",\n  \"isRobot\",\n  \"isUnpatrolled\",\n  \"metroCode\",\n  \"namespace\",\n  \"page\",\n  \"regionIsoCode\",\n  \"regionName\",\n  \"user\",\n  MV_TO_ARRAY(\"userChars\") AS \"userChars\",\n  CAST(\"added\" AS BIGINT) AS \"added\",\n  CAST(\"deleted\" AS BIGINT) AS \"deleted\",\n  CAST(\"delta\" AS BIGINT) AS \"delta\",\n  CAST(\"min\" AS BIGINT) AS \"min\",\n  CAST(\"max\" AS BIGINT) AS \"max\",\n  CAST(1 AS BIGINT) AS \"count\",\n  TIME_SHIFT(TIME_PARSE(\"time\"), 'PT1H', 1) AS \"sometimeLater\"\nFROM TABLE(\n  EXTERN(\n    '{ \"type\": \"http\", \"uris\": [\"https://raw.githubusercontent.com/YahooArchive/swiv/768ebff8fd23f08fa480c35af200e9096cc6f414/assets/data/wikiticker-2015-09-12-sampled.json\"] }',\n    '{ \"type\": \"json\" }',\n    '[{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"countryName\",\"type\":\"string\"},{\"name\":\"deltaBucket100\",\"type\":\"long\"},{\"name\":\"isAnonymous\",\"type\":\"string\"},{\"name\":\"isMinor\",\"type\":\"string\"},{\"name\":\"isNew\",\"type\":\"string\"},{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"isUnpatrolled\",\"type\":\"string\"},{\"name\":\"metroCode\",\"type\":\"string\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"regionIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"userChars\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"min\",\"type\":\"long\"},{\"name\":\"max\",\"type\":\"long\"}]'\n  )\n)\nPARTITIONED BY DAY",
  "context": { "maxNumTasks": 3, "finalizeAggregations": false, "groupByEnableMultiValueUnnesting": false }
}
PAYLOAD
)

TASK_ID=$(echo "$RESPONSE" | jq -r '.taskId // .errorMessage // empty')

if [ -z "$TASK_ID" ]; then
  echo "ERROR: Failed to submit task. Response:"
  echo "$RESPONSE" | jq . 2>/dev/null || echo "$RESPONSE"
  exit 1
fi

echo "==> Task submitted: $TASK_ID"
echo "==> Polling for completion..."

while true; do
  STATUS=$(curl -s "http://$DRUID_HOST/druid/indexer/v1/task/$TASK_ID/status" | jq -r '.status.status')
  case "$STATUS" in
    SUCCESS)
      echo "==> Ingestion completed successfully!"
      break
      ;;
    FAILED)
      echo "ERROR: Ingestion failed."
      curl -s "http://$DRUID_HOST/druid/indexer/v1/task/$TASK_ID/status" | jq .
      exit 1
      ;;
    *)
      printf "    status: %s (waiting 5s...)\r" "$STATUS"
      sleep 5
      ;;
  esac
done

echo "==> Verifying row count..."
COUNT=$(curl -s "http://$DRUID_HOST/druid/v2/sql" \
  -H 'Content-Type: application/json' \
  -d '{"query": "SELECT COUNT(*) AS cnt FROM \"wikipedia\""}' | jq '.[0].cnt')

if [ "$COUNT" -eq 39244 ] 2>/dev/null; then
  echo "==> OK: wikipedia has $COUNT rows"
else
  echo "WARNING: Expected 39244 rows, got $COUNT"
  exit 1
fi
