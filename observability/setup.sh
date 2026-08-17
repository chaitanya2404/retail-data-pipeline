#!/usr/bin/env bash
#
# Provision the observability stack: index template, Kibana data view, and a smoke query.
#
# Written to be idempotent — re-running it updates rather than duplicates. Provisioning that can
# only be run once is provisioning nobody dares to run.
#
#   docker compose -f docker-compose.observability.yml up -d
#   ./observability/setup.sh

set -euo pipefail

ES_HOST="${ES_HOST:-http://localhost:9200}"
KIBANA_HOST="${KIBANA_HOST:-http://localhost:5601}"
INDEX_PATTERN="retail-pipeline-*"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { printf '  %s\n' "$*"; }

wait_for() {
  local name="$1" url="$2" attempts="${3:-30}"
  printf 'waiting for %s' "$name"
  for _ in $(seq 1 "$attempts"); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      printf ' ready\n'
      return 0
    fi
    printf '.'
    sleep 5
  done
  printf '\n'
  echo "ERROR: $name did not become ready at $url" >&2
  return 1
}

echo "== Elasticsearch =="
# wait_for_status=yellow, not green: a single node with no replicas is yellow by definition, so
# waiting for green would block forever on a perfectly healthy local cluster.
wait_for "elasticsearch" "${ES_HOST}/_cluster/health?wait_for_status=yellow&timeout=5s"

log "applying index template"
curl -fsS -X PUT "${ES_HOST}/_index_template/retail-pipeline" \
  -H 'Content-Type: application/json' \
  --data-binary "@${HERE}/elasticsearch/index-template.json" >/dev/null
log "index template applied to ${INDEX_PATTERN}"

echo "== Kibana =="
wait_for "kibana" "${KIBANA_HOST}/api/status" 60

# The data view is what makes the index searchable in Discover; without it the documents are in
# the cluster but invisible in the UI, which reads to a user as "the pipeline isn't logging".
# A fixed id keeps this idempotent — POSTing without one creates a duplicate view on every run.
log "creating data view"
http_code=$(curl -s -o /tmp/kibana-dataview.json -w '%{http_code}' \
  -X POST "${KIBANA_HOST}/api/data_views/data_view" \
  -H 'Content-Type: application/json' \
  -H 'kbn-xsrf: true' \
  -d "{
        \"data_view\": {
          \"id\": \"retail-pipeline\",
          \"title\": \"${INDEX_PATTERN}\",
          \"name\": \"Retail pipeline events\",
          \"timeFieldName\": \"@timestamp\"
        },
        \"override\": true
      }")

if [[ "$http_code" == "200" ]]; then
  log "data view 'Retail pipeline events' ready"
else
  log "data view request returned HTTP ${http_code}:"
  head -c 300 /tmp/kibana-dataview.json >&2 || true
  echo >&2
fi

echo "== Smoke check =="
count=$(curl -fsS "${ES_HOST}/${INDEX_PATTERN}/_count" | sed -n 's/.*"count":\([0-9]*\).*/\1/p')
log "documents indexed: ${count:-0}"

if [[ "${count:-0}" == "0" ]]; then
  cat <<'EOF'

  No documents yet. That is expected on a first run — Logstash ships events only after the
  pipeline has produced some. Generate a batch with:

      .venv-data/bin/python -c "import sys; sys.path.insert(0,'.'); from src.etl.pipeline import run_pipeline; run_pipeline()"

EOF
fi

echo
echo "Kibana: ${KIBANA_HOST}/app/discover  (data view: Retail pipeline events)"
