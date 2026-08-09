#!/usr/bin/env bash
# Dump the local control-plane + cache state for one running install, so a
# "why is so little cached?" question can be answered with data.
#
# Run on the HOST (macOS/Linux), not in the sandbox — the app's data dir is
# outside the repo. Reads only; safe while the app is running (WAL).
#
#   ./scripts/cache-doctor.sh            # dev build (tauri dev)
#   ./scripts/cache-doctor.sh Kstack     # installed release build
set -uo pipefail

APP_DIR_NAME="${1:-Kstack-dev}"

case "$(uname -s)" in
  Darwin) BASE="$HOME/Library/Application Support" ;;
  *)      BASE="${XDG_DATA_HOME:-$HOME/.local/share}" ;;
esac
DATA_DIR="$BASE/$APP_DIR_NAME"

if [ ! -d "$DATA_DIR" ]; then
  echo "no data dir at $DATA_DIR" >&2
  echo "(pass the other name: Kstack-dev for 'tauri dev', Kstack for an installed build)" >&2
  exit 1
fi
command -v sqlite3 >/dev/null || { echo "sqlite3 not found" >&2; exit 1; }

# Read-only URI so nothing here can disturb a running app.
q() { sqlite3 "file:$1?mode=ro" "$2"; }

BEEHIVE="$DATA_DIR/beehive.db"
echo "=== data dir: $DATA_DIR"
echo
echo "=== control plane objects, by kind"
q "$BEEHIVE" "SELECT kind, COUNT(*) FROM objects GROUP BY kind ORDER BY 2 DESC;"

echo
echo "=== ClusterCacheGVRSync — Synced condition, by (status, reason)"
q "$BEEHIVE" "
  SELECT c.status, COALESCE(c.reason,'(none)'), COUNT(*)
  FROM objects o LEFT JOIN conditions c
    ON c.object_id = o.id AND c.type = 'Synced'
  WHERE o.kind = 'ClusterCacheGVRSync'
  GROUP BY 1, 2 ORDER BY 3 DESC;"

echo
echo "=== ClusterCacheGVRSync — up to 15 that are NOT Synced=True"
q "$BEEHIVE" "
  SELECT o.name, COALESCE(c.status,'(no condition)'), COALESCE(c.reason,''), substr(COALESCE(c.message,''),1,70)
  FROM objects o LEFT JOIN conditions c
    ON c.object_id = o.id AND c.type = 'Synced'
  WHERE o.kind = 'ClusterCacheGVRSync' AND (c.status IS NULL OR c.status <> 'True')
  LIMIT 15;"

echo
echo "=== ClusterCacheGVRDiscovery — Discovered condition"
q "$BEEHIVE" "
  SELECT o.name, COALESCE(c.status,'(none)'), COALESCE(c.reason,''), substr(COALESCE(c.message,''),1,70)
  FROM objects o LEFT JOIN conditions c
    ON c.object_id = o.id AND c.type = 'Discovered'
  WHERE o.kind = 'ClusterCacheGVRDiscovery';"

for db in "$DATA_DIR"/clusters/*/*.db; do
  [ -e "$db" ] || continue
  echo
  echo "=== cache $db"
  echo "-- kinds registered in the catalog (one per running sync):"
  q "$db" "SELECT COUNT(*) FROM kind_catalog;"
  echo "-- kinds with objects / total objects:"
  q "$db" "SELECT COUNT(*), COALESCE(SUM(count),0) FROM kind_counts WHERE count > 0;"
  echo "-- catalog entries with NO objects (top 25) — a sync that ran but wrote nothing:"
  q "$db" "
    SELECT kc.api_version, kc.kind
    FROM kind_catalog kc LEFT JOIN kind_counts n
      ON n.api_version = kc.api_version AND n.kind = kc.kind
    WHERE COALESCE(n.count, 0) = 0
    ORDER BY 1, 2 LIMIT 25;"
  echo "-- per-kind counts (top 20):"
  q "$db" "SELECT api_version, kind, count FROM kind_counts WHERE count > 0 ORDER BY count DESC LIMIT 20;"
  echo "-- resume cookies written (one per kind that completed a LIST):"
  q "$db" "SELECT COUNT(*) FROM cluster_meta WHERE key LIKE '%.last_list_rv';"
done
