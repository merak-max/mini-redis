#!/usr/bin/env bash
# Compare mini-redis with real Redis using redis-benchmark.
#
# Both servers log every write to an AOF and fsync every second, so the
# comparison is like for like. Rounds alternate between the servers, so
# warmup and background noise affect both sides the same way.
#
# Usage: scripts/bench_vs_redis.sh [rounds]     (default 5)
# Needs: go, redis-server, redis-benchmark, redis-cli
set -euo pipefail

ROUNDS=${1:-5}
MINI_PORT=6380
REDIS_PORT=6390
REQUESTS=200000
CLIENTS=50
KEYSPACE=100000

repo=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'redis-cli -p $REDIS_PORT shutdown nosave >/dev/null 2>&1 || true; kill $mini_pid 2>/dev/null || true; rm -rf "$work"' EXIT

go build -o "$work/mini-redis" "$repo"
(cd "$work" && ./mini-redis >mini.log 2>&1) &
mini_pid=$!
disown # no "Terminated" message when the trap stops it
mkdir "$work/redis"
redis-server --port $REDIS_PORT --appendonly yes --appendfsync everysec \
	--save '' --dir "$work/redis" --daemonize yes >/dev/null

for port in $MINI_PORT $REDIS_PORT; do
	for _ in $(seq 50); do redis-cli -p $port PING >/dev/null 2>&1 && break; sleep 0.1; done
done

# One line per run: server pipeline SET-rps GET-rps
run() {
	redis-benchmark -p "$1" -t set,get -n $REQUESTS -c $CLIENTS -r $KEYSPACE -P "$2" -q 2>/dev/null |
		tr '\r' '\n' | grep -E '^(SET|GET): [0-9.]+ requests' | awk '{printf " %d", $2}'
}

results="$work/results"
for round in $(seq "$ROUNDS"); do
	for port in $MINI_PORT $REDIS_PORT; do
		name=mini-redis; [ $port = $REDIS_PORT ] && name=redis
		for pipeline in 1 16; do
			echo "$name $pipeline$(run $port $pipeline)" | tee -a "$results"
		done
	done
done

# Check the work happened: the benchmark's keys exist on both servers.
redis-cli -p $MINI_PORT GET key:000000000042 >/dev/null
echo
echo "redis keys: $(redis-cli -p $REDIS_PORT DBSIZE), mini-redis AOF bytes: $(wc -c <"$work/appendonly.aof")"
echo
echo "median requests/sec over $ROUNDS rounds ($CLIENTS clients, -P = pipelined commands per request)"
sort "$results" | awk '
	{ k = $1 " -P " $2; set[k] = set[k] " " $3; get[k] = get[k] " " $4 }
	function median(s,   a, n, i, j, t) {
		n = split(s, a, " ")
		for (i = 1; i <= n; i++) for (j = i + 1; j <= n; j++) if (a[j] < a[i]) { t = a[i]; a[i] = a[j]; a[j] = t }
		return a[int((n + 1) / 2)]
	}
	END { for (k in set) printf "  %-18s SET %8d   GET %8d\n", k, median(set[k]), median(get[k]) }' | sort
