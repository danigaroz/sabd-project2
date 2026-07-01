#!/usr/bin/env bash
#
# Run Query 1 with parallelism = {1, 2, 4} and record total time +
# throughput. Output goes to:
#   benchmarks/q1_benchmark_results.csv
# and each run's CSV under:
#   Results/q1_p<P>/...
#
# Expects:
#   * docker-compose cluster up (feeder + jobmanager + taskmanager)
#   * feeder restartable via docker-compose (it auto-loops on accept)
#
# Run from project root:  bash benchmarks/run_q1_benchmarks.sh
#
set -uo pipefail

cd "$(dirname "$0")/.."   # project root
COMPOSE="docker/docker-compose.yml"
OUT_CSV="benchmarks/q1_benchmark_results.csv"
mkdir -p benchmarks Results

echo "parallelism,total_time_sec,events_processed,throughput_ev_per_sec,output_lines" > "$OUT_CSV"

run_one () {
  local P="$1"
  echo ""
  echo "=========================================="
  echo "  Q1 benchmark: parallelism=$P"
  echo "=========================================="

  # Restart feeder so it serves from scratch with fresh elapsed counter.
  docker-compose -f "$COMPOSE" restart feeder >/dev/null 2>&1
  sleep 4

  # Clean previous q1 output
  docker exec -u root sabd2_jobmanager rm -rf /opt/flink/app/Results/q1 2>/dev/null || true
  mkdir -p Results/q1
  chmod 777 Results/q1

  # Wait for feeder ready
  for i in {1..30}; do
    if docker logs sabd2_feeder --tail 5 2>&1 | grep -q "listening on"; then
      break
    fi
    sleep 1
  done

  # Submit Q1
  SUBMIT_OUT=$(docker exec sabd2_jobmanager flink run -d \
      -p "$P" \
      -py /opt/flink/app/src/query1.py \
      -pyexec /opt/venv/bin/python \
      -pyclientexec /opt/venv/bin/python 2>&1)
  echo "$SUBMIT_OUT" | tail -3
  JOBID=$(echo "$SUBMIT_OUT" | grep -oE "JobID [a-f0-9]+" | awk '{print $2}')
  if [ -z "$JOBID" ]; then
    echo "  !! could not extract JobID, skipping this run"
    return
  fi
  echo "  JobID: $JOBID"

  T_START=$(date +%s)
  while true; do
    S=$(curl -s "http://localhost:8081/jobs/$JOBID" \
         | python3 -c "import sys,json; print(json.load(sys.stdin).get('state','?'))" 2>/dev/null)
    P_LINE=$(docker logs sabd2_feeder --tail 1 2>&1 | head -1)
    echo "  [$(date +%H:%M:%S)] state=$S | $P_LINE"
    if [ "$S" = "FINISHED" ] || [ "$S" = "FAILED" ] || [ "$S" = "CANCELED" ]; then
      break
    fi
    sleep 30
  done
  T_END=$(date +%s)
  WALL=$((T_END - T_START))

  # Extract events processed + elapsed reported by feeder (more accurate
  # than docker exec wall-clock).
  FEEDER_DONE=$(docker logs sabd2_feeder 2>&1 | grep "done: sent" | tail -1)
  EVENTS=$(echo "$FEEDER_DONE" | grep -oE "[0-9]+,[0-9]+,[0-9]+" | head -1 | tr -d ',')
  if [ -z "$EVENTS" ]; then
    EVENTS=$(echo "$FEEDER_DONE" | grep -oE "[0-9]+" | head -1)
  fi

  LAST_ELAPSED=$(docker logs sabd2_feeder 2>&1 | grep -oE "elapsed [0-9.]+s" | tail -1 | grep -oE "[0-9.]+")

  if [ -z "$EVENTS" ] || [ -z "$LAST_ELAPSED" ]; then
    EVENTS=0
    LAST_ELAPSED="$WALL"
  fi

  THROUGHPUT=$(python3 -c "print(f'{$EVENTS/$LAST_ELAPSED:.0f}')" 2>/dev/null || echo "?")
  OUT_LINES=$(find Results/q1 -name "q1-*.csv" ! -name "*.inprogress*" -exec wc -l {} + 2>/dev/null | tail -1 | awk '{print $1}')
  [ -z "$OUT_LINES" ] && OUT_LINES=0

  echo "  -> $S, wall=${WALL}s feeder_elapsed=${LAST_ELAPSED}s, events=$EVENTS, throughput=$THROUGHPUT ev/s, lines=$OUT_LINES"
  echo "$P,$LAST_ELAPSED,$EVENTS,$THROUGHPUT,$OUT_LINES" >> "$OUT_CSV"

  # Move Q1 output aside so next run starts clean.
  if [ -d Results/q1 ] && [ "$(ls -A Results/q1)" ]; then
    rm -rf "Results/q1_p${P}"
    mv Results/q1 "Results/q1_p${P}"
  fi
}

run_one 1
run_one 2
run_one 4

echo ""
echo "=========================================="
echo "  All benchmarks done. Results:"
echo "=========================================="
column -t -s, "$OUT_CSV"
