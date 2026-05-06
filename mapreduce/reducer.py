#!/usr/bin/env python3
"""
Universal word-count Reducer for Hadoop Streaming.

Expects sorted input (Hadoop guarantees this):
  key TAB count

Emits:
  key TAB total_count

Works for all 3 tasks (top_artists / plays_by_hour / user_sessions)
because every task reduces to simple SUM(count) GROUP BY key.

Example (local test):
  cat part-00000.json | TASK=top_artists python3 mapper.py | sort | python3 reducer.py
"""
import sys

cur_key   = None
cur_count = 0

for raw_line in sys.stdin:
    raw_line = raw_line.strip()
    if not raw_line:
        continue

    parts = raw_line.split("\t", 1)
    if len(parts) != 2:
        continue

    key, val = parts
    try:
        val = int(val)
    except ValueError:
        continue

    if key == cur_key:
        cur_count += val
    else:
        if cur_key is not None:
            sys.stdout.write(f"{cur_key}\t{cur_count}\n")
        cur_key   = key
        cur_count = val

# flush last key
if cur_key is not None:
    sys.stdout.write(f"{cur_key}\t{cur_count}\n")
