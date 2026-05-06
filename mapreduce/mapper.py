#!/usr/bin/env python3
"""
Universal Mapper for Hadoop Streaming.

Reads TASK env var to choose which key to emit:
  top_artists   → artist name
  plays_by_hour → hour of day (00–23)
  user_sessions → userId

Stdin : one JSON record per line (already-clean NextSong events)
Stdout: key TAB 1

Example (Hadoop Streaming):
  export TASK=top_artists
  hadoop jar $STREAMING_JAR \\
      -cmdenv TASK=$TASK \\
      -files mapper.py,reducer.py \\
      -mapper  "python3 mapper.py" \\
      -reducer "python3 reducer.py" \\
      -input  /music/benchmark/data.json \\
      -output /music/benchmark/mr_top_artists

Example (local test):
  cat part-00000.json | TASK=plays_by_hour python3 mapper.py | sort | python3 reducer.py
"""
import json
import os
import sys

TASK = os.environ.get("TASK", "top_artists")


def extract_key(record: dict):
    if TASK == "top_artists":
        return record.get("artist") or None

    if TASK == "plays_by_hour":
        ts = record.get("ts")
        if ts:
            hour = (int(ts) // 3_600_000) % 24  # epoch ms → UTC hour
            return str(hour).zfill(2)
        return None

    if TASK == "user_sessions":
        uid = record.get("userId")
        return str(uid) if uid is not None else None

    return None


for raw_line in sys.stdin:
    raw_line = raw_line.strip()
    if not raw_line:
        continue
    try:
        rec = json.loads(raw_line)
        key = extract_key(rec)
        if key:
            sys.stdout.write(f"{key}\t1\n")
    except Exception:
        pass
