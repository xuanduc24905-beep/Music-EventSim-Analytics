#!/usr/bin/env python3
"""
MapReduce Benchmark — Python simulation with real disk I/O between phases.

Models Hadoop MapReduce exactly:
  Map   → writes intermediate key-value pairs to disk
  Sort  → sorts map output by key (Hadoop shuffle/sort)
  Reduce → reads sorted file, aggregates by key, writes to disk

Run inside spark-master container:
  python3 /tmp/run_mapreduce.py

Results written to: /tmp/mr_results.json
"""
import json
import os
import sys
import time
import tempfile

import requests

WEBHDFS     = "http://namenode:9870/webhdfs/v1"
HDFS_DIR    = "/music/benchmark/data.json"
RESULT_FILE = "/tmp/mr_results.json"


# ── HDFS Download ─────────────────────────────────────────────────

def download_benchmark_data(local_dir: str):
    """Download all JSON part-files from HDFS benchmark directory."""
    os.makedirs(local_dir, exist_ok=True)

    resp = requests.get(f"{WEBHDFS}{HDFS_DIR}?op=LISTSTATUS", timeout=10)
    resp.raise_for_status()
    entries = resp.json()["FileStatuses"]["FileStatus"]

    part_files = [
        e for e in entries
        if not e["pathSuffix"].startswith("_") and e["length"] > 0
    ]

    local_files = []
    total_records = 0

    for entry in part_files:
        fname  = entry["pathSuffix"]
        remote = f"{HDFS_DIR}/{fname}"
        local  = os.path.join(local_dir, fname)

        r = requests.get(
            f"{WEBHDFS}{remote}?op=OPEN",
            allow_redirects=True,
            timeout=60,
        )
        r.raise_for_status()
        with open(local, "wb") as f:
            f.write(r.content)

        with open(local) as f:
            count = sum(1 for line in f if line.strip())
        total_records += count
        local_files.append(local)
        print(f"  ↓ {fname}: {count:,} records  ({entry['length']:,} bytes)")

    return local_files, total_records


# ── Mapper functions ──────────────────────────────────────────────

def map_artist(record: dict):
    artist = record.get("artist")
    return (artist, "1") if artist else None


def map_hour(record: dict):
    ts = record.get("ts")
    if ts:
        hour = (int(ts) // 3_600_000) % 24  # epoch ms → UTC hour
        return (str(hour).zfill(2), "1")
    return None


def map_user(record: dict):
    uid = record.get("userId")
    return (str(uid), "1") if uid is not None else None


# ── Full MapReduce pipeline ───────────────────────────────────────

def run_task(name: str, local_files, mapper_fn, tmp_dir: str) -> dict:
    """
    Complete MapReduce cycle with disk writes between each phase:
      Map phase   → map_output.tsv    (disk)
      Shuffle     → sorted_output.tsv (disk, GNU sort)
      Reduce phase → result.tsv       (disk)
    """
    map_out    = os.path.join(tmp_dir, f"{name}_map.tsv")
    sorted_out = os.path.join(tmp_dir, f"{name}_sorted.tsv")
    reduce_out = os.path.join(tmp_dir, f"{name}_reduce.tsv")

    t_total = time.perf_counter()

    # ── MAP ──────────────────────────────────────────────────────
    t = time.perf_counter()
    with open(map_out, "w") as fout:
        for path in local_files:
            with open(path) as fin:
                for line in fin:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        kv = mapper_fn(json.loads(line))
                        if kv:
                            fout.write(f"{kv[0]}\t{kv[1]}\n")
                    except Exception:
                        pass
    map_sec = time.perf_counter() - t

    # ── SHUFFLE / SORT (write sorted data to disk) ────────────────
    t = time.perf_counter()
    os.system(f"sort '{map_out}' > '{sorted_out}'")
    shuffle_sec = time.perf_counter() - t

    # ── REDUCE ───────────────────────────────────────────────────
    t = time.perf_counter()
    cur_key, cur_cnt = None, 0
    with open(sorted_out) as fin, open(reduce_out, "w") as fout:
        for line in fin:
            parts = line.rstrip("\n").split("\t", 1)
            if len(parts) != 2:
                continue
            key, val = parts
            try:
                val = int(val)
            except ValueError:
                continue
            if key == cur_key:
                cur_cnt += val
            else:
                if cur_key is not None:
                    fout.write(f"{cur_key}\t{cur_cnt}\n")
                cur_key, cur_cnt = key, val
        if cur_key is not None:
            fout.write(f"{cur_key}\t{cur_cnt}\n")
    reduce_sec = time.perf_counter() - t

    total_sec = time.perf_counter() - t_total

    return {
        "task":        name,
        "total_sec":   round(total_sec,   3),
        "map_sec":     round(map_sec,     3),
        "shuffle_sec": round(shuffle_sec, 3),
        "reduce_sec":  round(reduce_sec,  3),
    }


# ── Main ──────────────────────────────────────────────────────────

def main():
    print("=" * 64)
    print(" MapReduce Benchmark — Python simulation + real disk I/O")
    print(" Pipeline: Map → Shuffle/Sort (disk) → Reduce (disk)")
    print("=" * 64)

    tmp_dir  = tempfile.mkdtemp(prefix="mr_bench_")
    data_dir = os.path.join(tmp_dir, "input")

    print("\n[DOWNLOAD] Fetching benchmark data from HDFS ...")
    t0 = time.perf_counter()
    try:
        local_files, total_records = download_benchmark_data(data_dir)
    except Exception as e:
        print(f"[ERROR] Cannot reach HDFS/WebHDFS: {e}")
        sys.exit(1)
    dl_sec = round(time.perf_counter() - t0, 3)
    print(f"  Total: {total_records:,} records  |  Download: {dl_sec}s")

    TASKS = [
        ("top_artists",   map_artist),
        ("plays_by_hour", map_hour),
        ("user_sessions", map_user),
    ]

    results = []
    for i, (name, fn) in enumerate(TASKS, 1):
        print(f"\n[MR {i}/3] {name} ...")
        t = run_task(name, local_files, fn, tmp_dir)
        t["num_records"] = total_records
        print(
            f"  Map={t['map_sec']}s  "
            f"Shuffle={t['shuffle_sec']}s  "
            f"Reduce={t['reduce_sec']}s  "
            f"→ Total={t['total_sec']}s"
        )
        results.append(t)

    output = {
        "total_records": total_records,
        "dl_sec":        dl_sec,
        "tasks":         results,
    }
    with open(RESULT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\n✓ Results saved → {RESULT_FILE}")
    print("=" * 64)


if __name__ == "__main__":
    main()
