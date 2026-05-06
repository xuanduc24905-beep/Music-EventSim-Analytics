#!/usr/bin/env python3
import sys
import json

def mapper():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            # Chỉ đếm các sự kiện nghe nhạc (NextSong)
            if data.get("page") == "NextSong":
                artist = data.get("artist")
                if artist:
                    print(f"{artist}\t1")
        except Exception:
            continue

if __name__ == "__main__":
    mapper()
