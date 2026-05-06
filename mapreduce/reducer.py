#!/usr/bin/env python3
import sys

def reducer():
    current_artist = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            artist, count = line.split("\t", 1)
            count = int(count)
        except ValueError:
            continue

        if current_artist == artist:
            current_count += count
        else:
            if current_artist:
                print(f"{current_artist}\t{current_count}")
            current_artist = artist
            current_count = count

    if current_artist == artist:
        print(f"{current_artist}\t{current_count}")

if __name__ == "__main__":
    reducer()
