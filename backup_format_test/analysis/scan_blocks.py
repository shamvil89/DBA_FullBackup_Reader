#!/usr/bin/env python3
"""
Scan the data region of a .bak file for repeating block structure (e.g. fixed-size
headers before compressed blocks). Reports distances and sample bytes.
"""

import argparse
import sys


def main():
    ap = argparse.ArgumentParser(description="Scan .bak data region for repeated block structure")
    ap.add_argument("bak_file", help="Path to .bak file")
    ap.add_argument("--data-start", type=int, default=0, metavar="N",
                    help="Start of data region in bytes (default 0)")
    ap.add_argument("--header-size", type=int, default=20, metavar="B",
                    help="Candidate block header size to sample (default 20)")
    ap.add_argument("--max-samples", type=int, default=10, metavar="N",
                    help="Max block header samples to print (default 10)")
    args = ap.parse_args()

    with open(args.bak_file, "rb") as f:
        f.seek(args.data_start)
        data = f.read()

    if len(data) < args.header_size:
        print("Data region shorter than header-size", file=sys.stderr)
        sys.exit(1)

    # Look for repeated spacing: try common block sizes (e.g. 64K, 128K, variable)
    # Report first N bytes at offset 0 and at a few strides to see if structure repeats
    print(f"Data region: {len(data)} bytes (from file offset {args.data_start})")
    print(f"Sampling first {args.header_size} bytes at several offsets:")
    for i in range(args.max_samples):
        offset = i * (max(65536, len(data) // (args.max_samples or 1)))
        if offset + args.header_size > len(data):
            break
        chunk = data[offset : offset + args.header_size]
        print(f"  +{offset:8d} ({args.data_start + offset:8d} file): {chunk.hex()}")
    return


if __name__ == "__main__":
    main()
