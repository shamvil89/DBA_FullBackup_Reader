#!/usr/bin/env python3
"""
Binary diff of two .bak files. Reports first byte offset where they differ
and hex context. Optional --data-start N to compare only from offset N onward.
"""

import argparse
import sys


def main():
    ap = argparse.ArgumentParser(description="Binary diff of two SQL Server backup files")
    ap.add_argument("file1", help="First .bak path")
    ap.add_argument("file2", help="Second .bak path")
    ap.add_argument("--data-start", type=int, default=None, metavar="N",
                    help="If set, compare only from byte offset N to end (data region)")
    ap.add_argument("--context", type=int, default=16, metavar="B",
                    help="Bytes of context around first difference (default 16)")
    args = ap.parse_args()

    with open(args.file1, "rb") as f1, open(args.file2, "rb") as f2:
        data1 = f1.read()
        data2 = f2.read()

    start = args.data_start if args.data_start is not None else 0
    if start > 0:
        if start >= len(data1) or start >= len(data2):
            print("--data-start beyond file length", file=sys.stderr)
            sys.exit(1)
        data1 = data1[start:]
        data2 = data2[start:]

    n = min(len(data1), len(data2))
    for i in range(n):
        if data1[i] != data2[i]:
            abs_offset = start + i
            ctx = args.context
            lo = max(0, i - ctx)
            hi = min(n, i + ctx + 1)
            ctx1 = data1[lo:hi]
            ctx2 = data2[lo:hi]
            print(f"First difference at byte offset (from start of file): {abs_offset}")
            print(f"Common prefix length (from start): {abs_offset}")
            print(f"File1 context [{abs_offset - (i - lo)}..{abs_offset + (hi - 1 - i)}]: {ctx1.hex()}")
            print(f"File2 context [{abs_offset - (i - lo)}..{abs_offset + (hi - 1 - i)}]: {ctx2.hex()}")
            if len(data1) != len(data2):
                print(f"File lengths: {len(data1) + start} vs {len(data2) + start}")
            return

    if len(data1) != len(data2):
        print(f"Files identical up to byte {start + n}, then length differs: {len(data1) + start} vs {len(data2) + start}")
    else:
        print(f"Files are identical (from offset {start}, length {len(data1)} bytes)")


if __name__ == "__main__":
    main()
