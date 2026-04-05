#!/usr/bin/env python3
import sys


def iter_input_lines():
    for raw_line in sys.stdin.buffer:
        yield raw_line.decode("utf-8", errors="replace").rstrip("\r\n")


def emit(*values):
    line = "\t".join(str(value) for value in values)
    sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))


for line in iter_input_lines():
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4 or parts[0] != "POSTING":
        continue

    _, term, _doc_id, _tf = parts
    emit(f"VOCAB|{term}", 1)
