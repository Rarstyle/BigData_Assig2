#!/usr/bin/env python3
import sys


def iter_input_lines():
    for raw_line in sys.stdin.buffer:
        yield raw_line.decode("utf-8", errors="replace").rstrip("\r\n")


def emit(*values):
    line = "\t".join(str(value) for value in values)
    sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))


current_key = None
current_total = 0

for line in iter_input_lines():
    if not line:
        continue

    key, value = line.split("\t", 1)
    if current_key != key:
        if current_key is not None:
            term = current_key.split("|", 1)[1]
            emit("VOCAB", term, current_total)
        current_key = key
        current_total = int(value)
    else:
        current_total += int(value)

if current_key is not None:
    term = current_key.split("|", 1)[1]
    emit("VOCAB", term, current_total)
