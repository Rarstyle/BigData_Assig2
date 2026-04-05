#!/usr/bin/env python3
import sys


def iter_input_lines():
    for raw_line in sys.stdin.buffer:
        yield raw_line.decode("utf-8", errors="replace").rstrip("\r\n")


def emit(*values):
    line = "\t".join(str(value) for value in values)
    sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))


def flush(current_key, values):
    if current_key is None:
        return

    record_type, _, remainder = current_key.partition("|")

    if record_type == "POSTING":
        term, _, doc_id = remainder.partition("|")
        tf = sum(int(value[0]) for value in values)
        emit("POSTING", term, doc_id, tf)
        return

    if record_type == "DOC":
        doc_id = remainder
        title, doc_len = values[-1]
        emit("DOC", doc_id, title, doc_len)
        return

    if record_type == "STAT":
        stat_name = remainder
        stat_value = sum(int(value[0]) for value in values)
        emit("STAT", stat_name, stat_value)


current_key = None
buffer = []

for line in iter_input_lines():
    if not line:
        continue

    fields = line.split("\t")
    key = fields[0]
    value = fields[1:]

    if current_key != key:
        flush(current_key, buffer)
        current_key = key
        buffer = [value]
    else:
        buffer.append(value)

flush(current_key, buffer)
