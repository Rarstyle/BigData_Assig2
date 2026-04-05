#!/usr/bin/env python3
import sys
from collections import Counter


def tokenize(text):
    if not text:
        return []

    token = []
    tokens = []
    for char in text.lower():
        if char.isalnum() and char.isascii():
            token.append(char)
            continue

        if token:
            tokens.append("".join(token))
            token.clear()

    if token:
        tokens.append("".join(token))

    return tokens


def normalize_text_field(value):
    if value is None:
        return ""

    return " ".join(str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ").split())


def iter_input_lines():
    for raw_line in sys.stdin.buffer:
        yield raw_line.decode("utf-8", errors="replace").rstrip("\r\n")


def emit(key, *values):
    line = "\t".join([key, *[str(value) for value in values]])
    sys.stdout.buffer.write((line + "\n").encode("utf-8", errors="replace"))


for line in iter_input_lines():
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) != 3:
        continue

    doc_id, title, text = parts
    title = normalize_text_field(title)
    tokens = tokenize(text)
    if not tokens:
        continue

    term_counts = Counter(tokens)
    doc_len = len(tokens)

    emit(f"DOC|{doc_id}", title, doc_len)
    emit("STAT|TOTAL_DOCS", 1)
    emit("STAT|TOTAL_DOC_LEN", doc_len)

    for term, tf in sorted(term_counts.items()):
        emit(f"POSTING|{term}|{doc_id}", tf)
