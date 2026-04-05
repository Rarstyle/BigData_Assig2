#!/usr/bin/env python3
import math
import re
from pathlib import PurePosixPath


TOKEN_RE = re.compile(r"[a-z0-9]+")
BM25_K1 = 1.2
BM25_B = 0.75


def tokenize(text):
    if not text:
        return []
    return TOKEN_RE.findall(text.lower())


def normalize_text_field(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ").split())


def parse_doc_filename(path):
    filename = PurePosixPath(path).name
    if filename.endswith(".txt"):
        filename = filename[:-4]

    doc_id, separator, title = filename.partition("_")
    if not separator or not doc_id or not title:
        raise ValueError(f"Unexpected document filename format: {path}")

    return doc_id, normalize_text_field(title)


def bm25_idf(num_docs, df):
    if num_docs <= 0 or df <= 0 or df > num_docs:
        return 0.0
    return math.log(num_docs / df)


def bm25_score(tf, doc_len, avg_doc_len, df, num_docs, k1=BM25_K1, b=BM25_B):
    if tf <= 0 or doc_len <= 0 or avg_doc_len <= 0:
        return 0.0

    denominator = k1 * ((1.0 - b) + b * (doc_len / avg_doc_len)) + tf
    if denominator == 0:
        return 0.0

    return bm25_idf(num_docs, df) * (((k1 + 1.0) * tf) / denominator)
