#!/usr/bin/env python3
import math
import sys
from collections import Counter
from operator import add

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from pyspark.sql import SparkSession

from common import BM25_B, BM25_K1, tokenize


def get_query_text(argv):
    if argv:
        query = " ".join(argv).strip()
        if query:
            return query

    stdin_query = sys.stdin.read().strip()
    if stdin_query:
        return stdin_query

    raise SystemExit("No query text provided.")


def fetch_stats(session):
    stats = {}
    for row in session.execute("SELECT stat_name, stat_value FROM corpus_stats"):
        stats[row.stat_name] = row.stat_value
    return stats


def fetch_rows(session, statement, args):
    if not args:
        return []
    results = execute_concurrent_with_args(session, statement, args, concurrency=32, raise_on_first_error=True)
    rows = []
    for success, result in results:
        if success:
            rows.extend(list(result))
    return rows


def main():
    query_text = get_query_text(sys.argv[1:])
    query_terms = tokenize(query_text)
    if not query_terms:
        raise SystemExit("The query does not contain any searchable tokens.")

    cluster = Cluster(["cassandra-server"])
    session = cluster.connect("search_engine")
    session.default_timeout = 60

    stats = fetch_stats(session)
    num_docs = int(stats.get("TOTAL_DOCS", 0))
    avg_doc_len = float(stats.get("AVG_DOC_LEN", 0.0))
    if num_docs == 0 or avg_doc_len == 0:
        raise SystemExit("The index is empty. Run index.sh before querying.")

    query_term_counts = Counter(query_terms)
    unique_terms = sorted(query_term_counts)

    vocabulary_stmt = session.prepare("SELECT term, df FROM vocabulary WHERE term = ?")
    posting_stmt = session.prepare("SELECT term, doc_id, tf FROM postings WHERE term = ?")
    document_stmt = session.prepare("SELECT doc_id, title, doc_len FROM documents WHERE doc_id = ?")

    vocabulary_rows = fetch_rows(session, vocabulary_stmt, [(term,) for term in unique_terms])
    df_by_term = {row.term: row.df for row in vocabulary_rows}
    searchable_terms = sorted(df_by_term)
    if not searchable_terms:
        print(f'No indexed documents matched query "{query_text}".')
        cluster.shutdown()
        return

    posting_rows = fetch_rows(session, posting_stmt, [(term,) for term in searchable_terms])
    if not posting_rows:
        print(f'No indexed documents matched query "{query_text}".')
        cluster.shutdown()
        return

    candidate_doc_ids = sorted({row.doc_id for row in posting_rows})
    document_rows = fetch_rows(session, document_stmt, [(doc_id,) for doc_id in candidate_doc_ids])
    doc_by_id = {row.doc_id: {"title": row.title, "doc_len": row.doc_len} for row in document_rows}

    spark = SparkSession.builder.appName("bm25-query").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_broadcast = spark.sparkContext.broadcast(df_by_term)
    docs_broadcast = spark.sparkContext.broadcast(doc_by_id)
    query_counts_broadcast = spark.sparkContext.broadcast(dict(query_term_counts))

    records = [(row.term, row.doc_id, row.tf) for row in posting_rows if row.doc_id in doc_by_id]
    records_rdd = spark.sparkContext.parallelize(records, max(1, min(len(records), 32)))

    def score_record(record):
        term, doc_id, tf = record
        doc = docs_broadcast.value[doc_id]
        df = df_broadcast.value[term]
        qtf = query_counts_broadcast.value[term]
        idf = math.log(num_docs / df) if df and num_docs >= df else 0.0
        denominator = BM25_K1 * ((1.0 - BM25_B) + BM25_B * (doc["doc_len"] / avg_doc_len)) + tf
        score = qtf * idf * (((BM25_K1 + 1.0) * tf) / denominator)
        return doc_id, score

    top_results = (
        records_rdd.map(score_record)
        .reduceByKey(add)
        .takeOrdered(10, key=lambda item: -item[1])
    )

    print(f'Query: "{query_text}"')
    if not top_results:
        print("No results.")
    else:
        print("doc_id\ttitle\tscore")
        for doc_id, score in top_results:
            doc = doc_by_id[doc_id]
            print(f"{doc_id}\t{doc['title']}\t{score:.6f}")

    spark.stop()
    cluster.shutdown()


if __name__ == "__main__":
    main()
