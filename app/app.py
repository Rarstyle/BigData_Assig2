#!/usr/bin/env python3
import argparse
import socket
import subprocess
import time

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args


def hdfs_cat(path_glob):
    process = subprocess.Popen(
        ["bash", "-lc", f"hdfs dfs -cat {path_glob}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert process.stdout is not None
    for line in process.stdout:
        yield line.rstrip("\n")

    stderr = process.stderr.read() if process.stderr is not None else ""
    exit_code = process.wait()
    if exit_code != 0:
        raise RuntimeError(f"hdfs dfs -cat failed for {path_glob}: {stderr.strip()}")


def wait_for_cassandra(host, port, timeout_seconds):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(2)
            try:
                sock.connect((host, port))
                return
            except OSError:
                time.sleep(2)
    raise TimeoutError(f"Cassandra at {host}:{port} did not become ready within {timeout_seconds} seconds.")


def create_schema(session, keyspace):
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(keyspace)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS postings (
            term text,
            doc_id bigint,
            tf int,
            PRIMARY KEY ((term), doc_id)
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id bigint PRIMARY KEY,
            title text,
            doc_len int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            stat_name text PRIMARY KEY,
            stat_value double
        )
        """
    )

    for table_name in ("postings", "vocabulary", "documents", "corpus_stats"):
        session.execute(f"TRUNCATE {table_name}")


def load_rows(session, statement, rows, batch_size=500):
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            execute_concurrent_with_args(session, statement, batch, concurrency=100, raise_on_first_error=True)
            batch.clear()

    if batch:
        execute_concurrent_with_args(session, statement, batch, concurrency=100, raise_on_first_error=True)


def parse_postings(hdfs_root):
    for line in hdfs_cat(f"{hdfs_root}/postings/part-*"):
        if not line:
            continue
        record_type, term, doc_id, tf = line.split("\t")
        if record_type != "POSTING":
            continue
        yield (term, int(doc_id), int(tf))


def parse_vocabulary(hdfs_root):
    for line in hdfs_cat(f"{hdfs_root}/vocabulary/part-*"):
        if not line:
            continue
        record_type, term, df = line.split("\t")
        if record_type != "VOCAB":
            continue
        yield (term, int(df))


def parse_documents(hdfs_root):
    for line in hdfs_cat(f"{hdfs_root}/documents/part-*"):
        if not line:
            continue
        record_type, doc_id, title, doc_len = line.split("\t")
        if record_type != "DOC":
            continue
        yield (int(doc_id), title, int(doc_len))


def parse_stats(hdfs_root):
    for line in hdfs_cat(f"{hdfs_root}/stats/part-*"):
        if not line:
            continue
        record_type, stat_name, stat_value = line.split("\t")
        if record_type != "STAT":
            continue
        yield (stat_name, float(stat_value))


def load_index(host, keyspace, hdfs_root):
    cluster = Cluster([host])
    session = cluster.connect()
    session.default_timeout = 60

    create_schema(session, keyspace)

    postings_stmt = session.prepare("INSERT INTO postings (term, doc_id, tf) VALUES (?, ?, ?)")
    vocabulary_stmt = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    documents_stmt = session.prepare("INSERT INTO documents (doc_id, title, doc_len) VALUES (?, ?, ?)")
    stats_stmt = session.prepare("INSERT INTO corpus_stats (stat_name, stat_value) VALUES (?, ?)")

    load_rows(session, postings_stmt, parse_postings(hdfs_root))
    load_rows(session, vocabulary_stmt, parse_vocabulary(hdfs_root))
    load_rows(session, documents_stmt, parse_documents(hdfs_root))
    load_rows(session, stats_stmt, parse_stats(hdfs_root))

    counts = {
        "postings": session.execute("SELECT count(*) FROM postings").one().count,
        "vocabulary": session.execute("SELECT count(*) FROM vocabulary").one().count,
        "documents": session.execute("SELECT count(*) FROM documents").one().count,
        "corpus_stats": session.execute("SELECT count(*) FROM corpus_stats").one().count,
    }
    print(counts)
    cluster.shutdown()


def parse_args():
    parser = argparse.ArgumentParser(description="Utilities for loading the index into Cassandra.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    wait_parser = subparsers.add_parser("wait-cassandra")
    wait_parser.add_argument("--host", default="cassandra-server")
    wait_parser.add_argument("--port", type=int, default=9042)
    wait_parser.add_argument("--timeout", type=int, default=180)

    load_parser = subparsers.add_parser("load-index")
    load_parser.add_argument("--host", default="cassandra-server")
    load_parser.add_argument("--keyspace", default="search_engine")
    load_parser.add_argument("--hdfs-root", default="/indexer")

    return parser.parse_args()


def main():
    args = parse_args()
    if args.command == "wait-cassandra":
        wait_for_cassandra(args.host, args.port, args.timeout)
        print(f"Cassandra is reachable at {args.host}:{args.port}")
        return

    if args.command == "load-index":
        load_index(args.host, args.keyspace, args.hdfs_root)
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
