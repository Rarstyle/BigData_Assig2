#!/usr/bin/env python3
import argparse
import json
import shlex
import subprocess
from pathlib import Path
from pathlib import PurePosixPath

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import normalize_text_field, parse_doc_filename, tokenize


def run(command):
    print(f"[prepare_data] $ {command}")
    subprocess.run(["bash", "-lc", command], check=True)


def clear_txt_files(directory):
    directory.mkdir(parents=True, exist_ok=True)
    for path in directory.glob("*.txt"):
        path.unlink()


def write_document(path, text):
    path.write_text(text, encoding="utf-8")


def create_docs_from_parquet(spark, parquet_path, output_dir, doc_target):
    clear_txt_files(output_dir)

    dataframe = (
        spark.read.parquet(parquet_path)
        .select("id", "title", "text")
        .where(F.col("id").isNotNull())
        .where(F.col("title").isNotNull())
        .where(F.col("text").isNotNull())
        .where(F.length(F.trim(F.col("text"))) > 0)
        .limit(doc_target)
    )

    written = 0
    for row in dataframe.toLocalIterator():
        doc_id = normalize_text_field(row["id"])
        title = normalize_text_field(row["title"]) or "untitled"
        text = str(row["text"])
        if not tokenize(text):
            continue

        filename = sanitize_filename(f"{doc_id}_{title}").replace(" ", "_") + ".txt"
        write_document(output_dir / filename, text)
        written += 1

    return written


def create_synthetic_smoke_corpus(output_dir):
    clear_txt_files(output_dir)
    docs = {
        "1_Cats_and_Dogs.txt": "cats and dogs are pets",
        "2_Dogs_Longer_Document.txt": (
            "cats and dogs are pet animals though i prefer dogs dogs obey our commands "
            "can be trained easily and play with us all the time"
        ),
        "3_Horses_and_Pets.txt": "horses are also pets",
        "4_Search_Engines.txt": "search engines index documents and answer queries with ranked results",
        "5_Hadoop_MapReduce.txt": "hadoop mapreduce can build an inverted index for simple search engines",
    }

    for filename, text in docs.items():
        write_document(output_dir / filename, text)

    return len(docs)


def sync_docs_to_hdfs(local_dir, hdfs_docs_dir):
    hdfs_target = PurePosixPath(hdfs_docs_dir)
    parent = str(hdfs_target.parent) if str(hdfs_target.parent) != "." else "/"
    local_dir_str = str(local_dir)
    uploaded_dir = str(PurePosixPath(parent) / local_dir.name) if parent != "/" else f"/{local_dir.name}"

    run(f"hdfs dfs -rm -r -f {shlex.quote(hdfs_docs_dir)} || true")
    run(f"hdfs dfs -mkdir -p {shlex.quote(parent)}")
    run(f"hdfs dfs -put -f {shlex.quote(local_dir_str)} {shlex.quote(parent)}")
    if uploaded_dir != hdfs_docs_dir:
        run(f"hdfs dfs -rm -r -f {shlex.quote(hdfs_docs_dir)} || true")
        run(f"hdfs dfs -mv {shlex.quote(uploaded_dir)} {shlex.quote(hdfs_docs_dir)}")
    run(f"hdfs dfs -test -d {shlex.quote(hdfs_docs_dir)}")


def make_input_line(record):
    path, content = record
    try:
        doc_id, title = parse_doc_filename(path)
    except ValueError:
        return None

    normalized_text = normalize_text_field(content)
    if not tokenize(normalized_text):
        return None

    return f"{doc_id}\t{title}\t{normalized_text}"


def build_input_dataset(spark, hdfs_uri, hdfs_docs_dir, hdfs_input_dir):
    input_glob = f"{hdfs_uri}{hdfs_docs_dir}/*.txt"
    output_path = f"{hdfs_uri}{hdfs_input_dir}"

    spark.sparkContext.setLogLevel("WARN")
    docs_rdd = spark.sparkContext.wholeTextFiles(input_glob)
    formatted_rdd = docs_rdd.map(make_input_line).filter(lambda line: line is not None)
    formatted_rdd = formatted_rdd.coalesce(1)

    count = formatted_rdd.count()
    if count == 0:
        raise RuntimeError("No non-empty documents were available to write to /input/data.")

    run(f"hdfs dfs -rm -r -f {shlex.quote(hdfs_input_dir)} || true")
    formatted_rdd.saveAsTextFile(output_path)
    return count


def write_summary(summary_path, payload):
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description="Prepare corpus documents and /input/data in HDFS.")
    parser.add_argument("--parquet-path", default="", help="Local or HDFS parquet file to use when available.")
    parser.add_argument("--doc-target", type=int, default=1000, help="Target number of documents to extract.")
    parser.add_argument("--hdfs-uri", default="hdfs://cluster-master:9000", help="Base HDFS URI.")
    parser.add_argument("--hdfs-docs-dir", default="/data", help="HDFS directory for plain text docs.")
    parser.add_argument("--hdfs-input-dir", default="/input/data", help="HDFS directory for the tabular input file.")
    return parser.parse_args()


def main():
    args = parse_args()
    app_dir = Path(__file__).resolve().parent
    generated_dir = app_dir / "generated_data"
    smoke_dir = app_dir / "smoke_data"
    fallback_dir = app_dir / "data"

    spark = (
        SparkSession.builder.appName("data-preparation")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )

    source_dir = fallback_dir
    source_mode = "sample_docs"
    local_doc_count = len(list(fallback_dir.glob("*.txt")))

    parquet_path = args.parquet_path.strip()
    if parquet_path:
        try:
            local_doc_count = create_docs_from_parquet(
                spark=spark,
                parquet_path=parquet_path,
                output_dir=generated_dir,
                doc_target=args.doc_target,
            )
            if local_doc_count > 0:
                source_dir = generated_dir
                source_mode = "parquet"
        except Exception as exc:
            print(f"[prepare_data] Parquet preparation failed, falling back to sample docs: {exc}")

    if not list(source_dir.glob("*.txt")):
        local_doc_count = create_synthetic_smoke_corpus(smoke_dir)
        source_dir = smoke_dir
        source_mode = "synthetic_smoke"

    sync_docs_to_hdfs(source_dir, args.hdfs_docs_dir)
    prepared_count = build_input_dataset(
        spark=spark,
        hdfs_uri=args.hdfs_uri,
        hdfs_docs_dir=args.hdfs_docs_dir,
        hdfs_input_dir=args.hdfs_input_dir,
    )

    summary = {
        "source_mode": source_mode,
        "local_source_dir": str(source_dir),
        "local_doc_count": local_doc_count,
        "prepared_count": prepared_count,
        "hdfs_docs_dir": args.hdfs_docs_dir,
        "hdfs_input_dir": args.hdfs_input_dir,
        "parquet_path": parquet_path,
    }
    write_summary(app_dir / "artifacts" / "prepare_data_summary.json", summary)
    print(json.dumps(summary, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
