#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
sys.path.append(str(APP_DIR))

from common import BM25_B, BM25_K1, bm25_score, normalize_text_field, parse_doc_filename, tokenize


def run_process(command, input_text):
    result = subprocess.run(
        command,
        cwd=APP_DIR,
        input=input_text,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def mapreduce_index(input_lines):
    mapper1_out = run_process(["python3", str(APP_DIR / "mapreduce" / "mapper1.py")], "".join(f"{line}\n" for line in input_lines))
    reducer1_in = "\n".join(sorted(line for line in mapper1_out.splitlines() if line.strip())) + "\n"
    base_output = run_process(["python3", str(APP_DIR / "mapreduce" / "reducer1.py")], reducer1_in)

    mapper2_out = run_process(["python3", str(APP_DIR / "mapreduce" / "mapper2.py")], base_output)
    reducer2_in = "\n".join(sorted(line for line in mapper2_out.splitlines() if line.strip())) + "\n"
    vocab_output = run_process(["python3", str(APP_DIR / "mapreduce" / "reducer2.py")], reducer2_in)
    return base_output, vocab_output


def parse_outputs(base_output, vocab_output):
    documents = {}
    postings = defaultdict(dict)
    stats = {}
    vocabulary = {}

    for line in base_output.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if parts[0] == "DOC":
            _, doc_id, title, doc_len = parts
            documents[doc_id] = {"title": title, "doc_len": int(doc_len)}
        elif parts[0] == "POSTING":
            _, term, doc_id, tf = parts
            postings[term][doc_id] = int(tf)
        elif parts[0] == "STAT":
            _, stat_name, stat_value = parts
            stats[stat_name] = float(stat_value)

    for line in vocab_output.splitlines():
        if not line.strip():
            continue
        _, term, df = line.split("\t")
        vocabulary[term] = int(df)

    if "TOTAL_DOCS" in stats and "TOTAL_DOC_LEN" in stats and "AVG_DOC_LEN" not in stats:
        total_docs = int(stats["TOTAL_DOCS"])
        total_doc_len = stats["TOTAL_DOC_LEN"]
        stats["AVG_DOC_LEN"] = (total_doc_len / total_docs) if total_docs else 0.0

    return {
        "documents": documents,
        "postings": postings,
        "stats": stats,
        "vocabulary": vocabulary,
    }


def search_index(index_data, query_text, top_k=10):
    query_terms = tokenize(query_text)
    query_term_counts = defaultdict(int)
    for term in query_terms:
        query_term_counts[term] += 1

    total_docs = int(index_data["stats"].get("TOTAL_DOCS", 0))
    avg_doc_len = float(index_data["stats"].get("AVG_DOC_LEN", 0.0))
    scores = defaultdict(float)

    for term, qtf in query_term_counts.items():
        df = index_data["vocabulary"].get(term)
        if not df:
            continue
        for doc_id, tf in index_data["postings"].get(term, {}).items():
            doc = index_data["documents"][doc_id]
            score = bm25_score(
                tf=tf,
                doc_len=doc["doc_len"],
                avg_doc_len=avg_doc_len,
                df=df,
                num_docs=total_docs,
                k1=BM25_K1,
                b=BM25_B,
            )
            scores[doc_id] += qtf * score

    ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:top_k]
    return [
        {
            "doc_id": doc_id,
            "title": index_data["documents"][doc_id]["title"],
            "score": score,
        }
        for doc_id, score in ranked
    ]


def input_lines_from_docs(documents):
    lines = []
    for doc_id, title, text in documents:
        lines.append(f"{doc_id}\t{normalize_text_field(title)}\t{normalize_text_field(text)}")
    return lines


def input_lines_from_directory(directory, limit=None):
    lines = []
    paths = sorted(directory.glob("*.txt"))
    if limit is not None:
        paths = paths[:limit]
    for path in paths:
        doc_id, title = parse_doc_filename(path.name)
        text = path.read_text(encoding="utf-8")
        if tokenize(text):
            lines.append(f"{doc_id}\t{title}\t{normalize_text_field(text)}")
    return lines


def format_results(query_text, results):
    lines = [f'Query: "{query_text}"']
    if not results:
        lines.append("No results")
    else:
        for row in results:
            lines.append(f"{row['doc_id']}\t{row['title']}\t{row['score']:.6f}")
    return "\n".join(lines)


def run_bm25_sanity(output_dir):
    documents = [
        ("1", "cats_and_dogs", "cats and dogs are pets"),
        (
            "2",
            "dogs_longer_document",
            "cats and dogs are pet animals though i prefer dogs dogs obey our commands can be trained easily and play with us all the time",
        ),
        ("3", "horses_and_pets", "horses are also pets"),
    ]
    base_output, vocab_output = mapreduce_index(input_lines_from_docs(documents))
    index_data = parse_outputs(base_output, vocab_output)
    results = search_index(index_data, "dogs", top_k=3)

    lines = [
        "BM25 sanity check using the classic dogs example",
        f"k1={BM25_K1}, b={BM25_B}",
        format_results("dogs", results),
    ]
    output_path = output_dir / "bm25_sanity.txt"
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path, results


def run_sample_validation(output_dir, sample_dir, limit):
    input_lines = input_lines_from_directory(sample_dir, limit=limit)
    base_output, vocab_output = mapreduce_index(input_lines)
    index_data = parse_outputs(base_output, vocab_output)

    queries = [
        "dogs",
        "history of science",
        "christmas carol",
    ]

    sections = [
        f"Local sample validation over {len(input_lines)} documents from {sample_dir}",
        json.dumps(
            {
                "total_docs": int(index_data["stats"].get("TOTAL_DOCS", 0)),
                "avg_doc_len": index_data["stats"].get("AVG_DOC_LEN", 0.0),
                "vocabulary_size": len(index_data["vocabulary"]),
            },
            indent=2,
        ),
    ]
    query_results = {}
    for query in queries:
        results = search_index(index_data, query)
        query_results[query] = results
        sections.append(format_results(query, results))

    output_path = output_dir / "local_sample_validation.txt"
    output_path.write_text("\n\n".join(sections) + "\n", encoding="utf-8")
    return output_path, query_results


def parse_args():
    parser = argparse.ArgumentParser(description="Local validation for the search engine without HDFS/Cassandra.")
    parser.add_argument("--sample-dir", default=str(APP_DIR / "data"))
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--output-dir", default=str(APP_DIR / "artifacts"))
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    bm25_path, bm25_results = run_bm25_sanity(output_dir)
    sample_path, sample_results = run_sample_validation(output_dir, Path(args.sample_dir), args.limit)

    summary = {
        "bm25_sanity_path": str(bm25_path),
        "bm25_top_doc": bm25_results[0]["doc_id"] if bm25_results else None,
        "local_sample_validation_path": str(sample_path),
        "sample_queries": list(sample_results),
    }
    summary_path = output_dir / "local_validation_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
