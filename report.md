# Assignment 2 Report: Simple Search Engine using Hadoop MapReduce

## Methodology

### Repository and runtime architecture

The repository follows the provided Docker Compose template and runs three services:

- `cluster-master`: Hadoop NameNode, YARN ResourceManager, Spark client, and the assignment entrypoint
- `cluster-slave-1`: Hadoop worker and YARN NodeManager
- `cassandra-server`: one-node Cassandra database used to store the search index

The verified grading path is:

```bash
docker compose up
```

This starts `/app/app.sh` inside `cluster-master`, which then:

1. starts HDFS, YARN, and the MapReduce history server
2. creates a Python virtual environment and installs dependencies from `app/requirements.txt`
3. uploads the committed parquet slice `app/a.parquet` to HDFS as `/a.parquet`
4. runs `prepare_data.py` to extract at least 1000 non-empty documents and write them to `/data` in HDFS
5. uses PySpark RDD operations to write `/input/data` as one partition with rows in the format `<doc_id>\t<doc_title>\t<doc_text>`
6. runs Hadoop Streaming pipelines to create the index in HDFS under `/indexer`
7. loads the index into Cassandra tables in keyspace `search_engine`
8. runs three BM25 searches through `search.sh` on YARN

To reduce grading risk, the required script names also exist at the repository root as thin wrappers around the real implementations in `app/`.

### Data collection and preparation

The repository includes a committed parquet slice at `app/a.parquet` so the default grading run does not depend on Kaggle credentials or manual downloads. The verified slice contains exactly 1000 non-empty rows with the columns `id`, `title`, and `text`.

`prepare_data.sh` first uploads the parquet file to HDFS:

```bash
hdfs dfs -put -f /app/a.parquet /a.parquet
```

Then `prepare_data.py` reads `hdfs://cluster-master:9000/a.parquet` with PySpark, selects the required fields, filters out empty rows, and creates UTF-8 plain text documents named:

```text
<doc_id>_<doc_title>.txt
```

The generated documents are stored in HDFS `/data`, and then PySpark RDD `wholeTextFiles` is used to build `/input/data` as one output partition. The verified run produced exactly 1000 lines in `/input/data`.

No Pandas or single-machine dataframe package is used anywhere in the data preparation path.

### Tokenization and text normalization

The shared tokenizer in `app/common.py` is used for both indexing and querying:

- lowercase the text
- extract tokens with the regex `[a-z0-9]+`
- treat document length as the number of extracted tokens
- do not apply stemming
- do not remove stopwords

This keeps the indexer and ranker consistent and deterministic.

### Indexer design

The index is created with Hadoop Streaming and two MapReduce pipelines.

#### Pipeline 1

Files:

- `app/mapreduce/mapper1.py`
- `app/mapreduce/reducer1.py`

Input:

- `/input/data`

Mapper 1 reads each prepared document row, tokenizes the text, computes per-document term frequency, and emits:

- `POSTING|term|doc_id -> tf`
- `DOC|doc_id -> title, doc_len`
- `STAT|TOTAL_DOCS -> 1`
- `STAT|TOTAL_DOC_LEN -> doc_len`

Reducer 1 aggregates these records and writes normalized tab-separated rows:

- `POSTING\tterm\tdoc_id\ttf`
- `DOC\tdoc_id\ttitle\tdoc_len`
- `STAT\tTOTAL_DOCS\tvalue`
- `STAT\tTOTAL_DOC_LEN\tvalue`

The output is stored in `/tmp/indexer/pipeline1_base`.

#### Pipeline 2

Files:

- `app/mapreduce/mapper2.py`
- `app/mapreduce/reducer2.py`

Input:

- `/tmp/indexer/pipeline1_base`

Mapper 2 keeps only posting rows and emits a single count for each distinct `(term, doc_id)` posting. Reducer 2 sums these counts and writes:

- `VOCAB\tterm\tdf`

The final vocabulary is written to `/indexer/vocabulary`.

#### Final HDFS index layout

After the pipelines finish, `create_index.sh` creates dedicated HDFS folders:

- `/indexer/postings`
- `/indexer/documents`
- `/indexer/stats`
- `/indexer/vocabulary`

The verified run produced:

- 251313 posting rows
- 40262 vocabulary rows
- 1000 documents
- 3 corpus statistic rows

The stats file stores:

- `TOTAL_DOCS`
- `TOTAL_DOC_LEN`
- `AVG_DOC_LEN`

### Cassandra schema

The index is loaded into Cassandra keyspace `search_engine` by `store_index.sh`, which calls `app.py`.

The tables are:

1. `postings`

   ```sql
   CREATE TABLE postings (
       term text,
       doc_id bigint,
       tf int,
       PRIMARY KEY ((term), doc_id)
   )
   ```

2. `vocabulary`

   ```sql
   CREATE TABLE vocabulary (
       term text PRIMARY KEY,
       df int
   )
   ```

3. `documents`

   ```sql
   CREATE TABLE documents (
       doc_id bigint PRIMARY KEY,
       title text,
       doc_len int
   )
   ```

4. `corpus_stats`

   ```sql
   CREATE TABLE corpus_stats (
       stat_name text PRIMARY KEY,
       stat_value double
   )
   ```

`app.py` waits for Cassandra, creates the schema if necessary, truncates old rows, loads the latest HDFS index, and prints the final row counts.

### Ranker design

`query.py` is a PySpark application that reads the user query from command-line arguments or stdin, tokenizes it with the same tokenizer as the indexer, and computes BM25 scores using Spark RDD operations.

The query flow is:

1. fetch corpus stats from `corpus_stats`
2. fetch document frequency values from `vocabulary`
3. fetch postings for the matched terms from `postings`
4. fetch document titles and lengths from `documents`
5. parallelize the candidate posting rows into a Spark RDD
6. compute BM25 partial scores per `(term, doc_id)`
7. reduce by `doc_id`
8. return the top 10 documents sorted by descending score

The implementation uses:

- `k1 = 1.2`
- `b = 0.75`

The BM25 contribution per term is:

```text
idf(t) * ((k1 + 1) * tf(t, d)) / (k1 * ((1 - b) + b * dl(d) / avgdl) + tf(t, d))
```

with repeated query terms contributing multiple times through query-term frequency.

### Search execution on YARN

`search.sh` runs the ranker on YARN by default:

```bash
bash /app/search.sh "dogs"
```

To make this reliable on the provided one-worker Docker cluster, the script uses:

- `--master yarn`
- `--deploy-mode client`
- `spark.yarn.jars=local:/usr/local/spark/jars/*`
- container `python3` for the executor Python runtime

This keeps the search step fully on YARN while avoiding the slow large-archive localization path that previously blocked the Application Master on this setup.

## Demonstration

### Verified grading run

The full `docker compose up` grading path was executed successfully on March 27, 2026.

The verified run showed:

- HDFS `/data` populated with 1000 documents
- `/input/data` created as one partition with 1000 lines
- HDFS index folders created under `/indexer`
- Cassandra loaded with postings, vocabulary, documents, and corpus stats
- three YARN-backed BM25 search queries completed successfully
- final message: `Workflow finished successfully`

### Commands used

Main grading command:

```bash
docker compose up
```

Useful manual commands inside the running master container:

```bash
hdfs dfs -ls /data
hdfs dfs -ls /input/data
hdfs dfs -ls -R /indexer
bash /app/search.sh "dogs"
bash /app/search.sh "history of science"
bash /app/search.sh "christmas carol"
```

### Observed indexing results

The verified index layout in HDFS was:

```text
/indexer/documents/part-00000
/indexer/postings/part-00000
/indexer/stats/part-00000
/indexer/vocabulary/part-00000
```

The verified Cassandra row counts were:

```text
postings      251313
vocabulary     40262
documents       1000
corpus_stats       3
```

### Observed query results

For the query `dogs`, the top results were dominated by dog-related titles such as `A_Dangerous_Path`, `A_Dog's_Purpose`, and `A_Dog_Year`, which is consistent with the indexed term distribution.

For the query `history of science`, the top result was `A_History_of_Science,_Technology,_and_Philosophy_in_the_16th_and_17thCenturies`, followed by several other history-themed articles. This is a good qualitative match for the query.

For the query `christmas carol`, the top results were strongly focused on Christmas Carol variants, including `A_Flintstones_Christmas_Carol`, `A_Christmas_Carol_(1982_film)`, `A_Christmas_Carol_(2006_film)`, and `A_Christmas_Carol`, which indicates that the BM25 ranker is favoring highly relevant documents as expected.

### Reflections

The implemented system satisfies the intended assignment pipeline:

- PySpark for data preparation
- Hadoop MapReduce for offline indexing
- Cassandra for persistent index storage
- Spark RDDs for BM25 ranking

The design is intentionally simple, but it is reproducible, easy to inspect, and stable enough for repeated `docker compose up` grading runs. The main engineering adjustment beyond the baseline template was making the YARN search step reliable in the one-worker Docker environment while preserving the assignment requirement that `search.sh` runs the query application on YARN.
