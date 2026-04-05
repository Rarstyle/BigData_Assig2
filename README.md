# big-data-assignment2

## How to run

### Main grading path

1. Install:
   - Docker
   - Docker Compose
2. Run:

```bash
docker compose up
```

This creates:

- `cluster-master`: Hadoop/Spark master and assignment entrypoint
- `cluster-slave-1`: Hadoop worker
- `cassandra-server`: Cassandra database

The master container runs `app/app.sh`, which:

- starts Hadoop/YARN services
- installs Python dependencies
- prepares data from the committed `app/a.parquet` 1000-document slice by default
- creates the HDFS index
- stores the index in Cassandra
- runs three BM25 search queries on YARN

For submission convenience, the required scripts also exist at the repository root as thin wrappers around the real implementations in `app/`.

The repository also keeps the bundled 1000-document plain-text corpus in `app/data` as a fallback if the parquet file is removed.

### Offline verification path

If Docker is not installed yet, you can still verify the core logic locally:

```bash
bash app/verify_local.sh
```

That script checks:

- required assignment files exist
- shell and Python syntax
- local MapReduce behavior using the real mapper/reducer scripts
- BM25 sanity behavior
- search behavior over the bundled 1000-document sample corpus
