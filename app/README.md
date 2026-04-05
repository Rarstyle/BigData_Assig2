## app folder
This folder contains the data folder and all scripts and source code that are required to run your simple search engine. 

### data
This folder stores the text documents required to index. The repository includes a bundled 1000-document plain-text corpus and a committed `a.parquet` slice so the default grading path can still follow the parquet-based preparation step without any external download.

### common.py
Shared tokenization and BM25 utilities used by both indexing and querying.

### mapreduce
This folder stores the mapper `mapperx.py` and reducer `reducerx.py` scripts for the MapReduce pipelines.

### app.py
This is a Python file to write code to store index data in Cassandra.

### app.sh
The entrypoint for the executables in your repository and includes all commands that will run your programs in this folder.

### create_index.sh
A script to create index data using MapReduce pipelines and store them in HDFS.

### index.sh
A script to run the MapReduce pipelines and the programs to store data in Cassandra/ScyllaDB.

### prepare_data.py
The script that will create documents from the parquet file, copy the generated text corpus to HDFS `/data`, and write `/input/data` as a single-partition PySpark output.

### prepare_data.sh
The script that will run the prevoious Python file and will copy the data to HDFS.

### query.py
A Python file to write PySpark app that will process a user's query and retrieves a list of top 10 relevant documents ranked using BM25.

### local_validation.py
An offline validation helper that runs the real mapper/reducer code and BM25 scoring without requiring Docker, HDFS, YARN, or Cassandra.

### requirements.txt
This file contains all Python depenedencies that are needed for running the programs in this repository. This file is read by pip when installing the dependencies in `app.sh` script.

### search.sh
This script runs the `query.py` PySpark app on the Hadoop YARN cluster by default. It uses the local virtual environment for the driver and the Spark installation already present on the cluster nodes, with container `python3` for executors.


### start-services.sh
This script will initiate the services required to run Hadoop components. This script is called in `app.sh` file.


### store_index.sh
This script will create Cassandra/ScyllaDB tables and load the index data from HDFS to them.

### verify_local.sh
Runs syntax checks and offline validation so the repository can be sanity-checked before Docker is available.
