#!/usr/bin/env bash
set -euo pipefail

cd /app

INPUT_PATH="${1:-/input/data}"
TMP_ROOT=/tmp/indexer
INDEX_ROOT=/indexer
LOCAL_WORKDIR=/tmp/search_engine_index
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"

find_streaming_jar() {
  local candidates=(
    "${HADOOP_HOME}/share/hadoop/tools/lib"
    "/opt/hadoop/share/hadoop/tools/lib"
    "/usr/local/hadoop/share/hadoop/tools/lib"
  )
  for candidate_dir in "${candidates[@]}"; do
    if [[ -d "${candidate_dir}" ]]; then
      find "${candidate_dir}" -name 'hadoop-streaming*.jar' | head -n 1
      return
    fi
  done
}

STREAMING_JAR="$(find_streaming_jar)"
if [[ -z "${STREAMING_JAR}" ]]; then
  echo "[create_index.sh] Failed to locate hadoop-streaming jar." >&2
  exit 1
fi

if ! hdfs dfs -test -d "${INPUT_PATH}"; then
  echo "[create_index.sh] Input path ${INPUT_PATH} does not exist in HDFS." >&2
  exit 1
fi

echo "[create_index.sh] Using Hadoop streaming jar: ${STREAMING_JAR}"
echo "[create_index.sh] Indexing input path: ${INPUT_PATH}"

hdfs dfs -rm -r -f "${TMP_ROOT}" || true
hdfs dfs -rm -r -f "${INDEX_ROOT}" || true
hdfs dfs -mkdir -p "${TMP_ROOT}"
hdfs dfs -mkdir -p "${INDEX_ROOT}"

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name="search-engine-pipeline1-base" \
  -D mapreduce.job.reduces=1 \
  -D mapreduce.map.memory.mb=512 \
  -D mapreduce.reduce.memory.mb=512 \
  -D mapreduce.map.java.opts=-Xmx384m \
  -D mapreduce.reduce.java.opts=-Xmx384m \
  -cmdenv PYTHONIOENCODING=UTF-8 \
  -cmdenv LANG=C.UTF-8 \
  -cmdenv LC_ALL=C.UTF-8 \
  -file /app/mapreduce/mapper1.py \
  -file /app/mapreduce/reducer1.py \
  -file /app/common.py \
  -input "${INPUT_PATH}" \
  -output "${TMP_ROOT}/pipeline1_base" \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py"

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name="search-engine-pipeline2-vocabulary" \
  -D mapreduce.job.reduces=1 \
  -D mapreduce.map.memory.mb=512 \
  -D mapreduce.reduce.memory.mb=512 \
  -D mapreduce.map.java.opts=-Xmx384m \
  -D mapreduce.reduce.java.opts=-Xmx384m \
  -cmdenv PYTHONIOENCODING=UTF-8 \
  -cmdenv LANG=C.UTF-8 \
  -cmdenv LC_ALL=C.UTF-8 \
  -file /app/mapreduce/mapper2.py \
  -file /app/mapreduce/reducer2.py \
  -input "${TMP_ROOT}/pipeline1_base" \
  -output "${INDEX_ROOT}/vocabulary" \
  -mapper "python3 mapper2.py" \
  -reducer "python3 reducer2.py"

mkdir -p "${LOCAL_WORKDIR}"
hdfs dfs -cat "${TMP_ROOT}/pipeline1_base/part-*" > "${LOCAL_WORKDIR}/pipeline1_base.tsv"

awk -F '\t' '$1=="POSTING"{print}' "${LOCAL_WORKDIR}/pipeline1_base.tsv" > "${LOCAL_WORKDIR}/postings.tsv"
awk -F '\t' '$1=="DOC"{print}' "${LOCAL_WORKDIR}/pipeline1_base.tsv" > "${LOCAL_WORKDIR}/documents.tsv"

TOTAL_DOCS="$(awk -F '\t' '$1=="STAT" && $2=="TOTAL_DOCS"{sum+=$3} END{print sum+0}' "${LOCAL_WORKDIR}/pipeline1_base.tsv")"
TOTAL_DOC_LEN="$(awk -F '\t' '$1=="STAT" && $2=="TOTAL_DOC_LEN"{sum+=$3} END{print sum+0}' "${LOCAL_WORKDIR}/pipeline1_base.tsv")"
AVG_DOC_LEN="$(python3 - <<PY
total_docs = int("${TOTAL_DOCS}")
total_doc_len = int("${TOTAL_DOC_LEN}")
print((total_doc_len / total_docs) if total_docs else 0.0)
PY
)"

cat > "${LOCAL_WORKDIR}/stats.tsv" <<EOF
STAT	TOTAL_DOCS	${TOTAL_DOCS}
STAT	TOTAL_DOC_LEN	${TOTAL_DOC_LEN}
STAT	AVG_DOC_LEN	${AVG_DOC_LEN}
EOF

hdfs dfs -mkdir -p "${INDEX_ROOT}/postings" "${INDEX_ROOT}/documents" "${INDEX_ROOT}/stats"
hdfs dfs -put -f "${LOCAL_WORKDIR}/postings.tsv" "${INDEX_ROOT}/postings/part-00000"
hdfs dfs -put -f "${LOCAL_WORKDIR}/documents.tsv" "${INDEX_ROOT}/documents/part-00000"
hdfs dfs -put -f "${LOCAL_WORKDIR}/stats.tsv" "${INDEX_ROOT}/stats/part-00000"

echo "[create_index.sh] Final HDFS index layout:"
hdfs dfs -ls -R "${INDEX_ROOT}" | sed 's/^/[create_index.sh] /'
