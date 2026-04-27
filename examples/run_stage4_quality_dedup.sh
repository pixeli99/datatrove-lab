#!/bin/bash
# Submit global Stage 4 exact dedup as three dependent Slurm arrays.
#
# Defaults are tuned for FineWeb-scale corpora (~50B docs across ~100 CC dumps):
#   - sha1 hashing: 64-bit xxhash has a non-trivial collision rate at this scale
#     (birthday-paradox estimate ~70 false positives at 5e10 docs).
#   - TASKS=2048, FINDER_WORKERS=128: keeps the signature shard count
#     (TASKS * FINDER_WORKERS) at ~260K instead of ~1M to spare Lustre metadata.
#   - SIGNATURE_TIME=48h: sha1 is ~2x slower than xxhash; signature stage is
#     the long tail.
#   - FINDER_MEM_PER_TASK=96G: per-bucket sort peaks at 2-3x raw signature size.
#
# Run from a login node for the full corpus:
#   OVERWRITE=1 bash examples/run_stage4_quality_dedup.sh
#
# Smoke test on a single CC dump first (recommended before full submission):
#   INPUT_ROOT=/work/projects/polyullm/lipengxiang_tmp/fineweb_012/CC-MAIN-2013-20 \
#   OUTPUT_ROOT=/tmp/dedup_test_out WORK_ROOT=/tmp/dedup_test_work \
#   LOG_ROOT=logs/stage4_smoke \
#   TASKS=64 FINDER_WORKERS=16 \
#   SIGNATURE_CONCURRENCY=32 FILTER_CONCURRENCY=32 FINDER_CONCURRENCY=8 \
#   SIGNATURE_TIME=04:00:00 FIND_TIME=02:00:00 FILTER_TIME=04:00:00 \
#   OVERWRITE=1 bash examples/run_stage4_quality_dedup.sh

set -euo pipefail

DATATROVE_ROOT="${DATATROVE_ROOT:-/lustre/projects/polyullm/lipengxiang_tmp/datatrove}"
INPUT_ROOT="${INPUT_ROOT:-/work/projects/polyullm/lipengxiang_tmp/fineweb_012}"
OUTPUT_ROOT="${OUTPUT_ROOT:-/work/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup}"
WORK_ROOT="${WORK_ROOT:-/work/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_work}"
LOG_ROOT="${LOG_ROOT:-logs/stage4_global_dedup}"
WORKER_SCRIPT="${WORKER_SCRIPT:-${DATATROVE_ROOT}/examples/stage4_quality_dedup_worker.sh}"
PATHS_FILE="${PATHS_FILE:-${WORK_ROOT}/input_paths.txt}"

IMAGE="${IMAGE:-/lustre/projects/polyullm/container/lmsysorg-sglang+v0.5.6.sqsh}"
MOUNTS="${MOUNTS:-/work/projects/polyullm:/work/projects/polyullm,/lustre/projects/polyullm:/lustre/projects/polyullm}"
HOME_DIR="${HOME_DIR:-/work/projects/polyullm/lipengxiang}"

TASKS="${TASKS:-2048}"
SIGNATURE_CONCURRENCY="${SIGNATURE_CONCURRENCY:-200}"
FILTER_CONCURRENCY="${FILTER_CONCURRENCY:-200}"
FINDER_WORKERS="${FINDER_WORKERS:-128}"
FINDER_CONCURRENCY="${FINDER_CONCURRENCY:-64}"

CPUS_PER_TASK="${CPUS_PER_TASK:-4}"
MEM_PER_TASK="${MEM_PER_TASK:-32G}"
FINDER_CPUS_PER_TASK="${FINDER_CPUS_PER_TASK:-4}"
FINDER_MEM_PER_TASK="${FINDER_MEM_PER_TASK:-96G}"

SIGNATURE_TIME="${SIGNATURE_TIME:-48:00:00}"
FIND_TIME="${FIND_TIME:-24:00:00}"
FILTER_TIME="${FILTER_TIME:-24:00:00}"
EXCLUDE="${EXCLUDE:-kb3-a1-nv-dgx11}"
HASH_FC="${HASH_FC:-sha1}"

export DATATROVE_ROOT INPUT_ROOT OUTPUT_ROOT WORK_ROOT PATHS_FILE IMAGE MOUNTS HOME_DIR TASKS FINDER_WORKERS HASH_FC

mkdir -p "${LOG_ROOT}"

if [[ "${OVERWRITE:-0}" == "1" ]]; then
  rm -rf "${OUTPUT_ROOT}" "${WORK_ROOT}"
elif [[ -e "${WORK_ROOT}/sigs" || -e "${WORK_ROOT}/dups" || -e "${OUTPUT_ROOT}" ]]; then
  echo "Refusing to reuse existing output/work dirs. Set OVERWRITE=1 to remove them first."
  echo "OUTPUT_ROOT=${OUTPUT_ROOT}"
  echo "WORK_ROOT=${WORK_ROOT}"
  exit 1
fi

mkdir -p "${OUTPUT_ROOT}" "${WORK_ROOT}"

find "${INPUT_ROOT}" -type f -name '*.parquet' ! -name '*.tmp' | sort | sed "s#^${INPUT_ROOT%/}/##" > "${PATHS_FILE}"
N_FILES=$(wc -l < "${PATHS_FILE}")
if [[ "${N_FILES}" -eq 0 ]]; then
  echo "No parquet files found under ${INPUT_ROOT}"
  exit 1
fi

echo "[SUBMIT] input=${INPUT_ROOT}"
echo "[SUBMIT] output=${OUTPUT_ROOT}"
echo "[SUBMIT] work=${WORK_ROOT}"
echo "[SUBMIT] paths_file=${PATHS_FILE} files=${N_FILES}"
echo "[SUBMIT] tasks=${TASKS} finder_workers=${FINDER_WORKERS}"

SIG_JOB=$(
  sbatch --parsable \
    --job-name=fw_s4_sig \
    --nodes=1 \
    --ntasks=1 \
    --array=0-$((TASKS - 1))%${SIGNATURE_CONCURRENCY} \
    --cpus-per-task="${CPUS_PER_TASK}" \
    --mem="${MEM_PER_TASK}" \
    --time="${SIGNATURE_TIME}" \
    --exclude="${EXCLUDE}" \
    --output="${LOG_ROOT}/%x-%A_%a.out" \
    --error="${LOG_ROOT}/%x-%A_%a.err" \
    --export=ALL,STAGE=signature \
    "${WORKER_SCRIPT}"
)
echo "[SUBMIT] signature job=${SIG_JOB}"

FIND_JOB=$(
  sbatch --parsable \
    --dependency=afterok:${SIG_JOB} \
    --job-name=fw_s4_find \
    --nodes=1 \
    --ntasks=1 \
    --array=0-$((FINDER_WORKERS - 1))%${FINDER_CONCURRENCY} \
    --cpus-per-task="${FINDER_CPUS_PER_TASK}" \
    --mem="${FINDER_MEM_PER_TASK}" \
    --time="${FIND_TIME}" \
    --exclude="${EXCLUDE}" \
    --output="${LOG_ROOT}/%x-%A_%a.out" \
    --error="${LOG_ROOT}/%x-%A_%a.err" \
    --export=ALL,STAGE=find \
    "${WORKER_SCRIPT}"
)
echo "[SUBMIT] find job=${FIND_JOB}"

FILTER_JOB=$(
  sbatch --parsable \
    --dependency=afterok:${FIND_JOB} \
    --job-name=fw_s4_filter \
    --nodes=1 \
    --ntasks=1 \
    --array=0-$((TASKS - 1))%${FILTER_CONCURRENCY} \
    --cpus-per-task="${CPUS_PER_TASK}" \
    --mem="${MEM_PER_TASK}" \
    --time="${FILTER_TIME}" \
    --exclude="${EXCLUDE}" \
    --output="${LOG_ROOT}/%x-%A_%a.out" \
    --error="${LOG_ROOT}/%x-%A_%a.err" \
    --export=ALL,STAGE=filter \
    "${WORKER_SCRIPT}"
)
echo "[SUBMIT] filter job=${FILTER_JOB}"
