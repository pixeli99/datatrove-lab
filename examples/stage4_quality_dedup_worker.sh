#!/bin/bash
# Worker script submitted by run_stage4_quality_dedup.sh.

set -euo pipefail

: "${STAGE:?STAGE must be signature, find, or filter}"

DATATROVE_ROOT="${DATATROVE_ROOT:-/lustre/projects/polyullm/lipengxiang_tmp/datatrove}"
INPUT_ROOT="${INPUT_ROOT:-/work/projects/polyullm/lipengxiang_tmp/fineweb_012}"
OUTPUT_ROOT="${OUTPUT_ROOT:-/work/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup}"
WORK_ROOT="${WORK_ROOT:-/work/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_work}"
PATHS_FILE="${PATHS_FILE:-${WORK_ROOT}/input_paths.txt}"
PY_SCRIPT="${PY_SCRIPT:-${DATATROVE_ROOT}/examples/stage4_quality_exact_dedup.py}"

IMAGE="${IMAGE:-/lustre/projects/polyullm/pretrain/container/datatrove.sqsh}"
MOUNTS="${MOUNTS:-/work/projects/polyullm:/work/projects/polyullm,/lustre/projects/polyullm:/lustre/projects/polyullm}"
HOME_DIR="${HOME_DIR:-/work/projects/polyullm/lipengxiang}"
TASKS="${TASKS:-4096}"
FINDER_WORKERS="${FINDER_WORKERS:-256}"
HASH_FC="${HASH_FC:-xxhash}"

RANK="${SLURM_ARRAY_TASK_ID:-0}"
if [[ "${STAGE}" == "find" ]]; then
  WORLD_SIZE="${FINDER_WORKERS}"
else
  WORLD_SIZE="${TASKS}"
fi

echo "[WORKER] stage=${STAGE} rank=${RANK}/${WORLD_SIZE}"
echo "[WORKER] input=${INPUT_ROOT}"
echo "[WORKER] output=${OUTPUT_ROOT}"
echo "[WORKER] work=${WORK_ROOT}"
echo "[WORKER] paths_file=${PATHS_FILE}"

srun --nodes=1 --ntasks=1 \
  --container-name=datatrove_s4_${STAGE}_${SLURM_JOB_ID}_${RANK} \
  --container-image="${IMAGE}" \
  --container-mounts="${MOUNTS}" \
  --container-writable \
  bash -lc "
    set -euo pipefail
    export HOME='${HOME_DIR}'
    export PYTHONPATH='${DATATROVE_ROOT}/src:'\"\${PYTHONPATH:-}\"
    cd '${DATATROVE_ROOT}'
    python '${PY_SCRIPT}' \
      --stage '${STAGE}' \
      --rank '${RANK}' \
      --world-size '${WORLD_SIZE}' \
      --input-root '${INPUT_ROOT}' \
      --output-root '${OUTPUT_ROOT}' \
      --work-root '${WORK_ROOT}' \
      --paths-file '${PATHS_FILE}' \
      --tasks '${TASKS}' \
      --finder-workers '${FINDER_WORKERS}' \
      --hash-fc '${HASH_FC}' \
      --normalize-whitespace
  "
