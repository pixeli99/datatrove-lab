#!/usr/bin/env python3
"""
Stage 4: exact deduplication for Stage 3 quality parquet output.

This reads scored parquet files, deduplicates by text, and writes parquet files
with the original quality columns plus duplicate_count for kept rows.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq

from datatrove.data import Document
from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.dedup.exact_dedup import (
    ExactDedupConfig,
    ExactDedupFilter,
    ExactDedupSignature,
    ExactFindDedups,
)
from datatrove.pipeline.readers.parquet import ParquetReader
from datatrove.pipeline.writers.parquet import ParquetWriter
from datatrove.utils.hashing import HashConfig


_WS_RE = re.compile(r"\s+")


@dataclass
class TextContentGetter:
    normalize_whitespace: bool = False

    def __call__(self, doc: Document) -> str:
        text = doc.text or ""
        if self.normalize_whitespace:
            text = _WS_RE.sub(" ", text)
        return text.strip()


class QualityPriority:
    """Prefer higher quality score, then longer text, while staying in uint16."""

    def __call__(self, doc: Document) -> int:
        score = str(doc.metadata.get("stage3_score", "")).strip()
        score_rank = {"0": 1, "1": 2, "2": 3}.get(score, 0)
        length_bonus = min(5000, len(doc.text or "") // 20)
        return max(1, min(65535, score_rank * 20000 + length_bonus))


def quality_output_adapter(self, doc: Document) -> dict:
    return {
        "text": doc.text,
        "meta": doc.metadata.get("meta"),
        "stage1_category": doc.metadata.get("stage1_category", ""),
        "stage3_score": str(doc.metadata.get("stage3_score", "")),
        "stage3_reason": str(doc.metadata.get("stage3_reason", "")),
        "duplicate_count": int(doc.metadata.get("duplicate_count", 0)),
    }


def find_first_parquet(input_root: str) -> str:
    if os.path.isfile(input_root):
        if input_root.endswith(".parquet"):
            return input_root
        raise ValueError(f"input_root is a file but not parquet: {input_root}")

    for root, _, files in os.walk(input_root):
        for name in sorted(files):
            if name.endswith(".parquet") and not name.endswith(".tmp"):
                return os.path.join(root, name)
    raise RuntimeError(f"No parquet files found under {input_root}")


def build_output_schema(input_root: str) -> pa.Schema:
    input_schema = pq.read_schema(find_first_parquet(input_root))
    fields: list[pa.Field] = []

    def field_or(name: str, dtype: pa.DataType) -> pa.Field:
        return input_schema.field(name) if name in input_schema.names else pa.field(name, dtype)

    fields.append(field_or("text", pa.large_string()))
    fields.append(field_or("meta", pa.string()))
    fields.append(field_or("stage1_category", pa.string()))
    fields.append(field_or("stage3_score", pa.string()))
    fields.append(field_or("stage3_reason", pa.string()))
    fields.append(pa.field("duplicate_count", pa.int64()))
    return pa.schema(fields)


def get_reader_root_and_glob(input_root: str) -> tuple[str, str]:
    if os.path.isfile(input_root):
        return os.path.dirname(input_root) or ".", os.path.basename(input_root)
    return input_root, "**/*.parquet"


def clean_previous_outputs(args) -> None:
    if not args.overwrite:
        return
    for path in [args.output_root, args.work_root]:
        if os.path.exists(path):
            shutil.rmtree(path)


def make_reader(reader_root: str, glob_pattern: str, batch_size: int, paths_file: str | None = None) -> ParquetReader:
    return ParquetReader(
        data_folder=reader_root,
        paths_file=paths_file,
        batch_size=batch_size,
        recursive=True,
        glob_pattern=glob_pattern,
        shuffle_files=False,
    )


def build_config(args) -> ExactDedupConfig:
    return ExactDedupConfig(
        content_getter=TextContentGetter(normalize_whitespace=args.normalize_whitespace),
        document_priority=QualityPriority(),
        hash_config=HashConfig(precision=64, hash_fc=args.hash_fc),
    )


def resolve_rank(args, default_world_size: int) -> tuple[int, int]:
    rank = args.rank
    if rank is None:
        rank = int(os.environ.get("SLURM_ARRAY_TASK_ID", "0"))
    world_size = args.world_size or default_world_size
    if rank < 0 or rank >= world_size:
        raise ValueError(f"rank must be in [0, {world_size}), got {rank}")
    return rank, world_size


def run_signature_stage(args) -> None:
    reader_root, glob_pattern = get_reader_root_and_glob(args.input_root)
    config = build_config(args)
    rank, world_size = resolve_rank(args, args.tasks)
    reader = make_reader(reader_root, glob_pattern, args.read_batch_size, args.paths_file)
    signature = ExactDedupSignature(
        output_folder=os.path.join(args.work_root, "sigs"),
        config=config,
        finder_workers=args.finder_workers,
    )
    signature(data=reader(rank=rank, world_size=world_size), rank=rank, world_size=world_size)
    print(f"signature done rank={rank}/{world_size}", flush=True)


def run_find_stage(args) -> None:
    config = build_config(args)
    rank, world_size = resolve_rank(args, args.finder_workers)
    finder = ExactFindDedups(
        data_folder=os.path.join(args.work_root, "sigs"),
        output_folder=os.path.join(args.work_root, "dups"),
        config=config,
        save_cluster_size=True,
        lines_to_buffer=1000,
    )
    finder(rank=rank, world_size=world_size)
    print(f"find done rank={rank}/{world_size}", flush=True)


def run_filter_stage(args) -> None:
    reader_root, glob_pattern = get_reader_root_and_glob(args.input_root)
    config = build_config(args)
    rank, world_size = resolve_rank(args, args.tasks)
    output_schema = build_output_schema(args.input_root)
    reader = make_reader(reader_root, glob_pattern, args.read_batch_size, args.paths_file)
    dedup_filter = ExactDedupFilter(
        data_folder=os.path.join(args.work_root, "dups"),
        config=config,
        exclusion_writer=ParquetWriter(
            output_folder=os.path.join(args.output_root, "removed"),
            adapter=quality_output_adapter,
            batch_size=args.write_batch_size,
            schema=output_schema,
        ),
    )
    writer = ParquetWriter(
        output_folder=args.output_root,
        adapter=quality_output_adapter,
        batch_size=args.write_batch_size,
        schema=output_schema,
    )
    data = reader(rank=rank, world_size=world_size)
    filtered = dedup_filter(data=data, rank=rank, world_size=world_size)
    for _ in writer(data=filtered, rank=rank, world_size=world_size):
        pass
    print(f"filter done rank={rank}/{world_size}", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Stage 4: exact deduplication for scored parquet files")
    ap.add_argument("--input-root", required=True, help="Stage 3 parquet file or directory")
    ap.add_argument("--output-root", required=True, help="Directory for deduplicated parquet output")
    ap.add_argument("--work-root", required=True, help="Directory for signatures and duplicate indexes")
    ap.add_argument("--stage", choices=["all", "signature", "find", "filter"], default="all")
    ap.add_argument("--rank", type=int, default=None)
    ap.add_argument("--world-size", type=int, default=None)
    ap.add_argument("--paths-file", default=None, help="Optional file with parquet paths relative to input-root")
    ap.add_argument("--tasks", type=int, default=64, help="Reader/filter task count. Must match for stage 1 and 3")
    ap.add_argument("--workers", type=int, default=64, help="Local workers for signature and filter stages")
    ap.add_argument("--finder-workers", type=int, default=64, help="Task count for duplicate finding stage")
    ap.add_argument("--read-batch-size", type=int, default=1000)
    ap.add_argument("--write-batch-size", type=int, default=1000)
    ap.add_argument("--normalize-whitespace", action="store_true", help="Collapse whitespace before hashing text")
    ap.add_argument("--hash-fc", choices=["xxhash", "sha1"], default="xxhash")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    if args.tasks <= 0 or args.workers <= 0 or args.finder_workers <= 0:
        raise ValueError("tasks, workers, and finder-workers must be positive")

    os.makedirs(args.output_root, exist_ok=True)
    os.makedirs(args.work_root, exist_ok=True)

    if args.stage == "signature":
        run_signature_stage(args)
        return
    if args.stage == "find":
        run_find_stage(args)
        return
    if args.stage == "filter":
        run_filter_stage(args)
        return

    clean_previous_outputs(args)
    os.makedirs(args.output_root, exist_ok=True)
    os.makedirs(args.work_root, exist_ok=True)

    sigs_dir = os.path.join(args.work_root, "sigs")
    dups_dir = os.path.join(args.work_root, "dups")
    removed_dir = os.path.join(args.output_root, "removed")
    logs_dir = os.path.join(args.work_root, "logs")

    config = build_config(args)

    reader_root, glob_pattern = get_reader_root_and_glob(args.input_root)
    output_schema = build_output_schema(args.input_root)

    stage1 = LocalPipelineExecutor(
        pipeline=[
            make_reader(reader_root, glob_pattern, args.read_batch_size, args.paths_file),
            ExactDedupSignature(output_folder=sigs_dir, config=config, finder_workers=args.finder_workers),
        ],
        tasks=args.tasks,
        workers=args.workers,
        logging_dir=os.path.join(logs_dir, "stage1_sigs"),
    )

    stage2 = LocalPipelineExecutor(
        pipeline=[
            ExactFindDedups(
                data_folder=sigs_dir,
                output_folder=dups_dir,
                config=config,
                save_cluster_size=True,
                lines_to_buffer=1000,
            )
        ],
        tasks=args.finder_workers,
        workers=args.finder_workers,
        depends=stage1,
        logging_dir=os.path.join(logs_dir, "stage2_dups"),
    )

    stage3 = LocalPipelineExecutor(
        pipeline=[
            make_reader(reader_root, glob_pattern, args.read_batch_size, args.paths_file),
            ExactDedupFilter(
                data_folder=dups_dir,
                config=config,
                exclusion_writer=ParquetWriter(
                    output_folder=removed_dir,
                    adapter=quality_output_adapter,
                    batch_size=args.write_batch_size,
                    schema=output_schema,
                ),
            ),
            ParquetWriter(
                output_folder=args.output_root,
                adapter=quality_output_adapter,
                batch_size=args.write_batch_size,
                schema=output_schema,
            ),
        ],
        tasks=args.tasks,
        workers=args.workers,
        depends=stage2,
        logging_dir=os.path.join(logs_dir, "stage3_filter"),
    )

    stage3.run()


if __name__ == "__main__":
    main()
