# parser.py — entrypoint: discover .py files in target-repo, parse each, print summary.

import sys
import subprocess
import traceback
import argparse
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent.parent   # repo root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from kafka_producer import CPGProducer
from cpg_visitor import parse_source, _now_iso
from discover_files import discover_files


TARGET = ROOT / "target-repo"
REPO_NAME = "huggingface/transformers-pr-agent"

def git_commit(repo_path: Path) -> str:
    # short commit hash of target-repo, fallback "dev"
    try:
        out = subprocess.check_output(
            ["git", "-C", str(repo_path), "rev-parse", "--short", "HEAD"],
            text=True, stderr=subprocess.DEVNULL,
        )
        return out.strip()
    except Exception:
        return "dev"


def error_event(file_path: str, exc: Exception, repo_commit: str) -> dict:
    return {
        "schema_version": "v1",
        "event_timestamp": _now_iso(),
        "file_path": file_path,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "stack_trace": traceback.format_exc()[-2000:],   # truncate to keep event small
        "repo_commit": repo_commit,
    }


def run(limit=None, producer=None):
    repo_commit = git_commit(TARGET)
    files = discover_files(TARGET)
    if limit:
        files = files[:limit]   # limit for demo

    stats = Counter()       # files: ok, errors, nodes, edges
    edge_types = Counter()
    for fp in files:
        rel = str(fp.relative_to(TARGET)).replace("\\", "/")
        try:
            source = fp.read_text(encoding="utf-8")
            nodes, edges, meta = parse_source(source, rel, repo_commit, REPO_NAME)
            if producer:
                producer.publish_nodes(nodes)
                producer.publish_edges(edges)
                producer.publish_metadata(meta)
            stats["files_ok"] += 1
            stats["nodes"] += len(nodes)
            stats["edges"] += len(edges)
            edge_types.update(e["edge_type"] for e in edges)
        except Exception as exc:                 # 1 bad file must not stop the rest
            err = error_event(rel, exc, repo_commit)
            if producer:
                producer.publish_errors(err)
            stats["errors"] += 1
    if producer:
        producer.flush()
    return stats, edge_types

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="only first N files (demo)")
    ap.add_argument("--publish", action="store_true", help="publish events to Kafka")
    ap.add_argument("--bootstrap", default="localhost:9092")
    args = ap.parse_args()

    producer = None
    if args.publish:
        producer = CPGProducer(args.bootstrap)

    stats, edge_types = run(limit=args.limit, producer=producer)
    if producer:
        producer.close()

    print("Parser Service — Task 2")
    print(f"Files parsed OK : {stats['files_ok']}")
    print(f"Parse errors    : {stats['errors']}")
    print(f"Nodes           : {stats['nodes']}")
    print(f"Edges           : {stats['edges']}  {dict(edge_types)}")
    print(f"Published       : {'YES -> Kafka' if producer else 'no (summary only)'}")