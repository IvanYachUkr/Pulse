"""Standalone training script for async model retraining.

Called by consumer.py as a subprocess to train models without blocking inference.
Accepts Arrow/Feather shard files via command line or manifest file.
"""

import argparse
import os
import sys
import inspect

# Add project root + outlier_tool to path
_project_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(_project_root, "outlier_tool"))
sys.path.insert(0, _project_root)

from outlier_tool.redset_outlier_lib import OutlierService, TrainingConfig, OnnxExportConfig


def parse_train_paths(s: str) -> list[str]:
    """Parse comma-separated training paths into a deduplicated list.
    
    Args:
        s: Comma-separated string of file paths
    
    Returns:
        List of unique file paths (order preserved)
    """
    paths = [p.strip() for p in (s or "").split(",") if p.strip()]
    # Deduplicate while preserving order
    seen = set()
    out = []
    for p in paths:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def load_paths_from_file(filepath: str) -> list[str]:
    """Load training paths from a manifest file (one path per line)."""
    paths = []
    seen = set()
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            p = line.strip()
            if p and p not in seen:
                seen.add(p)
                paths.append(p)
    return paths


def main() -> int:
    """Entry point for training script.
    
    Returns:
        Exit code (0 = success, 1 = failure)
    """
    parser = argparse.ArgumentParser(
        description="Train outlier detection model from Arrow/Feather shard files"
    )
    parser.add_argument(
        "--train-paths",
        help='Comma-separated list of Arrow/Feather files, e.g. "a.arrow,b.arrow,c.arrow"',
    )
    parser.add_argument(
        "--train-paths-file",
        help='Path to manifest file containing training file paths (one per line)',
    )
    parser.add_argument("--model-dir", required=True, help="Model output directory")
    parser.add_argument("--stage", default="postcompile")
    parser.add_argument("--target", default="execution_duration_ms")
    parser.add_argument("--train-window-days", type=int, default=7)
    parser.add_argument("--max-train-rows", type=int, default=400000)
    parser.add_argument("--model-name-prefix", default="model")
    parser.add_argument("--opset", type=int, default=17)
    parser.add_argument("--fixed-batch-size", type=int, default=0, help="0 = dynamic batch")
    args = parser.parse_args()

    # Load paths from file or direct argument
    if args.train_paths_file:
        if not os.path.isfile(args.train_paths_file):
            print(f"[Trainer] FAILED: manifest file not found: {args.train_paths_file}")
            return 1
        train_paths = load_paths_from_file(args.train_paths_file)
        print(f"[Trainer] Loaded {len(train_paths)} paths from manifest: {args.train_paths_file}")
    elif args.train_paths:
        train_paths = parse_train_paths(args.train_paths)
    else:
        print("[Trainer] FAILED: either --train-paths or --train-paths-file is required")
        return 1

    if not train_paths:
        print("[Trainer] FAILED: no training paths found")
        return 1

    missing = [p for p in train_paths if not os.path.isfile(p)]
    if missing:
        print("[Trainer] FAILED: some training files do not exist:")
        for p in missing[:20]:
            print(f"  - {p}")
        if len(missing) > 20:
            print(f"  ... ({len(missing) - 20} more)")
        return 1

    print(f"[Trainer] Starting training")
    print(f"[Trainer] Model dir: {args.model_dir}")
    print(f"[Trainer] Files: {len(train_paths)}")
    for p in train_paths[:5]:
        print(f"[Trainer]   - {p}")
    if len(train_paths) > 5:
        print(f"[Trainer]   ... ({len(train_paths) - 5} more)")
    print(f"[Trainer] Train window (arg): {args.train_window_days} days")
    print(f"[Trainer] Max rows: {args.max_train_rows:,}")

    svc = OutlierService(model_dir=args.model_dir)

    # Determine which parameter name the retrain method accepts
    retrain_sig = inspect.signature(svc.retrain)
    params = retrain_sig.parameters

    retrain_kwargs = dict(
        training=TrainingConfig(
            stage=args.stage,
            target=args.target,
            train_window_days=args.train_window_days,
            max_train_rows=args.max_train_rows,
            model_name_prefix=args.model_name_prefix,
        ),
        onnx=OnnxExportConfig(
            opset=args.opset,
            fixed_batch_size=None if args.fixed_batch_size == 0 else args.fixed_batch_size,
        ),
        duckdb_table=None,
    )

    try:
        if "train_paths" in params:
            artifact = svc.retrain(train_paths=train_paths, **retrain_kwargs)
        elif "paths" in params:
            artifact = svc.retrain(paths=train_paths, **retrain_kwargs)
        else:
            # No backward compatibility: fail with an actionable message.
            raise TypeError(
                "OutlierService.retrain does not accept a list of input files yet. "
                "Expected parameter `train_paths` (preferred) or `paths`. "
                f"Current signature: {retrain_sig}. "
                "Next step: update OutlierService.retrain (or the function it calls) to accept "
                "the list and read/concat shards."
            )

        print(f"[Trainer] SUCCESS! Model tag: {artifact.tag}")
        print(f"[Trainer] Saved to: {args.model_dir}")
        return 0

    except Exception as e:
        print(f"[Trainer] FAILED: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
