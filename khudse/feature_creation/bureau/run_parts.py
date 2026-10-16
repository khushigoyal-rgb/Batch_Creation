"""
Parallel driver for the bureau feature pipeline.

Steps:
  1. (optional) run split_ids.py once to create split_ids/part_<N>.csv.
  2. run feat_func.py once per part, as SEPARATE processes (true parallelism,
     no GIL), capped at --workers so RAM/CPU stay bounded. Each part runs in
     its own working dir (feat_func.py chdir's into runs/part_<N>), so the
     intermediate feature files never collide.
  3. (optional) combine all part_<N>_Final_features.parquet into one file.

Why processes (not threads): the feature code is CPU-bound pure-Python
(.apply loops); threads would be serialized by the GIL and only multiply RAM.

Usage:
  python run_parts.py --workers 4
  python run_parts.py --workers 4 --no-split          # reuse existing split_ids
  python run_parts.py --workers 4 --parts 1 3 5        # only these parts
  python run_parts.py --workers 4 --no-combine         # skip the final concat
"""
import argparse
import glob
import os
import re
import subprocess
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed

_BUREAU_DIR = os.path.dirname(os.path.abspath(__file__))
SPLIT_IDS_DIR = os.path.join(_BUREAU_DIR, "split_ids")
FINAL_FEATURES_DIR = os.path.join(_BUREAU_DIR, "data", "features")
FEAT_FUNC = os.path.join(_BUREAU_DIR, "feat_func.py")
SPLIT_SCRIPT = os.path.join(_BUREAU_DIR, "split_ids.py")


def discover_parts():
    """Part numbers present as split_ids/part_<N>.csv, sorted ascending."""
    parts = []
    for path in glob.glob(os.path.join(SPLIT_IDS_DIR, "part_*.csv")):
        m = re.search(r"part_(\d+)\.csv$", os.path.basename(path))
        if m:
            parts.append(int(m.group(1)))
    return sorted(parts)


def run_one_part(part):
    """Run feat_func.py for a single part as a subprocess. Returns (part, rc)."""
    env = dict(os.environ, PART=str(part))
    log_path = os.path.join(_BUREAU_DIR, "runs", f"part_{part}.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w") as log:
        rc = subprocess.call([sys.executable, FEAT_FUNC], env=env,
                             stdout=log, stderr=subprocess.STDOUT)
    return part, rc


def combine_parts():
    """Stream all part_<N>_Final_features.parquet into one parquet.

    Streaming (row-group append) keeps peak memory ~= ONE part, not the whole
    combined dataset: we read one part, write it as parquet row-groups, free it,
    then move to the next. Columns are aligned to the first part's schema so a
    part missing/extra columns doesn't break the single-schema parquet writer.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    files = sorted(glob.glob(os.path.join(FINAL_FEATURES_DIR, "part_*_Final_features.parquet")))
    if not files:
        print("combine: no part_*_Final_features.parquet found, skipping.")
        return None

    out_path = os.path.join(FINAL_FEATURES_DIR, "all_parts_Final_features.parquet")
    writer = None
    ref_columns = None
    total_rows = 0
    try:
        for f in files:
            df = pd.read_parquet(f)
            if ref_columns is None:
                ref_columns = list(df.columns)          # lock schema from first part
            else:
                df = df.reindex(columns=ref_columns)     # align (missing -> NaN)
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema)
            else:
                table = table.cast(writer.schema)        # keep schema identical
            writer.write_table(table)                    # append as row-group(s)
            total_rows += len(df)
            print(f"combine: appended {os.path.basename(f)} ({len(df)} rows)")
            del df, table
    finally:
        if writer is not None:
            writer.close()

    print(f"combine: wrote {out_path} ({total_rows} rows, {len(ref_columns)} cols) from {len(files)} parts")
    return out_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=2,
                    help="max parts to run in parallel (tune to free RAM/cores)")
    ap.add_argument("--parts", type=int, nargs="*", default=None,
                    help="specific part numbers to run (default: all discovered)")
    ap.add_argument("--no-split", action="store_true",
                    help="skip split_ids.py (reuse existing split_ids/part_*.csv)")
    ap.add_argument("--no-combine", action="store_true",
                    help="skip combining the final part outputs")
    args = ap.parse_args()

    if not args.no_split:
        print("=== splitting IDs ===")
        rc = subprocess.call([sys.executable, SPLIT_SCRIPT])
        if rc != 0:
            sys.exit(f"split_ids.py failed with code {rc}")

    parts = args.parts if args.parts else discover_parts()
    if not parts:
        sys.exit("no parts found in split_ids/ (did the split step run?)")
    print(f"=== running {len(parts)} part(s) {parts} with workers={args.workers} ===")

    failures = []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(run_one_part, p): p for p in parts}
        for fut in as_completed(futures):
            part, rc = fut.result()
            status = "OK" if rc == 0 else f"FAILED (rc={rc})"
            print(f"  part_{part}: {status}  (log: runs/part_{part}.log)")
            if rc != 0:
                failures.append(part)

    if failures:
        sys.exit(f"parts failed: {failures} -- check runs/part_<N>.log")

    if not args.no_combine:
        print("=== combining ===")
        combine_parts()

    print("=== done ===")


if __name__ == "__main__":
    main()
