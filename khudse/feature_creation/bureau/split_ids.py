"""
Split the cohort's bureau IDs into 50k-row chunks: split_ids/part_<N>.csv

Run ONCE, before the per-part feature runs (feat_func.py). Keeping the split
here (instead of inside feat_func.py) means parallel part-workers don't each
redo the Inquiry read + split.

Reads : <repo>/Output/Inquiry.csv   (produced by base_selection/scrub_data.py)
Writes: feature_creation/bureau/split_ids/part_1.csv, part_2.csv, ...
"""
import os
import pandas as pd

_BUREAU_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.abspath(os.path.join(_BUREAU_DIR, "..", ".."))

SCRUB_DIR = os.path.join(_REPO_ROOT, "Output")
SPLIT_IDS_DIR = os.path.join(_BUREAU_DIR, "split_ids")
CHUNK_SIZE = 50000


def main():
    os.makedirs(SPLIT_IDS_DIR, exist_ok=True)

    inquiry_path = os.path.join(SCRUB_DIR, "Inquiry.csv")
    df_ids = pd.read_csv(inquiry_path, sep="|", usecols=["RETRO-PR-DT", "LOS-APP-ID", "DOB"])
    print("df_ids (Inquiry) shape:", df_ids.shape)

    n_parts = 0
    for start in range(0, df_ids.shape[0], CHUNK_SIZE):
        n_parts += 1
        chunk = df_ids.iloc[start:start + CHUNK_SIZE]
        out_path = os.path.join(SPLIT_IDS_DIR, f"part_{n_parts}.csv")
        chunk.to_csv(out_path, index=False)
        print(f"wrote {out_path}: {chunk.shape}")

    print(f"total parts written: {n_parts}")
    return n_parts


if __name__ == "__main__":
    main()
