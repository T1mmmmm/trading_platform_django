import hashlib
from pathlib import Path
import pandas as pd

def compute_sha256_bytes(b: bytes) -> str:
    h = hashlib.sha256()
    h.update(b)
    return "sha256:" + h.hexdigest()

def normalize_and_profile_csv(raw_path: Path, timestamp_col: str, target_col: str):
    df = pd.read_csv(raw_path)

    if timestamp_col not in df.columns or target_col not in df.columns:
        raise ValueError(f"Missing columns. Need timestamp={timestamp_col}, target={target_col}")

    df = df[[timestamp_col, target_col]].copy()
    df.rename(columns={timestamp_col: "timestamp", target_col: "target"}, inplace=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    before = len(df)
    df = df.sort_values("timestamp")
    df = df.drop_duplicates(subset=["timestamp"], keep="last")
    dup_removed = before - len(df)

    missing_rate = float(df["target"].isna().mean())
    tmin = df["timestamp"].min()
    tmax = df["timestamp"].max()

    profile = {
        "rowCount": int(len(df)),
        "timeRangeStart": tmin.isoformat() if pd.notna(tmin) else None,
        "timeRangeEnd": tmax.isoformat() if pd.notna(tmax) else None,
        "missingRate": missing_rate,
        "dupRemoved": int(dup_removed),
    }

    processed_bytes = df.to_csv(index=False).encode("utf-8")
    checksum = compute_sha256_bytes(processed_bytes)
    return df, profile, checksum
