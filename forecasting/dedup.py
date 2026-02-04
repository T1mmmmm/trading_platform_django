import hashlib
import json
from pathlib import Path
from typing import Any, Dict

def file_checksum_sha256(path: str) -> str:
    p = Path(path)
    if not p.exists():
        raise ValueError(f"csvPath not found: {path}")
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def normalize_params(model_type: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params = params or {}

    if model_type == "MA":
        csv_path = params.get("csvPath")
        if not csv_path:
            raise ValueError("Missing params.csvPath")

        window = int(params.get("window", 20))
        target_col = params.get("targetColumn", "Close")

        return {
            "csvPath": csv_path,
            "window": window,
            "targetColumn": target_col,
        }

    # fallback: deterministic JSON (still stable)
    return params

def build_dedup_key(data_checksum: str, model_type: str, normalized_params: Dict[str, Any], horizon: int) -> str:
    normalized_params_str = json.dumps(normalized_params, sort_keys=True, separators=(",", ":"))
    raw = f"{data_checksum}|{model_type}|{normalized_params_str}|{horizon}"
    return "dd_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()
