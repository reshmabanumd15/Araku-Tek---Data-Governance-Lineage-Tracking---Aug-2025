import pandas as pd
from pathlib import Path
import gzip

def read_inventory(base_dir: str) -> pd.DataFrame:
    base = Path(base_dir)
    frames = []
    for gz in base.rglob('inventory.csv.gz'):
        with gzip.open(gz, 'rt', encoding='utf-8') as f:
            frames.append(pd.read_csv(f))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
