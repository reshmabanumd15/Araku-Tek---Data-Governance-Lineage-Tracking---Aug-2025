import csv, datetime
from pathlib import Path

def log_action(log_path: str, actor: str, action: str, entity: str, details: str=''):
    p = Path(log_path); p.parent.mkdir(parents=True, exist_ok=True)
    exists = p.exists()
    with open(p, 'a', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        if not exists: w.writerow(['ts','actor','action','entity','details'])
        w.writerow([datetime.datetime.utcnow().isoformat(), actor, action, entity, details])
