import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

from nc_tables_api_wrapper.fetch_table import fetch_table_data
from nc_tables_api_wrapper.table_properties import list_tables

load_dotenv()

BACKUP_PATH = os.getenv("BACKUP_PATH")
KEEP_N_BACKUPS = int(os.getenv("KEEP_N_BACKUPS", "5"))
if not BACKUP_PATH:
    raise ValueError("BACKUP_PATH environment variable is not set.")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

list_of_tables = list_tables()
print(f"Found {len(list_of_tables)} tables to back up.")

for table in list_of_tables:
    table_id = table.get("id")
    if table_id is None:
        raise ValueError("Table without ID found.")
    table_id = int(table_id)
    table_title = table.get("title", f"table_{table_id}")
    print(f"\n-----\nBacking up table ID {table_id}: '{table_title}'")

    # sanitize table_title for filesystem
    sanitized_title = "".join(
        c for c in table_title if c.isalnum() or c in (" ", "_", "-")
    ).rstrip()

    backup_file_path = (
        Path(BACKUP_PATH)
        / f"table_{table_id}_{sanitized_title}"
        / f"{timestamp}_{table_id}_{sanitized_title}.csv"
    )

    # ensure directory exists
    backup_file_path.parent.mkdir(parents=True, exist_ok=True)

    # fetch table data
    table_data = fetch_table_data(table_id=table_id, explode=False)

    # save to CSV
    table_data.to_csv(backup_file_path, index=False)
    print(f"Backup of table ID {table_id} saved to {backup_file_path}")

    # delete old backups, keep only the N most recent ones per table as
    # per KEEP_N_BACKUPS environment variable
    backup_dir = backup_file_path.parent
    backup_files = sorted(backup_dir.glob("*.csv"), key=os.path.getmtime)
    for old_backup in backup_files[:-KEEP_N_BACKUPS]:
        os.remove(old_backup)
        print(f"Deleted old backup: {old_backup}")
