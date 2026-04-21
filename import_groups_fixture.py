import json
from pathlib import Path

from app.db import ensure_database, get_connection
from app.import_service import _upsert_group


FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "real_groups_2025.json"


def main():
    ensure_database()
    groups = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    summary = {"inserted": 0, "updated": 0}

    with get_connection() as connection:
        for group in groups:
            result = _upsert_group(connection, group)
            summary[result] += 1
        connection.commit()

    print(f"Imported groups: {summary['inserted']} inserted, {summary['updated']} updated")


if __name__ == "__main__":
    main()
