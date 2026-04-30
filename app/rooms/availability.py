from datetime import datetime

from ..core.db import db_execute, query_all


def recompute_room_availability(connection):
    # Room availability is a global on/off flag.
    # Slot-level occupancy is derived from schedules and room_blocks separately.
    db_execute(
        connection,
        """
        UPDATE rooms
        SET available = CASE
            WHEN available IS NULL THEN 1
            ELSE available
        END
        """,
    )


def normalize_room_block_day(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw).strftime("%A")
    except ValueError:
        return raw


def get_room_blocked_slots(connection, semester=None, year=None):
    clauses = []
    params = []
    if semester is not None:
        clauses.append("(semester = ? OR semester IS NULL)")
        params.append(semester)
    if year is not None:
        clauses.append("(year = ? OR year IS NULL)")
        params.append(year)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = query_all(
        connection,
        f"""
        SELECT room_id, day, start_hour, end_hour
        FROM room_blocks
        {where_sql}
        ORDER BY room_id, day, start_hour
        """,
        tuple(params),
    )
    blocked_by_room = {}
    for row in rows:
        room_id = row.get("room_id")
        day = normalize_room_block_day(row.get("day"))
        start_hour = row.get("start_hour")
        end_hour = row.get("end_hour")
        if not room_id or not day or start_hour in (None, ""):
            continue
        start_value = int(start_hour)
        end_value = int(end_hour) if end_hour not in (None, "") else start_value + 1
        if end_value <= start_value:
            end_value = start_value + 1
        room_slots = blocked_by_room.setdefault(room_id, set())
        for hour in range(start_value, end_value):
            room_slots.add((day, hour))
    return blocked_by_room
