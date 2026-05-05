from datetime import datetime

from sqlalchemy import or_, select, update

from ..core.orm import SessionLocal
from ..models import Room, RoomBlock


def recompute_room_availability(connection):
    # Room availability is a global on/off flag.
    # Slot-level occupancy is derived from schedules and room_blocks separately.
    with SessionLocal() as session:
        session.execute(
            update(Room)
            .where(Room.available.is_(None))
            .values(available=1)
        )
        session.commit()


def normalize_room_block_day(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw).strftime("%A")
    except ValueError:
        return raw


def get_room_blocked_slots(connection, semester=None, year=None):
    conditions = []
    if semester is not None:
        conditions.append(or_(RoomBlock.semester == semester, RoomBlock.semester.is_(None)))
    if year is not None:
        conditions.append(or_(RoomBlock.year == year, RoomBlock.year.is_(None)))
    statement = select(
        RoomBlock.room_id.label("room_id"),
        RoomBlock.day.label("day"),
        RoomBlock.start_hour.label("start_hour"),
        RoomBlock.end_hour.label("end_hour"),
    )
    if conditions:
        statement = statement.where(*conditions)
    with SessionLocal() as session:
        rows = session.execute(
            statement.order_by(RoomBlock.room_id, RoomBlock.day, RoomBlock.start_hour)
        ).mappings().all()
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
