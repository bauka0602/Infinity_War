from .db import db_execute


def recompute_room_availability(connection):
    db_execute(
        connection,
        """
        UPDATE rooms
        SET available = CASE
            WHEN EXISTS (
                SELECT 1
                FROM schedules s
                WHERE s.room_id = rooms.id
            )
            THEN 0
            ELSE 1
        END
        """,
    )
