from __future__ import annotations

import json
from datetime import datetime, timezone

from ..auth.service import require_auth_user
from ..core.db import db_execute, get_connection, insert_and_get_id, query_all, query_one
from ..core.errors import ApiError
from ..schedule.time_slots import format_lesson_time_range


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _normalize_subgroup(value):
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"A", "B"} else ""


def _recipient_key(role, recipient_id):
    return f"{role}:{int(recipient_id)}"


def _fetch_student_recipients(connection, schedule_item):
    group_id = schedule_item.get("group_id")
    if not group_id:
        return []

    subgroup = _normalize_subgroup(schedule_item.get("subgroup"))
    if subgroup:
        return query_all(
            connection,
            """
            SELECT id, name
            FROM students
            WHERE group_id = ? AND upper(coalesce(subgroup, '')) = ?
            ORDER BY id
            """,
            (group_id, subgroup),
        )

    return query_all(
        connection,
        """
        SELECT id, name
        FROM students
        WHERE group_id = ?
        ORDER BY id
        """,
        (group_id,),
    )


def _build_recipients_for_schedule(connection, schedule_item):
    recipients = {}

    teacher_id = schedule_item.get("teacher_id")
    if teacher_id:
        recipients[_recipient_key("teacher", teacher_id)] = {
            "role": "teacher",
            "id": int(teacher_id),
            "name": schedule_item.get("teacher_name", ""),
        }

    for student in _fetch_student_recipients(connection, schedule_item):
        recipients[_recipient_key("student", student["id"])] = {
            "role": "student",
            "id": int(student["id"]),
            "name": student.get("name", ""),
        }

    return recipients


def _schedule_signature(schedule_item):
    return (
        schedule_item.get("section_id"),
        schedule_item.get("course_id"),
        schedule_item.get("course_name", ""),
        schedule_item.get("teacher_id"),
        schedule_item.get("teacher_name", ""),
        schedule_item.get("room_id"),
        schedule_item.get("room_number", ""),
        schedule_item.get("group_id"),
        schedule_item.get("group_name", ""),
        _normalize_subgroup(schedule_item.get("subgroup")),
        schedule_item.get("day", ""),
        int(schedule_item.get("start_hour") or 0),
        schedule_item.get("semester"),
        schedule_item.get("year"),
    )


def _format_schedule_brief(schedule_item):
    subgroup = _normalize_subgroup(schedule_item.get("subgroup"))
    subgroup_label = f", subgroup {subgroup}" if subgroup else ""
    return (
        f"{schedule_item.get('course_name', 'Unknown course')} | "
        f"{schedule_item.get('day', '')} {format_lesson_time_range(schedule_item.get('start_hour', 0))} | "
        f"room {schedule_item.get('room_number', '')} | "
        f"group {schedule_item.get('group_name', '')}{subgroup_label}"
    )


def _collect_snapshots_by_recipient(connection, schedule_items):
    snapshots = {}
    recipient_info = {}

    for item in schedule_items:
        signature = _schedule_signature(item)
        recipients = _build_recipients_for_schedule(connection, item)
        for key, recipient in recipients.items():
            recipient_info[key] = recipient
            snapshots.setdefault(key, set()).add(signature)

    return snapshots, recipient_info


def _insert_notification(
    connection,
    recipient_role,
    recipient_id,
    title,
    message,
    notification_type,
    metadata=None,
):
    notification_id = insert_and_get_id(
        connection,
        """
        INSERT INTO notifications (
            recipient_role, recipient_id, title, message, metadata, notification_type, is_read, created_at, read_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            recipient_role,
            recipient_id,
            title,
            message,
            json.dumps(metadata or {}, ensure_ascii=False),
            notification_type,
            0,
            _utc_now_iso(),
            None,
        ),
    )
    return query_one(
        connection,
        """
        SELECT id, recipient_role, recipient_id, title, message, metadata, notification_type, is_read, created_at, read_at
        FROM notifications
        WHERE id = ?
        """,
        (notification_id,),
    )


def create_schedule_change_notifications(connection, before_item=None, after_item=None):
    before_item = before_item or None
    after_item = after_item or None
    if before_item is None and after_item is None:
        return []

    before_snapshots, before_recipient_info = _collect_snapshots_by_recipient(
        connection,
        [before_item] if before_item else [],
    )
    after_snapshots, after_recipient_info = _collect_snapshots_by_recipient(
        connection,
        [after_item] if after_item else [],
    )

    created = []
    all_recipient_keys = set(before_snapshots) | set(after_snapshots)
    for key in all_recipient_keys:
        if before_snapshots.get(key, set()) == after_snapshots.get(key, set()):
            continue

        recipient = after_recipient_info.get(key) or before_recipient_info.get(key)
        if recipient is None:
            continue

        title = "Расписание изменено"
        if before_item and after_item:
            message = (
                "В вашем расписании обновлена пара. "
                f"Было: {_format_schedule_brief(before_item)}. "
                f"Стало: {_format_schedule_brief(after_item)}."
            )
        elif after_item:
            message = f"В ваше расписание добавлена пара: {_format_schedule_brief(after_item)}."
        else:
            message = f"Из вашего расписания удалена пара: {_format_schedule_brief(before_item)}."

        created.append(
            _insert_notification(
                connection,
                recipient["role"],
                recipient["id"],
                title,
                message,
                "schedule_changed",
                {
                    "before": before_item,
                    "after": after_item,
                },
            )
        )

    if created:
        connection.commit()
    return created


def create_schedule_regeneration_notifications(connection, semester, year, before_items, after_items):
    before_snapshots, before_recipient_info = _collect_snapshots_by_recipient(connection, before_items)
    after_snapshots, after_recipient_info = _collect_snapshots_by_recipient(connection, after_items)

    created = []
    all_recipient_keys = set(before_snapshots) | set(after_snapshots)
    for key in all_recipient_keys:
        if before_snapshots.get(key, set()) == after_snapshots.get(key, set()):
            continue

        recipient = after_recipient_info.get(key) or before_recipient_info.get(key)
        if recipient is None:
            continue

        created.append(
            _insert_notification(
                connection,
                recipient["role"],
                recipient["id"],
                "Расписание обновлено",
                f"Ваше расписание изменилось после перегенерации за {semester} семестр {year} года.",
                "schedule_regenerated",
                {
                    "semester": semester,
                    "year": year,
                },
            )
        )

    if created:
        connection.commit()
    return created


def list_notifications(headers):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        return {"items": [], "unreadCount": 0}

    with get_connection() as connection:
        items = query_all(
            connection,
            """
            SELECT id, recipient_role, recipient_id, title, message, metadata, notification_type, is_read, created_at, read_at
            FROM notifications
            WHERE recipient_role = ? AND recipient_id = ?
            ORDER BY created_at DESC, id DESC
            """,
            (user["role"], user["id"]),
        )
        unread_count = sum(1 for item in items if not int(item.get("is_read") or 0))
    return {"items": items, "unreadCount": unread_count}


def mark_notification_as_read(headers, notification_id):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with get_connection() as connection:
        existing = query_one(
            connection,
            """
            SELECT id, recipient_role, recipient_id, title, message, metadata, notification_type, is_read, created_at, read_at
            FROM notifications
            WHERE id = ? AND recipient_role = ? AND recipient_id = ?
            """,
            (notification_id, user["role"], user["id"]),
        )
        if existing is None:
            raise ApiError(404, "record_not_found", "Уведомление не найдено.")

        if not int(existing.get("is_read") or 0):
            db_execute(
                connection,
                """
                UPDATE notifications
                SET is_read = 1, read_at = ?
                WHERE id = ?
                """,
                (_utc_now_iso(), notification_id),
            )
            connection.commit()

        return query_one(
            connection,
            """
            SELECT id, recipient_role, recipient_id, title, message, metadata, notification_type, is_read, created_at, read_at
            FROM notifications
            WHERE id = ?
            """,
            (notification_id,),
        )


def mark_all_notifications_as_read(headers):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with get_connection() as connection:
        db_execute(
            connection,
            """
            UPDATE notifications
            SET is_read = 1, read_at = ?
            WHERE recipient_role = ? AND recipient_id = ? AND coalesce(is_read, 0) = 0
            """,
            (_utc_now_iso(), user["role"], user["id"]),
        )
        connection.commit()
        unread_count = query_one(
            connection,
            """
            SELECT COUNT(*) AS count
            FROM notifications
            WHERE recipient_role = ? AND recipient_id = ? AND coalesce(is_read, 0) = 0
            """,
            (user["role"], user["id"]),
        )
    return {"success": True, "unreadCount": int((unread_count or {}).get("count", 0))}


def delete_notification(headers, notification_id):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with get_connection() as connection:
        existing = query_one(
            connection,
            """
            SELECT id
            FROM notifications
            WHERE id = ? AND recipient_role = ? AND recipient_id = ?
            """,
            (notification_id, user["role"], user["id"]),
        )
        if existing is None:
            raise ApiError(404, "record_not_found", "Уведомление не найдено.")

        db_execute(connection, "DELETE FROM notifications WHERE id = ?", (notification_id,))
        connection.commit()

        unread_count = query_one(
            connection,
            """
            SELECT COUNT(*) AS count
            FROM notifications
            WHERE recipient_role = ? AND recipient_id = ? AND coalesce(is_read, 0) = 0
            """,
            (user["role"], user["id"]),
        )
    return {"success": True, "unreadCount": int((unread_count or {}).get("count", 0))}


def delete_all_notifications(headers):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with get_connection() as connection:
        db_execute(
            connection,
            "DELETE FROM notifications WHERE recipient_role = ? AND recipient_id = ?",
            (user["role"], user["id"]),
        )
        connection.commit()
    return {"success": True, "unreadCount": 0}
