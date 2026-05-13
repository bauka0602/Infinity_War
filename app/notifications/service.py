from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import delete, func, select, update

from ..auth.service import require_auth_user
from ..core.errors import ApiError
from ..core.orm import SessionLocal
from ..models import Notification, Student
from ..schedule.time_slots import format_lesson_time_range


def _utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _normalize_subgroup(value):
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"A", "B"} else ""


def _recipient_key(role, recipient_id):
    return f"{role}:{int(recipient_id)}"


def _fetch_student_recipients(session, schedule_item):
    group_id = schedule_item.get("group_id")
    if not group_id:
        return []

    subgroup = _normalize_subgroup(schedule_item.get("subgroup"))
    statement = select(Student.id.label("id"), Student.name.label("name")).where(
        Student.group_id == group_id
    )
    if subgroup:
        statement = statement.where(
            func.upper(func.coalesce(Student.subgroup, "")) == subgroup
        )
    return session.execute(statement.order_by(Student.id)).mappings().all()


def _build_recipients_for_schedule(session, schedule_item):
    recipients = {}

    teacher_id = schedule_item.get("teacher_id")
    if teacher_id:
        recipients[_recipient_key("teacher", teacher_id)] = {
            "role": "teacher",
            "id": int(teacher_id),
            "name": schedule_item.get("teacher_name", ""),
        }

    for student in _fetch_student_recipients(session, schedule_item):
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


def _collect_snapshots_by_recipient(session, schedule_items):
    snapshots = {}
    recipient_info = {}

    for item in schedule_items:
        signature = _schedule_signature(item)
        recipients = _build_recipients_for_schedule(session, item)
        for key, recipient in recipients.items():
            recipient_info[key] = recipient
            snapshots.setdefault(key, set()).add(signature)

    return snapshots, recipient_info


def _insert_notification(
    session,
    recipient_role,
    recipient_id,
    title,
    message,
    notification_type,
    metadata=None,
):
    notification = Notification(
        recipient_role=recipient_role,
        recipient_id=recipient_id,
        title=title,
        message=message,
        metadata_json=json.dumps(metadata or {}, ensure_ascii=False),
        notification_type=notification_type,
        is_read=0,
        created_at=_utc_now_iso(),
        read_at=None,
    )
    session.add(notification)
    session.flush()
    return _notification_to_dict(notification)


def _notification_to_dict(notification):
    if notification is None:
        return None
    return {
        "id": notification.id,
        "recipient_role": notification.recipient_role,
        "recipient_id": notification.recipient_id,
        "title": notification.title,
        "message": notification.message,
        "metadata": notification.metadata_json,
        "notification_type": notification.notification_type,
        "is_read": notification.is_read,
        "created_at": notification.created_at,
        "read_at": notification.read_at,
    }


def create_schedule_change_notifications(connection, before_item=None, after_item=None):
    before_item = before_item or None
    after_item = after_item or None
    if before_item is None and after_item is None:
        return []

    with SessionLocal() as session:
        before_snapshots, before_recipient_info = _collect_snapshots_by_recipient(
            session,
            [before_item] if before_item else [],
        )
        after_snapshots, after_recipient_info = _collect_snapshots_by_recipient(
            session,
            [after_item] if after_item else [],
        )

        created = []
        all_recipient_keys = set(before_snapshots) | set(after_snapshots)
        for key in all_recipient_keys:
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
                    session,
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
            session.commit()
    return created


def create_schedule_regeneration_notifications(connection, semester, year, before_items, after_items):
    with SessionLocal() as session:
        before_snapshots, before_recipient_info = _collect_snapshots_by_recipient(session, before_items)
        after_snapshots, after_recipient_info = _collect_snapshots_by_recipient(session, after_items)

        created = []
        all_recipient_keys = set(before_snapshots) | set(after_snapshots)
        for key in all_recipient_keys:
            recipient = after_recipient_info.get(key) or before_recipient_info.get(key)
            if recipient is None:
                continue

            created.append(
                _insert_notification(
                    session,
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
            session.commit()
    return created


def list_notifications(headers):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        return {"items": [], "unreadCount": 0}

    with SessionLocal() as session:
        notifications = session.scalars(
            select(Notification)
            .where(
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
            )
            .order_by(Notification.created_at.desc(), Notification.id.desc())
        ).all()
        unread_count = session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
                Notification.is_read == 0,
            )
        )
    return {
        "items": [_notification_to_dict(notification) for notification in notifications],
        "unreadCount": int(unread_count or 0),
    }


def mark_notification_as_read(headers, notification_id):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with SessionLocal() as session:
        existing = session.scalar(
            select(Notification).where(
                Notification.id == notification_id,
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
            )
        )
        if existing is None:
            raise ApiError(404, "record_not_found", "Уведомление не найдено.")

        if not int(existing.is_read or 0):
            existing.is_read = 1
            existing.read_at = _utc_now_iso()
            session.commit()

        return _notification_to_dict(existing)


def mark_all_notifications_as_read(headers):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with SessionLocal() as session:
        session.execute(
            update(Notification)
            .where(
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
                Notification.is_read == 0,
            )
            .values(is_read=1, read_at=_utc_now_iso())
        )
        session.commit()
        unread_count = session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
                Notification.is_read == 0,
            )
        )
    return {"success": True, "unreadCount": int(unread_count or 0)}


def delete_notification(headers, notification_id):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with SessionLocal() as session:
        existing = session.scalar(
            select(Notification.id).where(
                Notification.id == notification_id,
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
            )
        )
        if existing is None:
            raise ApiError(404, "record_not_found", "Уведомление не найдено.")

        session.execute(delete(Notification).where(Notification.id == notification_id))
        session.commit()
        unread_count = session.scalar(
            select(func.count())
            .select_from(Notification)
            .where(
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
                Notification.is_read == 0,
            )
        )
    return {"success": True, "unreadCount": int(unread_count or 0)}


def delete_all_notifications(headers):
    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Недостаточно прав")

    with SessionLocal() as session:
        session.execute(
            delete(Notification).where(
                Notification.recipient_role == user["role"],
                Notification.recipient_id == user["id"],
            )
        )
        session.commit()
    return {"success": True, "unreadCount": 0}
