import logging
import smtplib
import ssl
from email.message import EmailMessage

from .config import (
    EMAIL_NOTIFICATIONS_ENABLED,
    EMAIL_NOTIFY_STUDENTS,
    EMAIL_NOTIFY_TEACHERS,
    SMTP_FROM_EMAIL,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_TIMEOUT_SECONDS,
    SMTP_USER,
    SMTP_USE_SSL,
    SMTP_USE_TLS,
)
from .db import query_all, query_one


logger = logging.getLogger(__name__)

ACTION_LABELS = {
    "created": "добавлено",
    "updated": "изменено",
    "deleted": "удалено",
}

ACTION_TITLES = {
    "created": "В расписание добавлено новое занятие",
    "updated": "В расписании изменено занятие",
    "deleted": "Из расписания удалено занятие",
}


def notifications_configured():
    return (
        EMAIL_NOTIFICATIONS_ENABLED
        and bool(SMTP_HOST)
        and bool(SMTP_USER)
        and bool(SMTP_PASSWORD)
        and bool(SMTP_FROM_EMAIL)
    )


def _schedule_summary(schedule_item):
    if not schedule_item:
        return "Нет данных"

    subgroup = (schedule_item.get("subgroup") or "").strip()
    subgroup_label = f", подгруппа {subgroup}" if subgroup else ""
    return (
        f"Предмет: {schedule_item.get('course_name') or '-'}\n"
        f"Преподаватель: {schedule_item.get('teacher_name') or '-'}\n"
        f"Группа: {schedule_item.get('group_name') or '-'}{subgroup_label}\n"
        f"Дата: {schedule_item.get('day') or '-'}\n"
        f"Время: {schedule_item.get('start_hour') or '-'}:00\n"
        f"Аудитория: {schedule_item.get('room_number') or '-'}\n"
        f"Семестр: {schedule_item.get('semester') or '-'}\n"
        f"Год: {schedule_item.get('year') or '-'}"
    )


def _change_lines(before_item, after_item):
    if not before_item or not after_item:
        return []

    field_labels = (
        ("course_name", "Предмет"),
        ("teacher_name", "Преподаватель"),
        ("group_name", "Группа"),
        ("subgroup", "Подгруппа"),
        ("day", "Дата"),
        ("start_hour", "Время"),
        ("room_number", "Аудитория"),
        ("semester", "Семестр"),
        ("year", "Год"),
    )

    changes = []
    for field_name, label in field_labels:
        before_value = before_item.get(field_name) or "-"
        after_value = after_item.get(field_name) or "-"
        if field_name == "start_hour":
            before_value = f"{before_value}:00" if before_value != "-" else before_value
            after_value = f"{after_value}:00" if after_value != "-" else after_value
        if str(before_value) != str(after_value):
            changes.append(f"{label}: {before_value} -> {after_value}")
    return changes


def _build_message(action, before_item, after_item):
    effective_item = after_item or before_item or {}
    action_label = ACTION_LABELS.get(action, "изменено")
    action_title = ACTION_TITLES.get(action, "В расписании есть изменение")
    course_name = effective_item.get("course_name") or "занятие"
    group_name = effective_item.get("group_name") or "неизвестная группа"
    subject = f"Расписание {action_label}: {course_name}"

    lines = [
        action_title,
        f"Предмет: {course_name}",
        f"Группа: {group_name}",
        "",
    ]

    if action == "created":
        lines.extend(["Новая запись:", _schedule_summary(after_item)])
    elif action == "deleted":
        lines.extend(["Удаленная запись:", _schedule_summary(before_item)])
    else:
        changes = _change_lines(before_item, after_item)
        if changes:
            lines.extend(["Что изменилось:"])
            lines.extend(changes)
            lines.append("")
        lines.extend(
            [
                "Было:",
                _schedule_summary(before_item),
                "",
                "Стало:",
                _schedule_summary(after_item),
            ]
        )

    lines.extend(
        [
            "",
            "Проверьте актуальное расписание в системе TimeTableG.",
        ]
    )
    return subject, "\n".join(lines)


def _build_regeneration_message(semester, year, before_items, after_items):
    before_count = len(before_items or [])
    after_count = len(after_items or [])
    subject = f"Расписание обновлено: {semester} семестр {year}"
    body = "\n".join(
        [
            "Расписание было перегенерировано администратором.",
            f"Семестр: {semester}",
            f"Год: {year}",
            f"Было занятий: {before_count}",
            f"Стало занятий: {after_count}",
            "",
            "Проверьте актуальное расписание в системе TimeTableG.",
        ]
    )
    return subject, body


def _collect_teacher_recipient(connection, schedule_item):
    teacher_id = schedule_item.get("teacher_id")
    if not teacher_id or not EMAIL_NOTIFY_TEACHERS:
        return []

    teacher = query_one(
        connection,
        """
        SELECT id, name, email
        FROM teachers
        WHERE id = ? AND email IS NOT NULL AND trim(email) <> ''
        """,
        (teacher_id,),
    )
    if not teacher:
        return []

    return [
        {
            "kind": "teacher",
            "key": f"teacher:{teacher['id']}",
            "email": teacher["email"],
            "name": teacher.get("name") or "Преподаватель",
        }
    ]


def _collect_student_recipients(connection, schedule_item):
    group_id = schedule_item.get("group_id")
    if not group_id or not EMAIL_NOTIFY_STUDENTS:
        return []

    subgroup = (schedule_item.get("subgroup") or "").strip()
    params = [group_id]
    query = """
        SELECT id, name, email
        FROM students
        WHERE group_id = ? AND email IS NOT NULL AND trim(email) <> ''
    """
    if subgroup:
        query += " AND subgroup = ?"
        params.append(subgroup)

    students = query_all(connection, query, tuple(params))
    return [
        {
            "kind": "student",
            "key": f"student:{student['id']}",
            "email": student["email"],
            "name": student.get("name") or "Студент",
        }
        for student in students
    ]


def _collect_recipients(connection, schedule_item):
    if not schedule_item:
        return []
    return _collect_teacher_recipient(connection, schedule_item) + _collect_student_recipients(
        connection, schedule_item
    )


def _send_email(recipient, subject, body):
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = SMTP_FROM_EMAIL
    message["To"] = recipient["email"]
    message.set_content(f"Здравствуйте, {recipient['name']}.\n\n{body}")

    if SMTP_USE_SSL:
        with smtplib.SMTP_SSL(
            SMTP_HOST,
            SMTP_PORT,
            timeout=SMTP_TIMEOUT_SECONDS,
            context=ssl.create_default_context(),
        ) as server:
            server.ehlo()
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(message)
        return

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as server:
        server.ehlo()
        if SMTP_USE_TLS:
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
        if SMTP_USER:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(message)


def send_schedule_change_notifications(connection, action, before_item=None, after_item=None):
    if not notifications_configured():
        return {"sent": 0, "skipped": True}

    recipients = {}
    for schedule_item in (before_item, after_item):
        for recipient in _collect_recipients(connection, schedule_item):
            recipients[recipient["key"]] = recipient

    if not recipients:
        return {"sent": 0, "skipped": True}

    subject, body = _build_message(action, before_item, after_item)
    sent_count = 0

    for recipient in recipients.values():
        try:
            _send_email(recipient, subject, body)
            sent_count += 1
        except Exception:
            logger.exception("Failed to send schedule change email to %s", recipient["email"])

    return {"sent": sent_count, "skipped": False}


def send_schedule_regeneration_notifications(connection, semester, year, before_items, after_items):
    if not notifications_configured():
        return {"sent": 0, "skipped": True}

    recipients = {}
    for schedule_item in [*(before_items or []), *(after_items or [])]:
        for recipient in _collect_recipients(connection, schedule_item):
            recipients[recipient["key"]] = recipient

    if not recipients:
        return {"sent": 0, "skipped": True}

    subject, body = _build_regeneration_message(semester, year, before_items, after_items)
    sent_count = 0

    for recipient in recipients.values():
        try:
            _send_email(recipient, subject, body)
            sent_count += 1
        except Exception:
            logger.exception("Failed to send regeneration email to %s", recipient["email"])

    return {"sent": sent_count, "skipped": False}
