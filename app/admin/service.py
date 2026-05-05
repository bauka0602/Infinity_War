from ..auth.service import require_auth_user
from ..core.config import DB_LOCK
from ..core.errors import ApiError
from ..core.orm import SessionLocal
from sqlalchemy import delete, update
from ..models import (
    Course,
    CourseComponent,
    Group,
    IupEntry,
    Notification,
    Room,
    RoomBlock,
    Schedule,
    Section,
    Student,
    Teacher,
    TeacherPreferenceRequest,
)

CLEARABLE_COLLECTIONS = {"courses", "teachers", "students", "rooms", "groups", "sections"}

COLLECTION_MODELS = {
    "courses": Course,
    "teachers": Teacher,
    "students": Student,
    "rooms": Room,
    "groups": Group,
    "sections": Section,
}


def _require_admin(headers):
    user = require_auth_user(headers)
    if user["role"] != "admin":
        raise ApiError(403, "forbidden", "Недостаточно прав")
    return user


def _delete_all(session, model):
    session.execute(delete(model))


def _recompute_room_availability(session):
    session.execute(
        update(Room)
        .where(Room.available.is_(None))
        .values(available=1)
    )


def clear_collection_data(headers, collection):
    _require_admin(headers)

    if collection == "schedules":
        raise ApiError(
            400,
            "bad_request",
            "Для расписания используйте сброс по выбранному семестру и году.",
        )

    if collection not in CLEARABLE_COLLECTIONS:
        raise ApiError(400, "bad_request", "Неподдерживаемая коллекция")

    with DB_LOCK:
        with SessionLocal() as session:
            if collection == "courses":
                _delete_all(session, Schedule)
                _delete_all(session, Section)
                _delete_all(session, CourseComponent)
                _delete_all(session, IupEntry)
            elif collection == "groups":
                _delete_all(session, Schedule)
                _delete_all(session, Section)
                _delete_all(session, IupEntry)
                session.execute(
                    update(Student).values(group_id=None, group_name="", subgroup="")
                )
            elif collection in {"teachers", "rooms"}:
                _delete_all(session, Schedule)
                if collection == "teachers":
                    session.execute(update(Course).values(instructor_id=None, instructor_name=""))
                    session.execute(update(CourseComponent).values(teacher_id=None, teacher_name=""))
                    session.execute(update(Section).values(teacher_id=None, teacher_name=""))
                    _delete_all(session, TeacherPreferenceRequest)
                    session.execute(delete(Notification).where(Notification.recipient_role == "teacher"))
                else:
                    _delete_all(session, RoomBlock)
            elif collection == "students":
                session.execute(delete(Notification).where(Notification.recipient_role == "student"))
            _delete_all(session, COLLECTION_MODELS[collection])
            if collection in {"courses", "teachers", "groups", "schedules", "sections"}:
                _recompute_room_availability(session)
            session.commit()

    return {"success": True, "collection": collection}


def clear_all_data(headers):
    _require_admin(headers)

    with DB_LOCK:
        with SessionLocal() as session:
            for model in (
                Notification,
                Schedule,
                Section,
                TeacherPreferenceRequest,
                RoomBlock,
                IupEntry,
                CourseComponent,
                Course,
                Teacher,
                Student,
                Room,
                Group,
            ):
                _delete_all(session, model)
            session.commit()

    return {
        "success": True,
        "collections": [
            "courses",
            "course_components",
            "iup_entries",
            "teacher_preference_requests",
            "teachers",
            "students",
            "rooms",
            "groups",
            "schedules",
            "sections",
        ],
    }


def clear_schedule_data(headers, semester=None, year=None):
    _require_admin(headers)

    deleted_count = 0
    with DB_LOCK:
        with SessionLocal() as session:
            statement = delete(Schedule)
            if semester is None and year is None:
                result = session.execute(statement)
            else:
                if semester is not None:
                    statement = statement.where(Schedule.semester == semester)
                if year is not None:
                    statement = statement.where(Schedule.year == year)
                result = session.execute(statement)
            deleted_count = max(0, int(getattr(result, "rowcount", 0) or 0))
            _recompute_room_availability(session)
            session.commit()

    return {
        "success": True,
        "collection": "schedules",
        "semester": semester,
        "year": year,
        "deleted": deleted_count,
    }
