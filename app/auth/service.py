import secrets
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, update

from ..core.config import DB_LOCK, EXPOSE_DEV_CLAIM_CODE, TEACHER_EMAIL_DOMAIN
from ..core.errors import ApiError
from ..core.orm import SessionLocal
from ..models import Course, CourseComponent, Group, Schedule, Section, Student, Teacher, User
from .security import (
    hash_password,
    needs_password_rehash,
    parse_bearer_token,
    sanitize_user,
    verify_password,
)
from ..teachers.utils import build_teacher_name_signature, normalize_teacher_name


def ensure_teacher_email_allowed(email, role):
    if role == "teacher" and not email.lower().endswith(TEACHER_EMAIL_DOMAIN):
        raise ApiError(
            400,
            "teacher_email_domain_required",
            "Для преподавателя нужен email, оканчивающийся на @kazatu.edu.kz",
        )


def normalize_language(value, default="ru"):
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"ru", "kk"} else default


def normalize_teaching_languages(value):
    if value in (None, ""):
        return []
    if isinstance(value, str):
        raw_values = value.split(",")
    else:
        raw_values = value or []
    seen = []
    for raw in raw_values:
        normalized = normalize_language(raw, "")
        if normalized and normalized not in seen:
            seen.append(normalized)
    return seen


def normalize_phone_search(value):
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def _count_students_in_group(connection, group_id):
    with SessionLocal() as session:
        return int(
            session.scalar(
                select(func.count()).select_from(Student).where(Student.group_id == group_id)
            )
            or 0
        )


def _count_students_in_subgroup(connection, group_id, subgroup):
    with SessionLocal() as session:
        return int(
            session.scalar(
                select(func.count())
                .select_from(Student)
                .where(
                    Student.group_id == group_id,
                    func.upper(func.coalesce(Student.subgroup, "")) == subgroup,
                )
            )
            or 0
        )


def _subgroup_capacity_limits(group_capacity):
    normalized_capacity = max(0, int(group_capacity or 0))
    first_capacity = (normalized_capacity + 1) // 2
    second_capacity = normalized_capacity // 2
    return {"A": first_capacity, "B": second_capacity}


def _enforce_student_group_capacity(connection, selected_group, subgroup):
    group_id = selected_group["id"]
    group_capacity = max(0, int(selected_group.get("student_count") or 0))
    current_group_count = _count_students_in_group(connection, group_id)
    if group_capacity and current_group_count >= group_capacity:
        raise ApiError(400, "group_full", "Группа уже заполнена")

    requires_subgroup = bool(
        selected_group.get("auto_has_subgroups") or selected_group.get("has_subgroups")
    )
    if not requires_subgroup:
        return

    subgroup_limits = _subgroup_capacity_limits(group_capacity)
    selected_subgroup = str(subgroup or "").strip().upper()
    selected_limit = subgroup_limits.get(selected_subgroup, 0)
    if selected_limit <= 0:
        raise ApiError(400, "subgroup_full", f"Подгруппа {selected_subgroup} недоступна")

    current_subgroup_count = _count_students_in_subgroup(connection, group_id, selected_subgroup)
    if current_subgroup_count < selected_limit:
        return

    alternate_subgroup = "B" if selected_subgroup == "A" else "A"
    alternate_limit = subgroup_limits.get(alternate_subgroup, 0)
    alternate_count = _count_students_in_subgroup(connection, group_id, alternate_subgroup)
    if alternate_limit > 0 and alternate_count < alternate_limit:
        raise ApiError(
            400,
            "subgroup_full",
            f"Подгруппа {selected_subgroup} заполнена, выберите {alternate_subgroup}",
        )

    raise ApiError(400, "group_full", "Группа уже заполнена")


def _utc_now():
    return datetime.now(timezone.utc)


def _utc_now_iso():
    return _utc_now().isoformat()


def _is_teacher_claimed(teacher):
    return bool(teacher.get("password") or teacher.get("token"))


def _serialize_claimable_teacher(row):
    email = str(row.get("email") or "").strip()
    local_part, _, domain = email.partition("@")
    if not email:
        masked_email = ""
    elif len(local_part) <= 2:
        masked_local = local_part[:1] + "*"
        masked_email = f"{masked_local}@{domain}" if domain else masked_local
    else:
        masked_local = local_part[:2] + "*" * max(1, len(local_part) - 2)
        masked_email = f"{masked_local}@{domain}" if domain else masked_local
    return {
        "id": row["id"],
        "name": row["name"],
        "maskedEmail": masked_email,
        "hasEmail": bool(email),
        "teachingLanguages": row.get("teaching_languages", "") or "ru,kk",
    }


def _admin_account_dict(row):
    return {
        "id": row.id,
        "email": row.email,
        "phone": "",
        "full_name": row.full_name,
        "role": row.role,
        "token": row.token,
        "avatar_data": row.avatar_data,
        "department": "",
        "programme": "",
        "group_id": None,
        "group_name": "",
        "subgroup": "",
        "language": "",
        "teaching_languages": "",
    }


def _teacher_account_dict(row, include_claim_fields=False):
    data = {
        "id": row.id,
        "email": row.email,
        "phone": row.phone,
        "password": row.password,
        "full_name": row.name,
        "name": row.name,
        "role": "teacher",
        "token": row.token,
        "avatar_data": row.avatar_data,
        "department": row.department,
        "subject_taught": row.subject_taught,
        "programme": "",
        "group_id": None,
        "group_name": "",
        "subgroup": "",
        "language": "",
        "weekly_hours_limit": row.weekly_hours_limit,
        "teaching_languages": row.teaching_languages,
    }
    if include_claim_fields:
        data.update(
            {
                "claim_code": row.claim_code,
                "claim_code_expires_at": row.claim_code_expires_at,
                "claim_requested_at": row.claim_requested_at,
            }
        )
    return data


def _student_account_dict(row):
    return {
        "id": row.id,
        "email": row.email,
        "phone": "",
        "password": row.password,
        "full_name": row.name,
        "name": row.name,
        "role": "student",
        "token": row.token,
        "avatar_data": row.avatar_data,
        "department": row.department,
        "programme": row.programme,
        "group_id": row.group_id,
        "group_name": row.group_name,
        "subgroup": row.subgroup,
        "language": row.language,
        "teaching_languages": "",
    }


def _find_account_by_token(connection, token):
    with SessionLocal() as session:
        admin = session.scalar(
            select(User).where(User.role == "admin", User.token == token)
        )
        if admin:
            return _admin_account_dict(admin)

        teacher = session.scalar(select(Teacher).where(Teacher.token == token))
        if teacher:
            return _teacher_account_dict(teacher)

        student = session.scalar(select(Student).where(Student.token == token))
        return _student_account_dict(student) if student else None


def _email_exists(connection, email):
    normalized = email.strip().lower()
    with SessionLocal() as session:
        return any(
            (
                session.scalar(select(User.id).where(func.lower(User.email) == normalized)),
                session.scalar(select(Teacher.id).where(func.lower(Teacher.email) == normalized)),
                session.scalar(select(Student.id).where(func.lower(Student.email) == normalized)),
            )
        )


def _email_exists_for_other_account(email, current_role, current_id):
    normalized = email.strip().lower()
    with SessionLocal() as session:
        admin_query = select(User.id).where(func.lower(User.email) == normalized)
        teacher_query = select(Teacher.id).where(func.lower(Teacher.email) == normalized)
        student_query = select(Student.id).where(func.lower(Student.email) == normalized)

        if current_role == "admin":
            admin_query = admin_query.where(User.id != current_id)
        elif current_role == "teacher":
            teacher_query = teacher_query.where(Teacher.id != current_id)
        elif current_role == "student":
            student_query = student_query.where(Student.id != current_id)

        return any(
            (
                session.scalar(admin_query),
                session.scalar(teacher_query),
                session.scalar(student_query),
            )
        )


def _find_teacher_by_email(connection, email):
    normalized = email.strip().lower()
    with SessionLocal() as session:
        teacher = session.scalar(select(Teacher).where(func.lower(Teacher.email) == normalized))
        return _teacher_account_dict(teacher) if teacher else None


def _find_teacher_by_id(connection, teacher_id):
    with SessionLocal() as session:
        teacher = session.get(Teacher, teacher_id)
        return _teacher_account_dict(teacher, include_claim_fields=True) if teacher else None


def _clear_teacher_claim_state(connection, teacher_id):
    with SessionLocal() as session:
        session.execute(
            update(Teacher)
            .where(Teacher.id == teacher_id)
            .values(
                claim_code=None,
                claim_code_expires_at=None,
                claim_requested_at=None,
            )
        )
        session.commit()


def _get_teacher_assigned_disciplines(connection, teacher_id):
    with SessionLocal() as session:
        rows = session.execute(
            select(CourseComponent.course_name.label("discipline_name"))
            .where(
                CourseComponent.teacher_id == teacher_id,
                func.trim(func.coalesce(CourseComponent.course_name, "")) != "",
            )
            .union(
                select(Course.name.label("discipline_name")).where(
                    Course.instructor_id == teacher_id,
                    func.trim(func.coalesce(Course.name, "")) != "",
                ),
                select(Section.course_name.label("discipline_name")).where(
                    Section.teacher_id == teacher_id,
                    func.trim(func.coalesce(Section.course_name, "")) != "",
                ),
                select(Teacher.subject_taught.label("discipline_name")).where(
                    Teacher.id == teacher_id,
                    func.trim(func.coalesce(Teacher.subject_taught, "")) != "",
                ),
            )
            .order_by("discipline_name")
        ).all()
    disciplines = []
    for row in rows:
        for item in str(row.discipline_name or "").replace(";", ",").split(","):
            discipline = item.strip()
            if discipline and discipline not in disciplines:
                disciplines.append(discipline)
    return sorted(disciplines)


def _sanitize_profile_user(connection, user):
    if user and user.get("role") == "teacher":
        disciplines = _get_teacher_assigned_disciplines(connection, user["id"])
        return sanitize_user(
            {
                **user,
                "assigned_disciplines": disciplines,
                "assigned_disciplines_text": ", ".join(disciplines),
            }
        )
    return sanitize_user(user)


def _find_login_account(connection, email, selected_role):
    normalized = email.strip().lower()
    with SessionLocal() as session:
        if selected_role == "admin":
            admin = session.scalar(
                select(User).where(User.role == "admin", func.lower(User.email) == normalized)
            )
            if admin is None:
                return None
            data = _admin_account_dict(admin)
            data["password"] = admin.password
            return data
        if selected_role == "teacher":
            teacher = session.scalar(select(Teacher).where(func.lower(Teacher.email) == normalized))
            return _teacher_account_dict(teacher) if teacher else None
        if selected_role == "student":
            student = session.scalar(select(Student).where(func.lower(Student.email) == normalized))
            return _student_account_dict(student) if student else None

    return (
        _find_login_account(connection, email, "admin")
        or _find_login_account(connection, email, "teacher")
        or _find_login_account(connection, email, "student")
    )


def require_auth_user(headers):
    token = parse_bearer_token(headers.get("Authorization"))
    if not token:
        raise ApiError(401, "auth_required", "Требуется авторизация")

    with DB_LOCK:
        user = _find_account_by_token(None, token)

    if user is None:
        raise ApiError(401, "invalid_token", "Недействительный токен")

    return user


def register_user(payload):
    required = ["email", "password", "displayName"]
    missing = [field for field in required if not payload.get(field)]
    if missing:
        raise ApiError(
            400,
            "fill_required_fields",
            f"Заполните поля: {', '.join(missing)}",
            {"fields": missing},
        )

    role = (payload.get("role") or "student").strip().lower()
    email = payload["email"].strip()
    phone = (payload.get("phone") or "").strip()
    department = (payload.get("department") or "").strip()
    subject_taught = (payload.get("subjectTaught") or payload.get("subject_taught") or department).strip()
    programme_name = (payload.get("programmeName") or "").strip()
    subgroup = (payload.get("subgroup") or "").strip().upper()
    group_id = payload.get("groupId")
    student_language = normalize_language(payload.get("language"), "")
    teaching_languages = normalize_teaching_languages(payload.get("teachingLanguages"))
    if role not in {"student", "teacher"}:
        raise ApiError(
            400,
            "invalid_registration_role",
            "Можно зарегистрироваться только как студент или преподаватель",
        )

    if role == "student":
        student_missing = []
        if not department:
            student_missing.append("department")
        if not programme_name:
            student_missing.append("programmeName")
        if not group_id:
            student_missing.append("groupId")
        if not student_language:
            student_missing.append("language")
        if student_missing:
            raise ApiError(
                400,
                "fill_required_fields",
                f"Заполните поля: {', '.join(student_missing)}",
                {"fields": student_missing},
            )
    else:
        teacher_missing = []
        if not phone:
            teacher_missing.append("phone")
        if not department:
            teacher_missing.append("department")
        if not teaching_languages:
            teacher_missing.append("teachingLanguages")
        if teacher_missing:
            raise ApiError(
                400,
                "fill_required_fields",
                f"Заполните поля: {', '.join(teacher_missing)}",
                {"fields": teacher_missing},
            )

    selected_group = None

    ensure_teacher_email_allowed(email, role)

    with DB_LOCK:
        with SessionLocal() as session:
            existing_teacher = _find_teacher_by_email(None, email) if role == "teacher" else None
            if role == "student":
                try:
                    group_id = int(group_id)
                except (TypeError, ValueError) as exc:
                    raise ApiError(
                        400,
                        "fill_required_fields",
                        "Заполните поля: groupId",
                        {"fields": ["groupId"]},
                    ) from exc

                group = session.get(Group, group_id)
                if group is None:
                    raise ApiError(400, "bad_request", "Выбрана некорректная группа")
                auto_has_subgroups = bool(
                    session.scalar(
                        select(Schedule.id)
                        .where(
                            Schedule.group_id == group.id,
                            func.trim(func.coalesce(Schedule.subgroup, "")) != "",
                        )
                        .limit(1)
                    )
                )
                selected_group = {
                    "id": group.id,
                    "name": group.name,
                    "student_count": group.student_count,
                    "has_subgroups": group.has_subgroups,
                    "language": group.language,
                    "auto_has_subgroups": 1 if auto_has_subgroups else 0,
                }
                if student_language != normalize_language(selected_group.get("language"), "ru"):
                    raise ApiError(
                        400,
                        "bad_request",
                        "Язык студента должен совпадать с языком обучения группы",
                    )
                if selected_group.get("auto_has_subgroups") or selected_group.get("has_subgroups"):
                    if subgroup not in {"A", "B"}:
                        raise ApiError(
                            400,
                            "fill_required_fields",
                            "Заполните поля: subgroup",
                            {"fields": ["subgroup"]},
                        )
                else:
                    subgroup = ""

                _enforce_student_group_capacity(None, selected_group, subgroup)

            if role == "teacher" and existing_teacher and not _is_teacher_claimed(existing_teacher):
                raise ApiError(
                    400,
                    "teacher_claim_required",
                    "Для импортированного преподавателя нужно подтвердить аккаунт через поиск и код.",
                )

            if _email_exists(None, email):
                raise ApiError(
                    400,
                    "email_already_exists",
                    "Пользователь с таким email уже существует",
                )
            token = secrets.token_urlsafe(32)
            if role == "teacher":
                teacher = Teacher(
                    name=payload["displayName"],
                    email=email,
                    password=hash_password(payload["password"]),
                    token=token,
                    avatar_data=None,
                    phone=phone,
                    department=department,
                    subject_taught=subject_taught,
                    weekly_hours_limit=None,
                    teaching_languages=",".join(teaching_languages),
                    name_normalized=normalize_teacher_name(payload["displayName"]),
                    name_signature=build_teacher_name_signature(payload["displayName"]),
                )
                session.add(teacher)
                session.commit()
                user = _teacher_account_dict(teacher)
            else:
                student = Student(
                    name=payload["displayName"],
                    email=email,
                    password=hash_password(payload["password"]),
                    token=token,
                    avatar_data=None,
                    department=department,
                    programme=programme_name,
                    group_id=selected_group["id"],
                    group_name=selected_group["name"],
                    subgroup=subgroup,
                    language=student_language,
                )
                session.add(student)
                session.commit()
                user = _student_account_dict(student)
            return _sanitize_profile_user(None, user)


def get_current_profile(headers):
    user = require_auth_user(headers)
    with DB_LOCK:
        return _sanitize_profile_user(None, user)


def update_profile_avatar(headers, payload):
    avatar_data = (payload.get("avatarData") or "").strip()
    if not avatar_data:
        raise ApiError(400, "fill_required_fields", "Заполните поля: avatarData")

    if not avatar_data.startswith("data:image/"):
        raise ApiError(400, "bad_request", "Допустимы только изображения")

    if len(avatar_data) > 1_500_000:
        raise ApiError(400, "bad_request", "Изображение слишком большое")

    user = require_auth_user(headers)

    with DB_LOCK:
        with SessionLocal() as session:
            if user["role"] == "admin":
                session.execute(
                    update(User).where(User.id == user["id"]).values(avatar_data=avatar_data)
                )
            elif user["role"] == "teacher":
                session.execute(
                    update(Teacher)
                    .where(Teacher.id == user["id"])
                    .values(avatar_data=avatar_data)
                )
            else:
                session.execute(
                    update(Student)
                    .where(Student.id == user["id"])
                    .values(avatar_data=avatar_data)
                )
            session.commit()

        updated_user = _find_account_by_token(None, user["token"])

    return _sanitize_profile_user(None, updated_user)


def update_profile_email(headers, payload):
    email = (payload.get("email") or "").strip().lower()
    if not email:
        raise ApiError(400, "fill_required_fields", "Заполните поля: email")
    if "@" not in email or "." not in email.rsplit("@", 1)[-1]:
        raise ApiError(400, "bad_request", "Некорректный email")

    user = require_auth_user(headers)
    if user["role"] not in {"teacher", "student"}:
        raise ApiError(403, "forbidden", "Email могут менять только преподаватели и студенты")

    if _email_exists_for_other_account(email, user["role"], user["id"]):
        raise ApiError(400, "email_already_exists", "Пользователь с таким email уже существует")

    with DB_LOCK:
        with SessionLocal() as session:
            if user["role"] == "teacher":
                session.execute(update(Teacher).where(Teacher.id == user["id"]).values(email=email))
            else:
                session.execute(update(Student).where(Student.id == user["id"]).values(email=email))
            session.commit()

        updated_user = _find_account_by_token(None, user["token"])

    return _sanitize_profile_user(None, updated_user)


def login_user(payload):
    email = payload.get("email", "").strip()
    password = payload.get("password", "")
    selected_role = (payload.get("role") or "").strip().lower()

    if selected_role and selected_role not in {"admin", "student", "teacher"}:
        raise ApiError(400, "invalid_role", "Некорректная роль")

    with DB_LOCK:
        user = _find_login_account(None, email, selected_role or None)

        if user is None or not verify_password(user["password"], password):
            raise ApiError(401, "invalid_credentials", "Неверный email или пароль")

        if selected_role and user["role"] != selected_role:
            raise ApiError(
                403,
                "role_mismatch",
                "Этот аккаунт зарегистрирован с другой ролью",
            )

        if user["role"] == "teacher" and not user["email"].lower().endswith(
            TEACHER_EMAIL_DOMAIN
        ):
            raise ApiError(
                403,
                "teacher_account_email_domain_required",
                "У аккаунта преподавателя должен быть email @kazatu.edu.kz",
            )

        token = secrets.token_urlsafe(32)
        password_hash = hash_password(password) if needs_password_rehash(user["password"]) else user["password"]
        with SessionLocal() as session:
            if user["role"] == "admin":
                session.execute(
                    update(User)
                    .where(User.id == user["id"])
                    .values(token=token, password=password_hash)
                )
            elif user["role"] == "teacher":
                session.execute(
                    update(Teacher)
                    .where(Teacher.id == user["id"])
                    .values(token=token, password=password_hash)
                )
            else:
                session.execute(
                    update(Student)
                    .where(Student.id == user["id"])
                    .values(token=token, password=password_hash)
                )
            session.commit()

        user["token"] = token
        user["password"] = password_hash

    return sanitize_user(user)


def logout_user(headers):
    user = require_auth_user(headers)

    with DB_LOCK:
        with SessionLocal() as session:
            if user["role"] == "admin":
                session.execute(update(User).where(User.id == user["id"]).values(token=""))
            elif user["role"] == "teacher":
                session.execute(update(Teacher).where(Teacher.id == user["id"]).values(token=""))
            else:
                session.execute(update(Student).where(Student.id == user["id"]).values(token=""))
            session.commit()

    return {"success": True}


def search_claimable_teachers(query_value):
    search = str(query_value or "").strip().lower()
    if len(search) < 2:
        return []

    search_tokens = []
    for raw_part in [search, *search.split()]:
        token = raw_part.strip()
        if len(token) < 2:
            continue
        if token not in search_tokens:
            search_tokens.append(token)

    phone_tokens = []
    for raw_part in [query_value, *str(query_value or "").split()]:
        token = normalize_phone_search(raw_part)
        if len(token) < 2:
            continue
        if token not in phone_tokens:
            phone_tokens.append(token)

    if not search_tokens and not phone_tokens:
        return []

    with DB_LOCK:
        with SessionLocal() as session:
            rows = session.execute(
                select(
                    Teacher.id.label("id"),
                    Teacher.name.label("name"),
                    Teacher.email.label("email"),
                    Teacher.phone.label("phone"),
                    Teacher.teaching_languages.label("teaching_languages"),
                )
                .where(
                    func.coalesce(Teacher.password, "") == "",
                    func.coalesce(Teacher.token, "") == "",
                )
                .order_by(Teacher.name, Teacher.id)
            ).mappings().all()

    matched_rows = []
    for row in rows:
        haystack = " ".join(
            [
                str(row.get("name") or "").lower(),
                str(row.get("email") or "").lower(),
                str(row.get("phone") or "").lower(),
            ]
        )
        normalized_phone = normalize_phone_search(row.get("phone"))
        text_match = any(token in haystack for token in search_tokens)
        phone_match = any(token in normalized_phone for token in phone_tokens)
        if text_match or phone_match:
            matched_rows.append(row)
        if len(matched_rows) >= 10:
            break

    return [_serialize_claimable_teacher(row) for row in matched_rows]


def request_teacher_claim(payload):
    teacher_id = payload.get("teacherId")
    provided_email = str(payload.get("email") or "").strip().lower()

    if not teacher_id:
        raise ApiError(
            400,
            "fill_required_fields",
            "Заполните поля: teacherId",
            {"fields": ["teacherId"]},
        )

    try:
        teacher_id = int(teacher_id)
    except (TypeError, ValueError) as exc:
        raise ApiError(400, "invalid_id", "ID должен быть числом.") from exc

    with DB_LOCK:
        with SessionLocal() as session:
            teacher = session.get(Teacher, teacher_id)
            if teacher is None:
                raise ApiError(404, "record_not_found", "Преподаватель не найден.")
            if teacher.password or teacher.token:
                raise ApiError(
                    400,
                    "teacher_claim_already_completed",
                    "Этот аккаунт преподавателя уже активирован.",
                )
            email = str(teacher.email or "").strip().lower()
            if not email:
                if not provided_email:
                    raise ApiError(
                        400,
                        "fill_required_fields",
                        "Заполните поля: email",
                        {"fields": ["email"]},
                    )
                ensure_teacher_email_allowed(provided_email, "teacher")
                email = provided_email
                teacher.email = email
            else:
                ensure_teacher_email_allowed(email, "teacher")

            claim_code = f"{secrets.randbelow(1_000_000):06d}"
            expires_at = (_utc_now() + timedelta(minutes=10)).isoformat()
            requested_at = _utc_now_iso()
            teacher.claim_code = claim_code
            teacher.claim_code_expires_at = expires_at
            teacher.claim_requested_at = requested_at
            session.commit()

    return {
        "success": True,
        "teacherId": teacher_id,
        "email": email,
        "expiresAt": expires_at,
        "debugCode": claim_code if EXPOSE_DEV_CLAIM_CODE else None,
    }


def confirm_teacher_claim(payload):
    teacher_id = payload.get("teacherId")
    provided_email = str(payload.get("email") or "").strip().lower()
    code = str(payload.get("code") or "").strip()
    password = payload.get("password") or ""

    missing = []
    if not teacher_id:
        missing.append("teacherId")
    if not code:
        missing.append("code")
    if not password:
        missing.append("password")
    if missing:
        raise ApiError(
            400,
            "fill_required_fields",
            f"Заполните поля: {', '.join(missing)}",
            {"fields": missing},
        )

    try:
        teacher_id = int(teacher_id)
    except (TypeError, ValueError) as exc:
        raise ApiError(400, "invalid_id", "ID должен быть числом.") from exc

    with DB_LOCK:
        with SessionLocal() as session:
            teacher = session.get(Teacher, teacher_id)
            if teacher is None:
                raise ApiError(404, "record_not_found", "Преподаватель не найден.")
            if teacher.password or teacher.token:
                raise ApiError(
                    400,
                    "teacher_claim_already_completed",
                    "Этот аккаунт преподавателя уже активирован.",
                )
            email = str(teacher.email or "").strip().lower()
            if not email:
                if not provided_email:
                    raise ApiError(
                        400,
                        "fill_required_fields",
                        "Заполните поля: email",
                        {"fields": ["email"]},
                    )
                ensure_teacher_email_allowed(provided_email, "teacher")
                email = provided_email
                teacher.email = email
            else:
                ensure_teacher_email_allowed(email, "teacher")
            if not teacher.claim_code or teacher.claim_code != code:
                raise ApiError(
                    400,
                    "teacher_claim_code_invalid",
                    "Код подтверждения неверный.",
                )

            expires_at_raw = teacher.claim_code_expires_at
            if not expires_at_raw:
                raise ApiError(
                    400,
                    "teacher_claim_code_expired",
                    "Срок действия кода истёк. Запросите новый код.",
                )
            expires_at = datetime.fromisoformat(expires_at_raw)
            if expires_at < _utc_now():
                teacher.claim_code = None
                teacher.claim_code_expires_at = None
                teacher.claim_requested_at = None
                session.commit()
                raise ApiError(
                    400,
                    "teacher_claim_code_expired",
                    "Срок действия кода истёк. Запросите новый код.",
                )

            token = secrets.token_urlsafe(32)
            teacher.password = hash_password(password)
            teacher.token = token
            teacher.claim_code = None
            teacher.claim_code_expires_at = None
            teacher.claim_requested_at = None
            session.commit()

    return {"success": True}
