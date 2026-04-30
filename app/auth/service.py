import secrets
from datetime import datetime, timedelta, timezone

from ..core.config import DB_LOCK, EXPOSE_DEV_CLAIM_CODE, TEACHER_EMAIL_DOMAIN
from ..core.db import db_execute, get_connection, insert_and_get_id, query_all, query_one
from ..core.errors import ApiError
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
    return int(
        query_one(
            connection,
            """
            SELECT COUNT(*) AS count
            FROM students
            WHERE group_id = ?
            """,
            (group_id,),
        )["count"]
    )


def _count_students_in_subgroup(connection, group_id, subgroup):
    return int(
        query_one(
            connection,
            """
            SELECT COUNT(*) AS count
            FROM students
            WHERE group_id = ?
              AND upper(coalesce(subgroup, '')) = ?
            """,
            (group_id, subgroup),
        )["count"]
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

def _find_account_by_token(connection, token):
    admin = query_one(
        connection,
        """
        SELECT id, email, '' AS phone, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup, '' AS language, '' AS teaching_languages
        FROM users
        WHERE role = 'admin' AND token = ?
        """,
        (token,),
    )
    if admin:
        return admin

    teacher = query_one(
        connection,
        """
        SELECT
            id, email, phone, name AS full_name, 'teacher' AS role, token, avatar_data,
            '' AS department, subject_taught, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup, '' AS language, teaching_languages
        FROM teachers
        WHERE token = ?
        """,
        (token,),
    )
    if teacher:
        return teacher

    return query_one(
        connection,
        """
        SELECT
            id, email, '' AS phone, name AS full_name, 'student' AS role, token, avatar_data,
            department, programme, group_id, group_name, subgroup, language, '' AS teaching_languages
        FROM students
        WHERE token = ?
        """,
        (token,),
    )


def _email_exists(connection, email):
    normalized = email.strip().lower()
    checks = (
        ("users", "SELECT id FROM users WHERE lower(email) = lower(?)"),
        ("teachers", "SELECT id FROM teachers WHERE lower(email) = lower(?)"),
        ("students", "SELECT id FROM students WHERE lower(email) = lower(?)"),
    )
    for _, query in checks:
        if query_one(connection, query, (normalized,)):
            return True
    return False


def _find_teacher_by_email(connection, email):
    normalized = email.strip().lower()
    return query_one(
        connection,
        """
        SELECT id, email, password, token, name, phone, subject_taught, weekly_hours_limit, avatar_data, teaching_languages
        FROM teachers
        WHERE lower(email) = lower(?)
        """,
        (normalized,),
    )


def _find_teacher_by_id(connection, teacher_id):
    return query_one(
        connection,
        """
        SELECT
            id, email, password, token, name, phone, subject_taught, weekly_hours_limit,
            avatar_data, teaching_languages, claim_code, claim_code_expires_at, claim_requested_at
        FROM teachers
        WHERE id = ?
        """,
        (teacher_id,),
    )


def _clear_teacher_claim_state(connection, teacher_id):
    db_execute(
        connection,
        """
        UPDATE teachers
        SET claim_code = NULL, claim_code_expires_at = NULL, claim_requested_at = NULL
        WHERE id = ?
        """,
        (teacher_id,),
    )


def _get_teacher_assigned_disciplines(connection, teacher_id):
    rows = query_all(
        connection,
        """
        SELECT discipline_name
        FROM (
            SELECT course_name AS discipline_name
            FROM course_components
            WHERE teacher_id = ?
              AND trim(coalesce(course_name, '')) <> ''

            UNION

            SELECT name AS discipline_name
            FROM courses
            WHERE instructor_id = ?
              AND trim(coalesce(name, '')) <> ''

            UNION

            SELECT course_name AS discipline_name
            FROM sections
            WHERE teacher_id = ?
              AND trim(coalesce(course_name, '')) <> ''
        ) assigned
        ORDER BY discipline_name
        """,
        (teacher_id, teacher_id, teacher_id),
    )
    return [row["discipline_name"] for row in rows if row.get("discipline_name")]


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
    if selected_role == "admin":
        return query_one(
            connection,
            """
            SELECT id, email, '' AS phone, password, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup, '' AS language, '' AS teaching_languages
            FROM users
            WHERE role = 'admin' AND lower(email) = lower(?)
            """,
            (normalized,),
        )
    if selected_role == "teacher":
        return query_one(
            connection,
            """
            SELECT
                id, email, phone, password, name AS full_name, 'teacher' AS role, token, avatar_data,
                '' AS department, subject_taught, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup, '' AS language, teaching_languages
            FROM teachers
            WHERE lower(email) = lower(?)
            """,
            (normalized,),
        )
    if selected_role == "student":
        return query_one(
            connection,
            """
            SELECT
                id, email, '' AS phone, password, name AS full_name, 'student' AS role, token, avatar_data,
                department, programme, group_id, group_name, subgroup, language, '' AS teaching_languages
            FROM students
            WHERE lower(email) = lower(?)
            """,
            (normalized,),
        )

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
        with get_connection() as connection:
            user = _find_account_by_token(connection, token)

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
        with get_connection() as connection:
            existing_teacher = (
                _find_teacher_by_email(connection, email) if role == "teacher" else None
            )
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

                selected_group = query_one(
                    connection,
                    """
                    SELECT
                        g.id,
                        g.name,
                        g.student_count,
                        g.has_subgroups,
                        g.language,
                        CASE
                            WHEN EXISTS (
                                SELECT 1
                                FROM schedules s
                                WHERE s.group_id = g.id
                                  AND trim(coalesce(s.subgroup, '')) <> ''
                            )
                            THEN 1
                            ELSE 0
                        END AS auto_has_subgroups
                    FROM groups g
                    WHERE g.id = ?
                    """,
                    (group_id,),
                )
                if selected_group is None:
                    raise ApiError(400, "bad_request", "Выбрана некорректная группа")
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

                _enforce_student_group_capacity(connection, selected_group, subgroup)

            if role == "teacher" and existing_teacher and not _is_teacher_claimed(existing_teacher):
                raise ApiError(
                    400,
                    "teacher_claim_required",
                    "Для импортированного преподавателя нужно подтвердить аккаунт через поиск и код.",
                )

            if _email_exists(connection, email):
                raise ApiError(
                    400,
                    "email_already_exists",
                    "Пользователь с таким email уже существует",
                )
            token = secrets.token_urlsafe(32)
            if role == "teacher":
                user_id = insert_and_get_id(
                    connection,
                    """
                    INSERT INTO teachers (
                        name, email, password, token, avatar_data, phone, subject_taught,
                        weekly_hours_limit, teaching_languages, name_normalized, name_signature
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["displayName"],
                        email,
                        hash_password(payload["password"]),
                        token,
                        None,
                        phone,
                        subject_taught,
                        None,
                        ",".join(teaching_languages),
                        normalize_teacher_name(payload["displayName"]),
                        build_teacher_name_signature(payload["displayName"]),
                    ),
                )
                user = query_one(
                    connection,
                    """
                    SELECT
                        id, email, phone, name AS full_name, 'teacher' AS role, token, avatar_data,
                        '' AS department, subject_taught, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup, '' AS language, teaching_languages
                    FROM teachers
                    WHERE id = ?
                    """,
                    (user_id,),
                )
            else:
                user_id = insert_and_get_id(
                    connection,
                    """
                    INSERT INTO students (
                        name, email, password, token, avatar_data, department, programme, group_id, group_name, subgroup, language
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["displayName"],
                        email,
                        hash_password(payload["password"]),
                        token,
                        None,
                        department,
                        programme_name,
                        selected_group["id"],
                        selected_group["name"],
                        subgroup,
                        student_language,
                    ),
                )
                user = query_one(
                    connection,
                    """
                    SELECT
                        id, email, '' AS phone, name AS full_name, 'student' AS role, token, avatar_data,
                        department, programme, group_id, group_name, subgroup, language, '' AS teaching_languages
                    FROM students
                    WHERE id = ?
                    """,
                    (user_id,),
                )
            connection.commit()
            return _sanitize_profile_user(connection, user)


def get_current_profile(headers):
    user = require_auth_user(headers)
    with DB_LOCK:
        with get_connection() as connection:
            return _sanitize_profile_user(connection, user)


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
        with get_connection() as connection:
            if user["role"] == "admin":
                db_execute(
                    connection,
                    """
                    UPDATE users
                    SET avatar_data = ?
                    WHERE id = ?
                    """,
                    (avatar_data, user["id"]),
                )
            elif user["role"] == "teacher":
                db_execute(
                    connection,
                    """
                    UPDATE teachers
                    SET avatar_data = ?
                    WHERE id = ?
                    """,
                    (avatar_data, user["id"]),
                )
            else:
                db_execute(
                    connection,
                    """
                    UPDATE students
                    SET avatar_data = ?
                    WHERE id = ?
                    """,
                    (avatar_data, user["id"]),
                )
            connection.commit()
            updated_user = _find_account_by_token(connection, user["token"])

    return _sanitize_profile_user(connection, updated_user)


def login_user(payload):
    email = payload.get("email", "").strip()
    password = payload.get("password", "")
    selected_role = (payload.get("role") or "").strip().lower()

    if selected_role and selected_role not in {"admin", "student", "teacher"}:
        raise ApiError(400, "invalid_role", "Некорректная роль")

    with DB_LOCK:
        with get_connection() as connection:
            user = _find_login_account(connection, email, selected_role or None)

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
            if user["role"] == "admin":
                db_execute(
                    connection,
                    "UPDATE users SET token = ?, password = ? WHERE id = ?",
                    (token, password_hash, user["id"]),
                )
            elif user["role"] == "teacher":
                db_execute(
                    connection,
                    "UPDATE teachers SET token = ?, password = ? WHERE id = ?",
                    (token, password_hash, user["id"]),
                )
            else:
                db_execute(
                    connection,
                    "UPDATE students SET token = ?, password = ? WHERE id = ?",
                    (token, password_hash, user["id"]),
                )
            connection.commit()

            user["token"] = token
            user["password"] = password_hash

    return sanitize_user(user)


def logout_user(headers):
    user = require_auth_user(headers)

    with DB_LOCK:
        with get_connection() as connection:
            if user["role"] == "admin":
                db_execute(connection, "UPDATE users SET token = '' WHERE id = ?", (user["id"],))
            elif user["role"] == "teacher":
                db_execute(connection, "UPDATE teachers SET token = '' WHERE id = ?", (user["id"],))
            else:
                db_execute(connection, "UPDATE students SET token = '' WHERE id = ?", (user["id"],))
            connection.commit()

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
        with get_connection() as connection:
            rows = query_all(
                connection,
                """
                SELECT id, name, email, phone, teaching_languages
                FROM teachers
                WHERE COALESCE(password, '') = '' AND COALESCE(token, '') = ''
                ORDER BY name, id
                """,
            )

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
        with get_connection() as connection:
            teacher = _find_teacher_by_id(connection, teacher_id)
            if teacher is None:
                raise ApiError(404, "record_not_found", "Преподаватель не найден.")
            if _is_teacher_claimed(teacher):
                raise ApiError(
                    400,
                    "teacher_claim_already_completed",
                    "Этот аккаунт преподавателя уже активирован.",
                )
            email = teacher["email"].strip().lower()
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
                db_execute(
                    connection,
                    "UPDATE teachers SET email = ? WHERE id = ?",
                    (email, teacher["id"]),
                )
            else:
                ensure_teacher_email_allowed(email, "teacher")

            claim_code = f"{secrets.randbelow(1_000_000):06d}"
            expires_at = (_utc_now() + timedelta(minutes=10)).isoformat()
            requested_at = _utc_now_iso()
            db_execute(
                connection,
                """
                UPDATE teachers
                SET claim_code = ?, claim_code_expires_at = ?, claim_requested_at = ?
                WHERE id = ?
                """,
                (claim_code, expires_at, requested_at, teacher["id"]),
            )
            connection.commit()

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
        with get_connection() as connection:
            teacher = _find_teacher_by_id(connection, teacher_id)
            if teacher is None:
                raise ApiError(404, "record_not_found", "Преподаватель не найден.")
            if _is_teacher_claimed(teacher):
                raise ApiError(
                    400,
                    "teacher_claim_already_completed",
                    "Этот аккаунт преподавателя уже активирован.",
                )
            email = teacher["email"].strip().lower()
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
                db_execute(
                    connection,
                    "UPDATE teachers SET email = ? WHERE id = ?",
                    (email, teacher["id"]),
                )
            else:
                ensure_teacher_email_allowed(email, "teacher")
            if not teacher.get("claim_code") or teacher["claim_code"] != code:
                raise ApiError(
                    400,
                    "teacher_claim_code_invalid",
                    "Код подтверждения неверный.",
                )

            expires_at_raw = teacher.get("claim_code_expires_at")
            if not expires_at_raw:
                raise ApiError(
                    400,
                    "teacher_claim_code_expired",
                    "Срок действия кода истёк. Запросите новый код.",
                )
            expires_at = datetime.fromisoformat(expires_at_raw)
            if expires_at < _utc_now():
                _clear_teacher_claim_state(connection, teacher["id"])
                connection.commit()
                raise ApiError(
                    400,
                    "teacher_claim_code_expired",
                    "Срок действия кода истёк. Запросите новый код.",
                )

            token = secrets.token_urlsafe(32)
            db_execute(
                connection,
                """
                UPDATE teachers
                SET password = ?, token = ?, claim_code = NULL, claim_code_expires_at = NULL, claim_requested_at = NULL
                WHERE id = ?
                """,
                (hash_password(password), token, teacher["id"]),
            )
            connection.commit()

    return {"success": True}
