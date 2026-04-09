import secrets
from datetime import datetime, timedelta, timezone

from .config import DB_LOCK, EXPOSE_DEV_CLAIM_CODE, TEACHER_EMAIL_DOMAIN
from .db import db_execute, get_connection, insert_and_get_id, query_all, query_one
from .errors import ApiError
from .security import hash_password, parse_bearer_token, sanitize_user, verify_password


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


def _utc_now():
    return datetime.now(timezone.utc)


def _utc_now_iso():
    return _utc_now().isoformat()


def _is_teacher_claimed(teacher):
    return bool(teacher.get("password") or teacher.get("token"))


def _serialize_claimable_teacher(row):
    email = row["email"]
    local_part, _, domain = email.partition("@")
    if len(local_part) <= 2:
        masked_local = local_part[:1] + "*"
    else:
        masked_local = local_part[:2] + "*" * max(1, len(local_part) - 2)
    return {
        "id": row["id"],
        "name": row["name"],
        "maskedEmail": f"{masked_local}@{domain}" if domain else masked_local,
        "teachingLanguages": row.get("teaching_languages", "") or "ru,kk",
    }

def _find_account_by_token(connection, token):
    admin = query_one(
        connection,
        """
        SELECT id, email, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup, '' AS language, '' AS teaching_languages
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
            id, email, name AS full_name, 'teacher' AS role, token, avatar_data,
            department, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup, '' AS language, teaching_languages
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
            id, email, name AS full_name, 'student' AS role, token, avatar_data,
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
        SELECT id, email, password, token, name, phone, department, weekly_hours_limit, avatar_data, teaching_languages
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
            id, email, password, token, name, phone, department, weekly_hours_limit,
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


def _find_login_account(connection, email, selected_role):
    normalized = email.strip().lower()
    if selected_role == "admin":
        return query_one(
            connection,
            """
            SELECT id, email, password, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup, '' AS language, '' AS teaching_languages
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
                id, email, password, name AS full_name, 'teacher' AS role, token, avatar_data,
                department, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup, '' AS language, teaching_languages
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
                id, email, password, name AS full_name, 'student' AS role, token, avatar_data,
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
    department = (payload.get("department") or "").strip()
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
                    SELECT id, name, has_subgroups, language
                    FROM groups
                    WHERE id = ?
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
                if selected_group.get("has_subgroups"):
                    if subgroup not in {"A", "B"}:
                        raise ApiError(
                            400,
                            "fill_required_fields",
                            "Заполните поля: subgroup",
                            {"fields": ["subgroup"]},
                        )
                else:
                    subgroup = ""

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
                        name, email, password, token, avatar_data, phone, department, weekly_hours_limit, teaching_languages
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["displayName"],
                        email,
                        hash_password(payload["password"]),
                        token,
                        None,
                        "",
                        department,
                        None,
                        ",".join(teaching_languages),
                    ),
                )
                user = query_one(
                    connection,
                    """
                    SELECT
                        id, email, name AS full_name, 'teacher' AS role, token, avatar_data,
                        department, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup, '' AS language, teaching_languages
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
                        id, email, name AS full_name, 'student' AS role, token, avatar_data,
                        department, programme, group_id, group_name, subgroup, language, '' AS teaching_languages
                    FROM students
                    WHERE id = ?
                    """,
                    (user_id,),
                )
            connection.commit()

    return sanitize_user(user)


def get_current_profile(headers):
    return sanitize_user(require_auth_user(headers))


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

    return sanitize_user(updated_user)


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

    search_patterns = []
    for raw_part in [search, *search.split()]:
        token = raw_part.strip()
        if len(token) < 2:
            continue
        pattern = f"%{token}%"
        if pattern not in search_patterns:
            search_patterns.append(pattern)

    if not search_patterns:
        return []

    where_parts = ["COALESCE(password, '') = ''", "COALESCE(token, '') = ''"]
    params = []
    search_clauses = []
    for pattern in search_patterns:
        search_clauses.append(
            "(lower(name) LIKE ? OR lower(email) LIKE ? OR lower(COALESCE(name, '') || ' ' || COALESCE(email, '')) LIKE ?)"
        )
        params.extend([pattern, pattern, pattern])
    where_parts.append(f"({' OR '.join(search_clauses)})")

    with DB_LOCK:
        with get_connection() as connection:
            rows = query_all(
                connection,
                f"""
                SELECT id, name, email, teaching_languages
                FROM teachers
                WHERE {' AND '.join(where_parts)}
                ORDER BY name, id
                LIMIT 10
                """,
                tuple(params),
            )
    return [_serialize_claimable_teacher(row) for row in rows]


def request_teacher_claim(payload):
    teacher_id = payload.get("teacherId")
    email = str(payload.get("email") or "").strip().lower()

    if not teacher_id or not email:
        raise ApiError(
            400,
            "fill_required_fields",
            "Заполните поля: teacherId, email",
            {"fields": ["teacherId", "email"]},
        )

    ensure_teacher_email_allowed(email, "teacher")

    with DB_LOCK:
        with get_connection() as connection:
            teacher = _find_teacher_by_id(connection, int(teacher_id))
            if teacher is None:
                raise ApiError(404, "record_not_found", "Преподаватель не найден.")
            if _is_teacher_claimed(teacher):
                raise ApiError(
                    400,
                    "teacher_claim_already_completed",
                    "Этот аккаунт преподавателя уже активирован.",
                )
            if teacher["email"].strip().lower() != email:
                raise ApiError(
                    400,
                    "teacher_claim_email_mismatch",
                    "Email не совпадает с записью преподавателя.",
                )

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
        "teacherId": int(teacher_id),
        "expiresAt": expires_at,
        "debugCode": claim_code if EXPOSE_DEV_CLAIM_CODE else None,
    }


def confirm_teacher_claim(payload):
    teacher_id = payload.get("teacherId")
    email = str(payload.get("email") or "").strip().lower()
    code = str(payload.get("code") or "").strip()
    password = payload.get("password") or ""

    missing = []
    if not teacher_id:
        missing.append("teacherId")
    if not email:
        missing.append("email")
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

    ensure_teacher_email_allowed(email, "teacher")

    with DB_LOCK:
        with get_connection() as connection:
            teacher = _find_teacher_by_id(connection, int(teacher_id))
            if teacher is None:
                raise ApiError(404, "record_not_found", "Преподаватель не найден.")
            if _is_teacher_claimed(teacher):
                raise ApiError(
                    400,
                    "teacher_claim_already_completed",
                    "Этот аккаунт преподавателя уже активирован.",
                )
            if teacher["email"].strip().lower() != email:
                raise ApiError(
                    400,
                    "teacher_claim_email_mismatch",
                    "Email не совпадает с записью преподавателя.",
                )
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
