import secrets

from .config import DB_LOCK, TEACHER_EMAIL_DOMAIN
from .db import db_execute, get_connection, insert_and_get_id, query_one
from .errors import ApiError
from .security import hash_password, parse_bearer_token, sanitize_user, verify_password


def ensure_teacher_email_allowed(email, role):
    if role == "teacher" and not email.lower().endswith(TEACHER_EMAIL_DOMAIN):
        raise ApiError(
            400,
            "teacher_email_domain_required",
            "Для преподавателя нужен email, оканчивающийся на @kazatu.edu.kz",
        )

def _find_account_by_token(connection, token):
    admin = query_one(
        connection,
        """
        SELECT id, email, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup
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
            department, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup
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
            department, programme, group_id, group_name, subgroup
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
        SELECT id, email, password, token, name, phone, department, weekly_hours_limit, avatar_data
        FROM teachers
        WHERE lower(email) = lower(?)
        """,
        (normalized,),
    )


def _find_login_account(connection, email, selected_role):
    normalized = email.strip().lower()
    if selected_role == "admin":
        return query_one(
            connection,
            """
            SELECT id, email, password, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup
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
                department, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup
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
                department, programme, group_id, group_name, subgroup
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
        if student_missing:
            raise ApiError(
                400,
                "fill_required_fields",
                f"Заполните поля: {', '.join(student_missing)}",
                {"fields": student_missing},
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
                    SELECT id, name, has_subgroups
                    FROM groups
                    WHERE id = ?
                    """,
                    (group_id,),
                )
                if selected_group is None:
                    raise ApiError(400, "bad_request", "Выбрана некорректная группа")
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

            if _email_exists(connection, email) and not (
                role == "teacher"
                and existing_teacher
                and not existing_teacher.get("password")
                and not existing_teacher.get("token")
            ):
                raise ApiError(
                    400,
                    "email_already_exists",
                    "Пользователь с таким email уже существует",
                )

            token = secrets.token_urlsafe(32)
            if role == "teacher":
                if existing_teacher:
                    db_execute(
                        connection,
                        """
                        UPDATE teachers
                        SET name = ?, password = ?, token = ?
                        WHERE id = ?
                        """,
                        (
                            payload["displayName"],
                            hash_password(payload["password"]),
                            token,
                            existing_teacher["id"],
                        ),
                    )
                    user_id = existing_teacher["id"]
                else:
                    user_id = insert_and_get_id(
                        connection,
                        """
                        INSERT INTO teachers (
                            name, email, password, token, avatar_data, phone, department, weekly_hours_limit
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            payload["displayName"],
                            email,
                            hash_password(payload["password"]),
                            token,
                            None,
                            "",
                            "",
                            None,
                        ),
                    )
                user = query_one(
                    connection,
                    """
                    SELECT
                        id, email, name AS full_name, 'teacher' AS role, token, avatar_data,
                        department, '' AS programme, NULL AS group_id, '' AS group_name, '' AS subgroup
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
                        name, email, password, token, avatar_data, department, programme, group_id, group_name, subgroup
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ),
                )
                user = query_one(
                    connection,
                    """
                    SELECT
                        id, email, name AS full_name, 'student' AS role, token, avatar_data,
                        department, programme, group_id, group_name, subgroup
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
