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


def require_auth_user(headers):
    token = parse_bearer_token(headers.get("Authorization"))
    if not token:
        raise ApiError(401, "auth_required", "Требуется авторизация")

    with DB_LOCK:
        with get_connection() as connection:
            user = query_one(
                connection,
                """
                SELECT id, email, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup
                FROM users
                WHERE token = ?
                """,
                (token,),
            )

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

            existing = query_one(
                connection,
                "SELECT id FROM users WHERE lower(email) = lower(?)",
                (email,),
            )
            if existing:
                raise ApiError(
                    400,
                    "email_already_exists",
                    "Пользователь с таким email уже существует",
                )

            token = secrets.token_urlsafe(32)
            user_id = insert_and_get_id(
                connection,
                """
                INSERT INTO users (
                    email, password, full_name, role, token, avatar_data, department, programme, group_id, group_name, subgroup
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    email,
                    hash_password(payload["password"]),
                    payload["displayName"],
                    role,
                    token,
                    None,
                    department if role == "student" else "",
                    programme_name if role == "student" else "",
                    selected_group["id"] if role == "student" else None,
                    selected_group["name"] if role == "student" else "",
                    subgroup if role == "student" else "",
                ),
            )
            connection.commit()
            user = query_one(connection, "SELECT * FROM users WHERE id = ?", (user_id,))

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
            db_execute(
                connection,
                """
                UPDATE users
                SET avatar_data = ?
                WHERE id = ?
                """,
                (avatar_data, user["id"]),
            )
            connection.commit()
            updated_user = query_one(connection, "SELECT * FROM users WHERE id = ?", (user["id"],))

    return sanitize_user(updated_user)


def login_user(payload):
    email = payload.get("email", "").strip()
    password = payload.get("password", "")
    selected_role = (payload.get("role") or "").strip().lower()

    if selected_role and selected_role not in {"admin", "student", "teacher"}:
        raise ApiError(400, "invalid_role", "Некорректная роль")

    with DB_LOCK:
        with get_connection() as connection:
            user = query_one(
                connection,
                """
                SELECT * FROM users
                WHERE lower(email) = lower(?)
                """,
                (email,),
            )

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
