import secrets

from .config import DB_LOCK, TEACHER_EMAIL_DOMAIN
from .db import get_connection, insert_and_get_id, query_one
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
                SELECT id, email, display_name, role, token
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
    if role not in {"student", "teacher"}:
        raise ApiError(
            400,
            "invalid_registration_role",
            "Можно зарегистрироваться только как студент или преподаватель",
        )

    ensure_teacher_email_allowed(email, role)

    with DB_LOCK:
        with get_connection() as connection:
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
                INSERT INTO users (email, password, display_name, role, token)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    email,
                    hash_password(payload["password"]),
                    payload["displayName"],
                    role,
                    token,
                ),
            )
            connection.commit()
            user = query_one(connection, "SELECT * FROM users WHERE id = ?", (user_id,))

    return sanitize_user(user)


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
