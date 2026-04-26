import hashlib
import hmac
import secrets

from .config import PASSWORD_PREFIX

PBKDF2_PREFIX = "pbkdf2_sha256$"
PBKDF2_ITERATIONS = 260_000


def hash_password(password):
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PBKDF2_ITERATIONS,
    ).hex()
    return f"{PBKDF2_PREFIX}{PBKDF2_ITERATIONS}${salt}${digest}"


def verify_password(stored_password, plain_password):
    if not stored_password:
        return False
    if stored_password.startswith(PBKDF2_PREFIX):
        try:
            _prefix, iterations, salt, digest = stored_password.split("$", 3)
            candidate = hashlib.pbkdf2_hmac(
                "sha256",
                plain_password.encode("utf-8"),
                salt.encode("utf-8"),
                int(iterations),
            ).hex()
            return hmac.compare_digest(digest, candidate)
        except (TypeError, ValueError):
            return False
    if stored_password.startswith(PASSWORD_PREFIX):
        legacy_digest = hashlib.sha256(plain_password.encode("utf-8")).hexdigest()
        return hmac.compare_digest(stored_password, f"{PASSWORD_PREFIX}{legacy_digest}")
    return False


def needs_password_rehash(stored_password):
    return not str(stored_password or "").startswith(PBKDF2_PREFIX)


def sanitize_user(row):
    return {
        "id": row["id"],
        "email": row["email"],
        "phone": row.get("phone", ""),
        "displayName": row.get("full_name") or row.get("name") or "",
        "role": row["role"],
        "token": row["token"],
        "avatarData": row.get("avatar_data"),
        "department": row.get("department", ""),
        "subjectTaught": row.get("subject_taught", ""),
        "programmeName": row.get("programme", ""),
        "groupId": row.get("group_id"),
        "groupName": row.get("group_name", ""),
        "subgroup": row.get("subgroup", ""),
        "language": row.get("language", ""),
        "teachingLanguages": row.get("teaching_languages", ""),
    }


def parse_bearer_token(header_value):
    if not header_value:
        return None
    parts = header_value.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None
