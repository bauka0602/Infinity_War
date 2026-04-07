import hashlib

from .config import PASSWORD_PREFIX


def hash_password(password):
    digest = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return f"{PASSWORD_PREFIX}{digest}"


def verify_password(stored_password, plain_password):
    if not stored_password:
        return False
    if stored_password.startswith(PASSWORD_PREFIX):
        return stored_password == hash_password(plain_password)
    return stored_password == plain_password


def sanitize_user(row):
    return {
        "id": row["id"],
        "email": row["email"],
        "displayName": row.get("full_name") or row.get("name") or "",
        "role": row["role"],
        "token": row["token"],
        "avatarData": row.get("avatar_data"),
        "department": row.get("department", ""),
        "programmeName": row.get("programme", ""),
        "groupId": row.get("group_id"),
        "groupName": row.get("group_name", ""),
        "subgroup": row.get("subgroup", ""),
    }


def parse_bearer_token(header_value):
    if not header_value:
        return None
    parts = header_value.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None
