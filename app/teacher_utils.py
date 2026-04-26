import re


def normalize_teacher_name(value):
    normalized = str(value or "").strip().lower().replace("ё", "е")
    normalized = normalized.replace(".", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def build_teacher_name_signature(value):
    normalized = normalize_teacher_name(value)
    if not normalized:
        return ""

    parts = normalized.split(" ")
    surname = parts[0]
    initials = []

    for part in parts[1:3]:
        if part:
            initials.append(part[0])

    return "|".join([surname, *initials])
