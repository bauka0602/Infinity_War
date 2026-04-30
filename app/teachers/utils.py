import re


def normalize_teacher_name(value):
    normalized = str(value or "").strip().lower().replace("ё", "е")
    normalized = normalized.replace(".", "")
    return re.sub(r"\s+", "", normalized)


def build_teacher_name_signature(value):
    text = str(value or "").strip().lower().replace("ё", "е").replace(".", " ")
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return ""

    parts = text.split(" ")
    surname = parts[0]
    initials = []

    for part in parts[1:3]:
        if part:
            initials.append(part[0])

    return "|".join([surname, *initials])
