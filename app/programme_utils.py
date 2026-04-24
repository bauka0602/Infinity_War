import re


def normalize_programme_text(value):
    normalized = str(value or "").strip().lower()
    normalized = re.sub(r"\s*\([^)]*\)\s*$", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def same_programme(left, right):
    left_normalized = normalize_programme_text(left)
    right_normalized = normalize_programme_text(right)
    return bool(left_normalized and right_normalized and left_normalized == right_normalized)
