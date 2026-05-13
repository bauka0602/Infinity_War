CYRILLIC_TO_LATIN = {
    "а": "a",
    "ә": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "ғ": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "i",
    "к": "k",
    "қ": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "ң": "n",
    "о": "o",
    "ө": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ұ": "u",
    "ү": "u",
    "ф": "f",
    "х": "kh",
    "һ": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "shch",
    "ы": "y",
    "і": "i",
    "э": "e",
    "ю": "yu",
    "я": "ya",
    "ь": "",
    "ъ": "",
}


def transliterate_teacher_name(name):
    parts = []
    for char in str(name or ""):
        lower = char.lower()
        replacement = CYRILLIC_TO_LATIN.get(lower)
        if replacement is None:
            parts.append(char)
        elif char.isupper():
            parts.append(replacement.capitalize())
        else:
            parts.append(replacement)
    return " ".join("".join(parts).split())


def teacher_name_translations(name, name_kk=None, name_en=None):
    clean_name = " ".join(str(name or "").split())
    clean_name_kk = " ".join(str(name_kk or "").split()) or clean_name
    clean_name_en = " ".join(str(name_en or "").split()) or transliterate_teacher_name(clean_name)
    return {
        "ru": clean_name,
        "kk": clean_name_kk,
        "en": clean_name_en,
    }
