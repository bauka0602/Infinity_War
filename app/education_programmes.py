import re

from .programme_utils import normalize_programme_text

EDUCATION_GROUPS = [
    {
        "value": "b031",
        "label": "Мода, дизайн (B031)",
        "programmes": [
            {"code": "6B02101", "label": "Дизайн"},
        ],
    },
    {
        "value": "b044",
        "label": "Менеджмент и управление (B044)",
        "programmes": [
            {"code": "6B04103", "label": "Управление бизнесом и предпринимательство"},
            {"code": "6B04105", "label": "Экономика современного бизнеса"},
        ],
    },
    {
        "value": "b045",
        "label": "Аудит и налогообложение (B045)",
        "programmes": [
            {"code": "6B04106", "label": "Бухгалтерский учет, аудит и налоговый консалтинг"},
        ],
    },
    {
        "value": "b046",
        "label": "Финансы, экономика, банковское и страховое дело (B046)",
        "programmes": [
            {"code": "6B04102", "label": "Финансовая аналитика"},
        ],
    },
    {
        "value": "b047",
        "label": "Маркетинг и реклама (B047)",
        "programmes": [
            {"code": "6B04104", "label": "Digital Маркетинг"},
        ],
    },
    {
        "value": "b050",
        "label": "Биологические и смежные науки (B050)",
        "programmes": [
            {"code": "6B05101", "label": "Сельскохозяйственная биотехнология"},
            {"code": "6B05102", "label": "Биотехнология"},
            {"code": "6B05103", "label": "Биология"},
        ],
    },
    {
        "value": "b051",
        "label": "Окружающая среда (B051)",
        "programmes": [
            {"code": "6B05201", "label": "Природопользование"},
            {"code": "6B05202", "label": "Агроэкология"},
        ],
    },
    {
        "value": "b057",
        "label": "Информационные технологии (B057)",
        "programmes": [
            {"code": "6B06101", "label": "Программная инженерия"},
            {"code": "6B06103", "label": "Компьютерная инженерия"},
            {"code": "6B06102", "label": "Бизнес-информатика"},
            {"code": "6B06115", "label": "Цифровые агросистемы и комплексы"},
            {"code": "6B06104", "label": "DevOps инжиниринг"},
        ],
    },
    {
        "value": "b059",
        "label": "Коммуникации и коммуникационные технологии (B059)",
        "programmes": [
            {"code": "6B06201", "label": "Телекоммуникационные сети и системы"},
            {"code": "6B06202", "label": "Радиотехника и электроника"},
        ],
    },
    {
        "value": "b062",
        "label": "Электротехника и энергетика (B062)",
        "programmes": [
            {"code": "6B07101", "label": "Теплоэнергетическая инженерия"},
            {"code": "6B07102", "label": "Электроэнергетика"},
            {"code": "6B07103", "label": "Электротехническая инженерия"},
            {"code": "6B07107", "label": "Теплогазоснабжение, вентиляция и экоинженерия в сельском хозяйстве"},
        ],
    },
    {
        "value": "b063",
        "label": "Электротехника и автоматизация (B063)",
        "programmes": [
            {"code": "6B07108", "label": "Автоматизация и энергетическая эффективность процессов и производств"},
        ],
    },
    {
        "value": "b064",
        "label": "Механика и металлообработка (B064)",
        "programmes": [
            {"code": "6B07104", "label": "Технологические машины и оборудование"},
            {"code": "6B07105", "label": "Механическая инженерия"},
        ],
    },
    {
        "value": "b065",
        "label": "Транспортная техника и технологии / Автотранспортные средства (B065)",
        "programmes": [
            {"code": "6B07106", "label": "Транспорт, транспортная техника и технологии"},
            {"code": "6B07111", "label": "Технический сервис автотранспортных средств (мастер производственного обучения)"},
        ],
    },
    {
        "value": "b068",
        "label": "Производство продуктов питания (B068)",
        "programmes": [
            {"code": "6B07201", "label": "Технология пищевых продуктов"},
        ],
    },
    {
        "value": "b073",
        "label": "Архитектура (B073)",
        "programmes": [
            {"code": "6B07301", "label": "Архитектура"},
        ],
    },
    {
        "value": "b074",
        "label": "Градостроительство, строительные работы и гражданское строительство (B074)",
        "programmes": [
            {"code": "6B07302", "label": "Геодезия и картография"},
            {"code": "6B07307", "label": "Геодезические работы в строительстве"},
            {"code": "6B07306", "label": "Геопространственная цифровая геодезия"},
        ],
    },
    {
        "value": "b075",
        "label": "Кадастр и землеустройство (B075)",
        "programmes": [
            {"code": "6B07303", "label": "Кадастр"},
            {"code": "6B07304", "label": "Землеустройство"},
        ],
    },
    {
        "value": "b076",
        "label": "Стандартизация, сертификация и метрология (B076)",
        "programmes": [
            {"code": "6B07501", "label": "Стандартизация, сертификация и метрология"},
        ],
    },
    {
        "value": "b077",
        "label": "Растениеводство (B077)",
        "programmes": [
            {"code": "6B08101", "label": "Агрономия"},
            {"code": "6B08102", "label": "Селекция и семеноводство"},
            {"code": "6B08103", "label": "Почвоведение и агрохимия"},
            {"code": "6B08104", "label": "Фитосанитарная безопасность"},
            {"code": "6B08105", "label": "Передовая агрономическая наука"},
            {"code": "6B08106", "label": "Агротехнология"},
        ],
    },
    {
        "value": "b078",
        "label": "Животноводство (B078)",
        "programmes": [
            {"code": "6B08201", "label": "Животноводство"},
            {"code": "6B08203", "label": "Птицеводство"},
            {"code": "6B08204", "label": "Зоотехния"},
        ],
    },
    {
        "value": "b079",
        "label": "Лесное хозяйство (B079)",
        "programmes": [
            {"code": "6B08301", "label": "Охотоведение и звероводство"},
            {"code": "6B08302", "label": "Ландшафтный дизайн и озеленение"},
            {"code": "6B08303", "label": "Защитное лесоразведение"},
            {"code": "6B08304", "label": "Лесные ресурсы и лесоводство"},
        ],
    },
    {
        "value": "b080",
        "label": "Рыбное хозяйство (B080)",
        "programmes": [
            {"code": "6B08401", "label": "Аквакультура и водные биоресурсы"},
        ],
    },
    {
        "value": "b082",
        "label": "Водные ресурсы и водопользование (B082)",
        "programmes": [],
    },
    {
        "value": "b083",
        "label": "Ветеринария (B083)",
        "programmes": [
            {"code": "6B09101", "label": "Ветеринарная безопасность"},
            {"code": "6B09102", "label": "Пищевая безопасность"},
            {"code": "6B09103", "label": "Ветеринария"},
        ],
    },
    {
        "value": "b095",
        "label": "Транспортные услуги (B095)",
        "programmes": [
            {"code": "6B11301", "label": "Логистика на транспорте"},
            {"code": "6B11302", "label": "Организация и безопасность дорожного движения"},
        ],
    },
    {
        "value": "b162",
        "label": "Теплоэнергетика (B162)",
        "programmes": [
            {"code": "6B07110", "label": "Теплоэнергетическая инженерия"},
        ],
    },
    {
        "value": "b174",
        "label": "Геодезия и картография (B174)",
        "programmes": [
            {"code": "6B07305", "label": "Геодезия и картография"},
        ],
    },
    {
        "value": "b183",
        "label": "Агроинженерия (B183)",
        "programmes": [
            {"code": "6B08701", "label": "Агроинженерия"},
            {"code": "6B08702", "label": "Энергообеспечение и автоматизация сельского хозяйства"},
        ],
    },
]

EDUCATION_GROUP_LABEL_BY_VALUE = {
    group["value"]: group["label"]
    for group in EDUCATION_GROUPS
}

PROGRAMME_CODE_TO_EDUCATION_GROUP = {
    programme["code"].upper(): group["value"]
    for group in EDUCATION_GROUPS
    for programme in group.get("programmes", [])
}

PROGRAMME_CODE_TO_LABEL = {
    programme["code"].upper(): programme["label"]
    for group in EDUCATION_GROUPS
    for programme in group.get("programmes", [])
}

HOME_ROOM_PROGRAMMES_BY_EDUCATION_GROUP = {
    "b031": ["Институт гуманитарных и педагогических наук"],
    "b044": ["Институт гуманитарных и педагогических наук"],
    "b045": ["Институт гуманитарных и педагогических наук"],
    "b046": ["Институт гуманитарных и педагогических наук"],
    "b047": ["Институт гуманитарных и педагогических наук"],
    "b050": ["Кафедра физики и химии"],
    "b051": ["Кафедра физики и химии"],
    "b057": ["Кафедра информационных систем", "Кафедра компьютерных наук"],
    "b059": ["Кафедра информационных систем", "Кафедра компьютерных наук"],
    "b062": ["Кафедра физики и химии"],
    "b063": ["Кафедра физики и химии"],
    "b064": ["Кафедра физики и химии"],
    "b065": ["Кафедра физики и химии"],
    "b068": ["Кафедра физики и химии"],
    "b073": ["Кафедра физики и химии"],
    "b074": ["Кафедра физики и химии"],
    "b075": ["Кафедра физики и химии"],
    "b076": ["Кафедра физики и химии"],
    "b077": ["Кафедра физики и химии"],
    "b078": ["Кафедра физики и химии"],
    "b079": ["Кафедра физики и химии"],
    "b080": ["Кафедра физики и химии"],
    "b082": ["Кафедра физики и химии"],
    "b083": ["Кафедра физики и химии"],
    "b095": ["Кафедра физики и химии"],
    "b162": ["Кафедра физики и химии"],
    "b174": ["Кафедра физики и химии"],
    "b183": ["Кафедра физики и химии"],
}

_NORMALIZED_GROUP_LABEL_TO_VALUE = {
    normalize_programme_text(label): value
    for value, label in EDUCATION_GROUP_LABEL_BY_VALUE.items()
}
_NORMALIZED_PROGRAMME_LABEL_TO_GROUP = {
    normalize_programme_text(programme["label"]): group["value"]
    for group in EDUCATION_GROUPS
    for programme in group.get("programmes", [])
}


def normalize_programme_code(value):
    return str(value or "").strip().upper()


def resolve_education_group_value(programme_value="", specialty_code=""):
    normalized_programme = normalize_programme_text(programme_value)
    if programme_value:
        stripped_programme = str(programme_value or "").strip().lower()
        if stripped_programme in EDUCATION_GROUP_LABEL_BY_VALUE:
            return stripped_programme
        code_match = re.search(r"\bb(\d{3})\b", stripped_programme)
        if code_match:
            inferred_value = f"b{code_match.group(1)}"
            if inferred_value in EDUCATION_GROUP_LABEL_BY_VALUE:
                return inferred_value
        if normalized_programme in _NORMALIZED_GROUP_LABEL_TO_VALUE:
            return _NORMALIZED_GROUP_LABEL_TO_VALUE[normalized_programme]
        if normalized_programme in _NORMALIZED_PROGRAMME_LABEL_TO_GROUP:
            return _NORMALIZED_PROGRAMME_LABEL_TO_GROUP[normalized_programme]

    normalized_code = normalize_programme_code(specialty_code)
    if normalized_code:
        return PROGRAMME_CODE_TO_EDUCATION_GROUP.get(normalized_code, "")
    return ""


def get_education_group_label(value):
    return EDUCATION_GROUP_LABEL_BY_VALUE.get(str(value or "").strip().lower(), str(value or "").strip())


def get_specialty_label(value):
    return PROGRAMME_CODE_TO_LABEL.get(normalize_programme_code(value), str(value or "").strip())


def get_home_room_programmes(programme_value="", specialty_code=""):
    group_value = resolve_education_group_value(programme_value, specialty_code)
    return HOME_ROOM_PROGRAMMES_BY_EDUCATION_GROUP.get(group_value, [])


def room_matches_home_programmes(room_programme, programme_value="", specialty_code=""):
    room_normalized = normalize_programme_text(room_programme)
    if not room_normalized:
        return False
    for home_programme in get_home_room_programmes(programme_value, specialty_code):
        if normalize_programme_text(home_programme) == room_normalized:
            return True
    return False
