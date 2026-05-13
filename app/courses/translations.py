DISCIPLINE_TRANSLATIONS = {
    "Иностранный язык": {"kk": "Шетел тілі", "en": "Foreign Language"},
    "История Казахстана": {"kk": "Қазақстан тарихы", "en": "History of Kazakhstan"},
    "Казахский (русский) язык": {"kk": "Қазақ (орыс) тілі", "en": "Kazakh (Russian) Language"},
    "Культурология и психология": {"kk": "Мәдениеттану және психология", "en": "Cultural Studies and Psychology"},
    "Информационнокоммуникационные технологии": {"kk": "Ақпараттық-коммуникациялық технологиялар", "en": "Information and Communication Technologies"},
    "Физика": {"kk": "Физика", "en": "Physics"},
    "Физическая культура.": {"kk": "Дене шынықтыру", "en": "Physical Education"},
    "Философия": {"kk": "Философия", "en": "Philosophy"},
    "Политология и социология": {"kk": "Саясаттану және әлеуметтану", "en": "Political Science and Sociology"},
    "Основы программирования": {"kk": "Бағдарламалау негіздері", "en": "Programming Fundamentals"},
    "Программирование на языке Python": {"kk": "Python тілінде бағдарламалау", "en": "Python Programming"},
    "Программирование на языке Java": {"kk": "Java тілінде бағдарламалау", "en": "Java Programming"},
    "Алгоритмы и структуры данных": {"kk": "Алгоритмдер және деректер құрылымдары", "en": "Algorithms and Data Structures"},
    "Алгоритмы и структуры данных I": {"kk": "Алгоритмдер және деректер құрылымдары I", "en": "Algorithms and Data Structures I"},
    "Алгоритмы и структуры данных II": {"kk": "Алгоритмдер және деректер құрылымдары II", "en": "Algorithms and Data Structures II"},
    "Операционные системы": {"kk": "Операциялық жүйелер", "en": "Operating Systems"},
    "Компьютерные сети": {"kk": "Компьютерлік желілер", "en": "Computer Networks"},
    "Информационная безопасность": {"kk": "Ақпараттық қауіпсіздік", "en": "Information Security"},
    "Кибербезопасность": {"kk": "Киберқауіпсіздік", "en": "Cybersecurity"},
    "Основы искусственного интеллекта": {"kk": "Жасанды интеллект негіздері", "en": "Fundamentals of Artificial Intelligence"},
    "Нейронные сети": {"kk": "Нейрондық желілер", "en": "Neural Networks"},
    "Методы машинного обучения": {"kk": "Машиналық оқыту әдістері", "en": "Machine Learning Methods"},
    "Анализ данных": {"kk": "Деректерді талдау", "en": "Data Analysis"},
    "Проектирование баз данных SQL": {"kk": "SQL деректер базасын жобалау", "en": "SQL Database Design"},
    "Проектирование программных систем": {"kk": "Бағдарламалық жүйелерді жобалау", "en": "Software Systems Design"},
    "Разработка мобильных приложений": {"kk": "Мобильді қосымшаларды әзірлеу", "en": "Mobile Application Development"},
    "Тестирование программного обеспечения": {"kk": "Бағдарламалық қамтамасыз етуді тестілеу", "en": "Software Testing"},
    "Управление IT проектами": {"kk": "IT жобаларды басқару", "en": "IT Project Management"},
    "Проектная работа": {"kk": "Жобалық жұмыс", "en": "Project Work"},
    "Производственная практика": {"kk": "Өндірістік практика", "en": "Industrial Internship"},
}


def discipline_name_translations(name):
    clean_name = str(name or "").strip()
    translation = DISCIPLINE_TRANSLATIONS.get(clean_name, {})
    return {
        "ru": clean_name,
        "kk": translation.get("kk") or clean_name,
        "en": translation.get("en") or clean_name,
    }


COURSE_META_TRANSLATIONS = {
    "Бизнес-информатика": {"kk": "Бизнес-информатика", "en": "Business Informatics"},
    "Компьютерная инженерия": {"kk": "Компьютерлік инженерия", "en": "Computer Engineering"},
    "Компьютерная инженерия (СОПР)": {
        "kk": "Компьютерлік инженерия (СОПР)",
        "en": "Computer Engineering (accelerated)",
    },
    "ООД": {"kk": "ЖББП", "en": "GED"},
    "БД": {"kk": "БП", "en": "BD"},
    "ПД": {"kk": "КП", "en": "PD"},
    "ОК": {"kk": "МК", "en": "RC"},
    "ВК": {"kk": "ЖООК", "en": "UC"},
    "КВ": {"kk": "ТК", "en": "EC"},
    "ДВО": {"kk": "ҚББ", "en": "AEC"},
    "УПП": {"kk": "ОӨП", "en": "TPP"},
    "B057 - Информационные технологии": {
        "kk": "B057 - Ақпараттық технологиялар",
        "en": "B057 - Information Technologies",
    },
    "Информационные технологии (B057)": {
        "kk": "Ақпараттық технологиялар (B057)",
        "en": "Information Technologies (B057)",
    },
}


def course_meta_translations(value):
    clean_value = str(value or "").strip()
    translation = COURSE_META_TRANSLATIONS.get(clean_value, {})
    return {
        "ru": clean_value,
        "kk": translation.get("kk") or clean_value,
        "en": translation.get("en") or clean_value,
    }
