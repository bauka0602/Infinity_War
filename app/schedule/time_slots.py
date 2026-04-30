SCHEDULE_START_HOUR = 8
SCHEDULE_END_HOUR = 19
SCHEDULE_BOUNDARY_END_HOUR = 20
SCHEDULE_HOURS = list(range(SCHEDULE_START_HOUR, SCHEDULE_END_HOUR + 1))


def format_hour(hour):
    if hour in (None, ""):
        return ""
    return f"{int(hour):02d}:00"


def format_lesson_time_range(hour):
    if hour in (None, ""):
        return ""
    normalized_hour = int(hour)
    return f"{normalized_hour:02d}:00-{normalized_hour:02d}:50"
