import base64
import time
from io import BytesIO

from openpyxl import Workbook, load_workbook


def _wait_for_job_completion(client, headers, job_id, timeout_seconds=5):
    started_at = time.time()
    while time.time() - started_at < timeout_seconds:
        response = client.get(f"/api/schedules/generate/{job_id}", headers=headers)
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in {"completed", "failed"}:
            return payload
        time.sleep(0.1)
    raise AssertionError(f"Job {job_id} did not finish within {timeout_seconds} seconds")


def _build_excel_import_payload():
    workbook = Workbook()
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    teachers = workbook.create_sheet("Teachers")
    teachers.append(["name", "email", "phone", "department", "teaching_languages"])
    teachers.append(
        [
            "Aruzhan Saparova",
            "aruzhan.saparova@kazatu.edu.kz",
            "+7 777 000 00 00",
            "CS",
            "ru,kk",
        ]
    )

    disciplines = workbook.create_sheet("Disciplines")
    disciplines.append(
        [
            "code",
            "name",
            "course",
            "semester",
            "programme",
            "department",
            "instructor_name",
            "requires_computers",
        ]
    )
    disciplines.append(
        [
            "CS201",
            "Algorithms",
            2,
            1,
            "Software Engineering",
            "CS",
            "Aruzhan Saparova",
            "no",
        ]
    )

    rooms = workbook.create_sheet("Rooms")
    rooms.append(["number", "capacity", "building", "type", "department", "available"])
    rooms.append(["401", 40, "Main", "lecture", "CS", "yes"])

    groups = workbook.create_sheet("Groups")
    groups.append(["name", "student_count", "study_course", "has_subgroups", "language"])
    groups.append(["SE-24-01", 24, 2, "no", "ru"])

    sections = workbook.create_sheet("Sections")
    sections.append(["course_code", "group_name", "classes_count", "lesson_type"])
    sections.append(["CS201", "SE-24-01", 2, "lecture"])

    buffer = BytesIO()
    workbook.save(buffer)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return {"fileName": "import-success.xlsx", "fileContent": encoded}


def _build_rop_preview_payload():
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Лист1"

    def row_with(values):
        row = [""] * 26
        for index, value in values.items():
            row[index] = value
        sheet.append(row)

    row_with({0: 'РАБОЧИЙ УЧЕБНЫЙ ПЛАН на 2025-2026 учебные годы'})
    row_with({0: 'для Модульной образовательной программы “Бизнес-информатика”'})
    row_with({0: 'Год поступления: 01-09-2024'})
    row_with({})
    row_with(
        {
            0: "№ модуля",
            1: "Тип модуля",
            2: "Наименование модуля",
            3: "Цикл дисциплины",
            4: "Компонент дисциплины",
            5: "Код дисциплины",
            6: "Наименование дисциплины",
            7: "Академические кредиты",
            9: "Экзамены (семестр)*",
            10: "Распределение часов",
        }
    )
    row_with({10: "3 Академический период", 18: "4 Академический период"})
    row_with(
        {
            10: "Всего",
            11: "Лекции",
            12: "Практические",
            13: "Лабораторные",
            18: "Всего",
            19: "Лекции",
            20: "Практические",
            21: "Лабораторные",
        }
    )
    row_with(
        {
            0: 1,
            1: "Общие модули",
            2: "Гуманитарно-социальный",
            3: "ООД",
            4: "ОК",
            5: "Fil 2108",
            6: "Философия",
            7: 5,
            9: 1,
            10: 150,
            11: 15,
            12: 30,
            16: 20,
            17: 85,
        }
    )

    buffer = BytesIO()
    workbook.save(buffer)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return {"fileName": "БИ_2_рус.xlsx", "fileContent": encoded}


def test_excel_import_success_flow(client, admin_auth_headers):
    response = client.post(
        "/api/import/excel",
        headers=admin_auth_headers,
        json=_build_excel_import_payload(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"] == "Excel import completed successfully."
    assert set(payload["recognizedSheets"]) == {
        "Teachers",
        "Disciplines",
        "Rooms",
        "Groups",
        "Sections",
    }
    assert payload["totals"] == {"inserted": 5, "updated": 0}

    teachers_response = client.get("/api/teachers", headers=admin_auth_headers)
    assert teachers_response.status_code == 200
    assert any(item["email"] == "aruzhan.saparova@kazatu.edu.kz" for item in teachers_response.json())

    sections_response = client.get("/api/sections", headers=admin_auth_headers)
    assert sections_response.status_code == 200
    assert len(sections_response.json()) == 1
    assert sections_response.json()[0]["course_name"] == "Algorithms"


def test_rop_preview_parses_curriculum_plan(client, admin_auth_headers):
    response = client.post(
        "/api/import/rop/preview",
        headers=admin_auth_headers,
        json=_build_rop_preview_payload(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["metadata"]["programme"] == "Бизнес-информатика"
    assert payload["metadata"]["academicYear"] == "2025-2026"
    assert payload["metadata"]["entryYear"] == "01-09-2024"
    assert payload["metadata"]["language"] == "ru"
    assert payload["metadata"]["studyYear"] == 2
    assert payload["metadata"]["academicPeriods"] == [3, 4]
    assert payload["totals"]["courses"] == 1
    assert payload["totals"]["offerings"] == 1
    assert payload["totals"]["lessonComponents"] == 2
    assert payload["courses"][0]["code"] == "Fil 2108"
    assert payload["offerings"][0]["semester"] == 1
    assert {item["lessonType"] for item in payload["lessonComponents"]} == {
        "lecture",
        "practical",
    }
    computer_flags = {
        item["lessonType"]: item["requiresComputers"]
        for item in payload["lessonComponents"]
    }
    assert computer_flags == {"lecture": False, "practical": True}


def test_rop_import_creates_courses_from_curriculum_plan(client, admin_auth_headers):
    response = client.post(
        "/api/import/rop",
        headers=admin_auth_headers,
        json=_build_rop_preview_payload(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["courses"] == {"inserted": 1, "updated": 0}

    courses_response = client.get("/api/disciplines", headers=admin_auth_headers)
    assert courses_response.status_code == 200
    courses = courses_response.json()
    assert len(courses) == 1
    assert courses[0]["code"] == "Fil 2108"
    assert courses[0]["name"] == "Философия"
    assert courses[0]["credits"] == 5
    assert courses[0]["hours"] == 150
    assert courses[0]["year"] == 2
    assert courses[0]["semester"] == 3
    assert courses[0]["programme"] == "Бизнес-информатика"
    assert courses[0]["cycle"] == "ООД"
    assert courses[0]["component"] == "ОК"
    assert courses[0]["academic_year"] == "2025-2026"
    assert courses[0]["entry_year"] == "01-09-2024"


def test_schedule_generation_success_flow_with_export(client, admin_auth_headers):
    import_response = client.post(
        "/api/import/excel",
        headers=admin_auth_headers,
        json=_build_excel_import_payload(),
    )
    assert import_response.status_code == 200

    generate_response = client.post(
        "/api/schedules/generate",
        headers=admin_auth_headers,
        json={"semester": 1, "year": 2026, "algorithm": "optimizer"},
    )

    assert generate_response.status_code == 202
    job_id = generate_response.json()["jobId"]

    final_job = _wait_for_job_completion(client, admin_auth_headers, job_id)
    assert final_job["status"] == "completed"
    assert final_job["result"]["scheduleCount"] == 2

    schedules_response = client.get("/api/schedules", headers=admin_auth_headers)
    assert schedules_response.status_code == 200
    schedules = schedules_response.json()
    assert len(schedules) == 2
    assert {item["course_name"] for item in schedules} == {"Algorithms"}
    assert {item["group_name"] for item in schedules} == {"SE-24-01"}
    assert {item["teacher_name"] for item in schedules} == {"Aruzhan Saparova"}
    assert {item["room_number"] for item in schedules} == {"401"}
    assert {item["semester"] for item in schedules} == {1}
    assert {item["year"] for item in schedules} == {2026}

    export_response = client.get("/api/export/schedule", headers=admin_auth_headers)
    assert export_response.status_code == 200

    workbook = load_workbook(filename=BytesIO(export_response.content), data_only=True)
    sheet = workbook["Schedule"]
    rows = list(sheet.iter_rows(values_only=True))

    assert rows[0] == (
        "course_name",
        "group_name",
        "subgroup",
        "teacher_name",
        "room_number",
        "day",
        "start_hour",
        "semester",
        "year",
        "algorithm",
    )
    assert len(rows) == 3
    assert all(row[0] == "Algorithms" for row in rows[1:])
    assert all(row[1] == "SE-24-01" for row in rows[1:])
    assert all(row[4] == "401" for row in rows[1:])
