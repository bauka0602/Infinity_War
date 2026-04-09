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
