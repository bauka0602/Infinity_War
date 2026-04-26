import importlib
import os
import sys
import time

import pytest
from fastapi.testclient import TestClient


MODULE_PREFIXES = ("backend.app", "backend.server")


def _reload_backend(test_db_path):
    os.environ["DATABASE_URL"] = ""
    os.environ["SQLITE_DB_FILE"] = str(test_db_path)
    os.environ["EXPOSE_DEV_CLAIM_CODE"] = "true"

    for module_name in list(sys.modules):
        if module_name.startswith(MODULE_PREFIXES):
            sys.modules.pop(module_name, None)

    db_module = importlib.import_module("backend.app.db")
    db_module.ensure_database()
    app_module = importlib.import_module("backend.app.fastapi_app")
    return app_module, db_module


@pytest.fixture
def backend_modules(tmp_path):
    test_db_path = tmp_path / "test_timetable.db"
    app_module, db_module = _reload_backend(test_db_path)
    return app_module, db_module


@pytest.fixture
def client(backend_modules):
    app_module, _db_module = backend_modules
    with TestClient(app_module.app) as test_client:
        yield test_client


@pytest.fixture
def admin_auth_headers(client):
    response = client.post(
        "/api/auth/login",
        json={
            "email": "admin@kazatu.edu.kz",
            "password": "admin123",
            "role": "admin",
        },
    )
    assert response.status_code == 200
    token = response.json()["token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def seeded_teacher_request(backend_modules):
    _app_module, db_module = backend_modules
    with db_module.get_connection() as connection:
        teacher_id = db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO teachers (
                name, email, password, token, avatar_data, phone, subject_taught, weekly_hours_limit, teaching_languages
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Teacher Test",
                "teacher.test@kazatu.edu.kz",
                "",
                "",
                "",
                "",
                "Test Department",
                20,
                "ru,kk",
            ),
        )
        request_id = db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO teacher_preference_requests (
                teacher_id, teacher_name, preferred_day, preferred_hour, note, status, admin_comment, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            (
                teacher_id,
                "Teacher Test",
                "monday",
                8,
                "Morning preferred",
                "pending",
                "",
            ),
        )
        connection.commit()

    return {"teacher_id": teacher_id, "request_id": request_id}


@pytest.fixture
def seeded_claimable_teacher(backend_modules):
    _app_module, db_module = backend_modules
    with db_module.get_connection() as connection:
        teacher_id = db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO teachers (
                name, email, password, token, avatar_data, phone, subject_taught, weekly_hours_limit, teaching_languages
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Claimable Teacher",
                "claimable.teacher@kazatu.edu.kz",
                "",
                "",
                "",
                "+77001112233",
                "CS",
                18,
                "ru,kk",
            ),
        )
        connection.commit()
    return {"teacher_id": teacher_id}


@pytest.fixture
def seeded_group(backend_modules):
    _app_module, db_module = backend_modules
    with db_module.get_connection() as connection:
        group_id = db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO groups (name, student_count, has_subgroups, language, study_course)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("SE-24-01", 24, 0, "ru", 2),
        )
        connection.commit()
    return {"group_id": group_id}


@pytest.fixture
def seeded_teacher_account(backend_modules):
    _app_module, db_module = backend_modules
    security_module = importlib.import_module("backend.app.security")
    with db_module.get_connection() as connection:
        teacher_id = db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO teachers (
                name, email, password, token, avatar_data, phone, subject_taught, weekly_hours_limit, teaching_languages
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Active Teacher",
                "active.teacher@kazatu.edu.kz",
                security_module.hash_password("teacher123"),
                "",
                "",
                "+77002223344",
                "CS",
                20,
                "ru,kk",
            ),
        )
        connection.commit()
    return {"teacher_id": teacher_id, "email": "active.teacher@kazatu.edu.kz", "password": "teacher123"}


@pytest.fixture
def teacher_auth_headers(client, seeded_teacher_account):
    response = client.post(
        "/api/auth/login",
        json={
            "email": seeded_teacher_account["email"],
            "password": seeded_teacher_account["password"],
            "role": "teacher",
        },
    )
    assert response.status_code == 200
    token = response.json()["token"]
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def seeded_notification(backend_modules):
    _app_module, db_module = backend_modules
    with db_module.get_connection() as connection:
        notification_id = db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO notifications (
                recipient_role, recipient_id, title, message, metadata, notification_type, is_read, created_at, read_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), NULL)
            """,
            (
                "teacher",
                1,
                "Schedule changed",
                "A lesson was updated",
                "{}",
                "schedule_changed",
                0,
            ),
        )
        connection.commit()
    return {"notification_id": notification_id}


def wait_for_job_completion(client, headers, job_id, timeout_seconds=5):
    started_at = time.time()
    while time.time() - started_at < timeout_seconds:
        response = client.get(
            f"/api/schedules/generate/{job_id}",
            headers=headers,
        )
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] in {"completed", "failed"}:
            return payload
        time.sleep(0.1)
    raise AssertionError(f"Job {job_id} did not finish within {timeout_seconds} seconds")
