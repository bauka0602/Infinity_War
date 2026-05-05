import importlib
import os
import sys
import time
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select


MODULE_PREFIXES = ("backend.app", "backend.server")


def _reload_backend(test_db_path):
    os.environ["DATABASE_URL"] = ""
    os.environ["SQLITE_DB_FILE"] = str(test_db_path)
    os.environ["EXPOSE_DEV_CLAIM_CODE"] = "true"

    for module_name in list(sys.modules):
        if module_name.startswith(MODULE_PREFIXES):
            sys.modules.pop(module_name, None)

    db_module = importlib.import_module("backend.app.core.db")
    db_module.ensure_database()
    app_module = importlib.import_module("backend.app.api.app")
    return app_module, db_module


@pytest.fixture
def backend_modules(tmp_path):
    test_db_path = tmp_path / "test_timetable.db"
    app_module, db_module = _reload_backend(test_db_path)
    return app_module, db_module


class OrmTestHelper:
    def __init__(self):
        self.orm_module = importlib.import_module("backend.app.core.orm")
        self.models = importlib.import_module("backend.app.models")

    def model(self, name):
        return getattr(self.models, name)

    def add(self, model_name, **values):
        model = self.model(model_name)
        with self.orm_module.SessionLocal() as session:
            row = model(**values)
            session.add(row)
            session.commit()
            return row

    def get(self, model_name, item_id):
        model = self.model(model_name)
        with self.orm_module.SessionLocal() as session:
            return session.get(model, item_id)

    def one(self, model_name, **filters):
        model = self.model(model_name)
        statement = select(model)
        for field, value in filters.items():
            statement = statement.where(getattr(model, field) == value)
        with self.orm_module.SessionLocal() as session:
            return session.scalar(statement)

    def list(self, model_name, *order_fields, **filters):
        model = self.model(model_name)
        statement = select(model)
        for field, value in filters.items():
            statement = statement.where(getattr(model, field) == value)
        if order_fields:
            statement = statement.order_by(*(getattr(model, field) for field in order_fields))
        with self.orm_module.SessionLocal() as session:
            return list(session.scalars(statement).all())

    def count(self, model_name):
        model = self.model(model_name)
        with self.orm_module.SessionLocal() as session:
            return int(session.scalar(select(func.count()).select_from(model)) or 0)


@pytest.fixture
def orm(backend_modules):
    _app_module, _db_module = backend_modules
    return OrmTestHelper()


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
    _app_module, _db_module = backend_modules
    helper = OrmTestHelper()
    teacher = helper.add(
        "Teacher",
        name="Teacher Test",
        email="teacher.test@kazatu.edu.kz",
        password="",
        token="",
        avatar_data="",
        phone="",
        subject_taught="Test Department",
        weekly_hours_limit=20,
        teaching_languages="ru,kk",
    )
    now = datetime.now(timezone.utc).isoformat()
    request = helper.add(
        "TeacherPreferenceRequest",
        teacher_id=teacher.id,
        teacher_name="Teacher Test",
        preferred_day="monday",
        preferred_hour=8,
        note="Morning preferred",
        status="pending",
        admin_comment="",
        created_at=now,
        updated_at=now,
    )

    return {"teacher_id": teacher.id, "request_id": request.id}


@pytest.fixture
def seeded_claimable_teacher(backend_modules):
    _app_module, _db_module = backend_modules
    teacher = OrmTestHelper().add(
        "Teacher",
        name="Claimable Teacher",
        email="claimable.teacher@kazatu.edu.kz",
        password="",
        token="",
        avatar_data="",
        phone="+77001112233",
        subject_taught="CS",
        weekly_hours_limit=18,
        teaching_languages="ru,kk",
    )
    return {"teacher_id": teacher.id}


@pytest.fixture
def seeded_group(backend_modules):
    _app_module, _db_module = backend_modules
    group = OrmTestHelper().add(
        "Group",
        name="SE-24-01",
        student_count=24,
        has_subgroups=0,
        language="ru",
        study_course=2,
    )
    return {"group_id": group.id}


@pytest.fixture
def seeded_teacher_account(backend_modules):
    _app_module, _db_module = backend_modules
    security_module = importlib.import_module("backend.app.auth.security")
    teacher = OrmTestHelper().add(
        "Teacher",
        name="Active Teacher",
        email="active.teacher@kazatu.edu.kz",
        password=security_module.hash_password("teacher123"),
        token="",
        avatar_data="",
        phone="+77002223344",
        subject_taught="CS",
        weekly_hours_limit=20,
        teaching_languages="ru,kk",
    )
    return {"teacher_id": teacher.id, "email": "active.teacher@kazatu.edu.kz", "password": "teacher123"}


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
    _app_module, _db_module = backend_modules
    notification = OrmTestHelper().add(
        "Notification",
        recipient_role="teacher",
        recipient_id=1,
        title="Schedule changed",
        message="A lesson was updated",
        metadata_json="{}",
        notification_type="schedule_changed",
        is_read=0,
        created_at=datetime.now(timezone.utc).isoformat(),
        read_at=None,
    )
    return {"notification_id": notification.id}


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
