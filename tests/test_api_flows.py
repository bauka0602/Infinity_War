import time


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


def test_teacher_claim_flow(client, seeded_claimable_teacher):
    request_response = client.post(
        "/api/auth/teacher-claim/request",
        json={"teacherId": seeded_claimable_teacher["teacher_id"]},
    )

    assert request_response.status_code == 200
    request_payload = request_response.json()
    assert request_payload["success"] is True
    assert request_payload["debugCode"]

    confirm_response = client.post(
        "/api/auth/teacher-claim/confirm",
        json={
            "teacherId": seeded_claimable_teacher["teacher_id"],
            "code": request_payload["debugCode"],
            "password": "newteacher123",
        },
    )

    assert confirm_response.status_code == 200
    assert confirm_response.json()["success"] is True

    login_response = client.post(
        "/api/auth/login",
        json={
            "email": "claimable.teacher@kazatu.edu.kz",
            "password": "newteacher123",
            "role": "teacher",
        },
    )
    assert login_response.status_code == 200
    assert login_response.json()["role"] == "teacher"


def test_student_registration_flow(client, seeded_group):
    response = client.post(
        "/api/auth/register",
        json={
            "email": "student.one@example.com",
            "password": "student123",
            "displayName": "Student One",
            "role": "student",
            "department": "CS",
            "programmeName": "Software Engineering",
            "groupId": seeded_group["group_id"],
            "language": "ru",
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["role"] == "student"
    assert payload["groupId"] == seeded_group["group_id"]


def test_teacher_preference_lifecycle(client, teacher_auth_headers, admin_auth_headers):
    create_response = client.post(
        "/api/teacher-preferences",
        headers=teacher_auth_headers,
        json={
            "preferred_day": "monday",
            "preferred_hour": 9,
            "note": "Morning is better",
        },
    )

    assert create_response.status_code == 201
    request_payload = create_response.json()
    assert request_payload["status"] == "pending"

    admin_list_response = client.get("/api/teacher-preferences", headers=admin_auth_headers)
    assert admin_list_response.status_code == 200
    assert any(item["id"] == request_payload["id"] for item in admin_list_response.json())

    approve_response = client.put(
        f"/api/teacher-preferences/{request_payload['id']}/status",
        headers=admin_auth_headers,
        json={"status": "approved", "admin_comment": "Accepted"},
    )
    assert approve_response.status_code == 200
    assert approve_response.json()["status"] == "approved"


def test_notifications_mark_read_and_delete_all(client, teacher_auth_headers, seeded_notification):
    list_response = client.get("/api/notifications", headers=teacher_auth_headers)
    assert list_response.status_code == 200
    assert any(item["id"] == seeded_notification["notification_id"] for item in list_response.json()["items"])

    mark_read_response = client.put(
        f"/api/notifications/{seeded_notification['notification_id']}/read",
        headers=teacher_auth_headers,
    )
    assert mark_read_response.status_code == 200
    assert int(mark_read_response.json()["is_read"]) == 1

    delete_all_response = client.delete("/api/notifications", headers=teacher_auth_headers)
    assert delete_all_response.status_code == 200
    assert delete_all_response.json()["success"] is True
    assert delete_all_response.json()["unreadCount"] == 0


def test_schedule_export_fails_without_data(client, admin_auth_headers):
    export_response = client.get("/api/export/schedule", headers=admin_auth_headers)
    assert export_response.status_code == 400
    assert export_response.json()["errorCode"] == "bad_request"


def test_schedule_generation_job_fails_cleanly_without_data(client, admin_auth_headers):
    generate_response = client.post(
        "/api/schedules/generate",
        headers=admin_auth_headers,
        json={"semester": 1, "year": 2026, "algorithm": "greedy"},
    )

    assert generate_response.status_code == 202
    job_id = generate_response.json()["jobId"]

    final_job = wait_for_job_completion(client, admin_auth_headers, job_id)
    assert final_job["status"] == "failed"
    assert final_job["errorCode"] in {"schedule_generation_requires_data", "optimizer_requires_teachers"}
