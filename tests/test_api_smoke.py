def test_health_endpoint(client):
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["engine"] == "sqlite"


def test_admin_login_and_profile(client):
    login_response = client.post(
        "/api/auth/login",
        json={
            "email": "admin@kazatu.edu.kz",
            "password": "admin123",
            "role": "admin",
        },
    )

    assert login_response.status_code == 200
    payload = login_response.json()
    assert payload["email"] == "admin@kazatu.edu.kz"
    assert payload["role"] == "admin"
    assert payload["token"]

    profile_response = client.get(
        "/api/profile",
        headers={"Authorization": f"Bearer {payload['token']}"},
    )

    assert profile_response.status_code == 200
    assert profile_response.json()["email"] == "admin@kazatu.edu.kz"


def test_admin_route_requires_auth(client):
    response = client.get("/api/teachers")

    assert response.status_code == 401
    assert response.json()["errorCode"] == "auth_required"


def test_admin_can_create_and_delete_teacher(client, admin_auth_headers):
    create_response = client.post(
        "/api/teachers",
        headers=admin_auth_headers,
        json={
            "name": "Teacher One",
            "email": "teacher.one@kazatu.edu.kz",
            "phone": "+77000000000",
            "department": "CS",
            "teaching_languages": ["ru", "kk"],
        },
    )

    assert create_response.status_code == 201
    teacher = create_response.json()
    assert teacher["email"] == "teacher.one@kazatu.edu.kz"

    list_response = client.get("/api/teachers", headers=admin_auth_headers)
    assert list_response.status_code == 200
    assert any(item["id"] == teacher["id"] for item in list_response.json())

    delete_response = client.delete(
        f"/api/teachers/{teacher['id']}",
        headers=admin_auth_headers,
    )
    assert delete_response.status_code == 200
    assert delete_response.json()["success"] is True


def test_admin_can_create_course_with_credits_and_hours(client, admin_auth_headers):
    create_response = client.post(
        "/api/disciplines",
        headers=admin_auth_headers,
        json={
            "name": "Databases",
            "code": "DB201",
            "credits": 5,
            "hours": 150,
            "year": 2,
            "semester": 3,
            "department": "CS",
            "programme": "Business Informatics",
            "cycle": "БД",
            "component": "КВ",
            "requires_computers": 1,
        },
    )

    assert create_response.status_code == 201
    course = create_response.json()
    assert course["credits"] == 5
    assert course["hours"] == 150
    assert course["cycle"] == "БД"
    assert course["component"] == "КВ"


def test_manual_schedule_entry_notifies_teacher_and_students(client, admin_auth_headers, orm):
    teacher_response = client.post(
        "/api/teachers",
        headers=admin_auth_headers,
        json={
            "name": "Notify Teacher",
            "email": "notify.teacher@kazatu.edu.kz",
            "phone": "+77000000001",
            "department": "CS",
            "teaching_languages": "ru,kk",
        },
    )
    assert teacher_response.status_code == 201
    teacher = teacher_response.json()

    course_response = client.post(
        "/api/disciplines",
        headers=admin_auth_headers,
        json={
            "name": "Notification Course",
            "code": "NOT201",
            "credits": 5,
            "hours": 150,
            "year": 2,
            "semester": 1,
            "department": "CS",
            "programme": "Software Engineering",
            "instructor_id": teacher["id"],
            "instructor_name": teacher["name"],
        },
    )
    assert course_response.status_code == 201
    course = course_response.json()

    room_response = client.post(
        "/api/rooms",
        headers=admin_auth_headers,
        json={
            "number": "2414",
            "capacity": 40,
            "building": "2",
            "type": "lecture",
            "department": "CS",
            "available": 1,
        },
    )
    assert room_response.status_code == 201
    room = room_response.json()

    group_response = client.post(
        "/api/groups",
        headers=admin_auth_headers,
        json={
            "name": "SE-24-09",
            "student_count": 24,
            "study_course": 2,
            "has_subgroups": 0,
            "language": "ru",
            "programme": "b057",
            "specialty_code": "6B06101",
        },
    )
    assert group_response.status_code == 201
    group = group_response.json()

    student_response = client.post(
        "/api/auth/register",
        json={
            "email": "notify.student@example.com",
            "password": "student123",
            "displayName": "Notify Student",
            "role": "student",
            "department": "b057",
            "programmeName": "6B06101",
            "groupId": group["id"],
            "language": "ru",
        },
    )
    assert student_response.status_code == 201
    student = student_response.json()

    section_response = client.post(
        "/api/sections",
        headers=admin_auth_headers,
        json={
            "course_id": course["id"],
            "course_name": course["name"],
            "group_id": group["id"],
            "group_name": group["name"],
            "classes_count": 1,
            "lesson_type": "lecture",
            "teacher_id": teacher["id"],
            "teacher_name": teacher["name"],
        },
    )
    assert section_response.status_code == 201
    section = section_response.json()

    schedule_response = client.post(
        "/api/schedules",
        headers=admin_auth_headers,
        json={
            "section_id": section["id"],
            "course_id": course["id"],
            "course_name": course["name"],
            "teacher_id": teacher["id"],
            "teacher_name": teacher["name"],
            "room_id": room["id"],
            "room_number": room["number"],
            "group_id": group["id"],
            "group_name": group["name"],
            "subgroup": "",
            "day": "monday",
            "start_hour": 12,
            "semester": 1,
            "year": 2026,
            "algorithm": "manual",
        },
    )
    assert schedule_response.status_code == 201

    teacher_notifications = orm.list(
        "Notification",
        "id",
        recipient_role="teacher",
        recipient_id=teacher["id"],
    )
    assert len(teacher_notifications) == 1
    assert teacher_notifications[0].notification_type == "schedule_changed"

    student_notifications_response = client.get(
        "/api/notifications",
        headers={"Authorization": f"Bearer {student['token']}"},
    )
    assert student_notifications_response.status_code == 200
    student_notifications = student_notifications_response.json()
    assert student_notifications["unreadCount"] == 1
    assert student_notifications["items"][0]["notification_type"] == "schedule_changed"


def test_admin_clear_all_removes_course_components(
    client,
    admin_auth_headers,
    orm,
):
    course = orm.add(
        "Course",
        name="Algorithms",
        code="ALG101",
        credits=5,
        hours=150,
        description="",
        year=1,
        semester=1,
        department="B057 - Информационные технологии",
        instructor_id=None,
        instructor_name="",
        programme="Бизнес-информатика",
        module_type="",
        module_name="",
        cycle="",
        component="ОК",
        language="ru",
        academic_year="",
        entry_year="",
        requires_computers=0,
    )
    orm.add(
        "CourseComponent",
        course_id=course.id,
        course_code="ALG101",
        course_name="Algorithms",
        programme="Бизнес-информатика",
        study_year=1,
        academic_period=1,
        semester=1,
        lesson_type="lecture",
        hours=30,
        weekly_classes=1,
        requires_computers=0,
        teacher_id=None,
        teacher_name="",
    )

    response = client.post("/api/admin/clear-all", headers=admin_auth_headers)
    assert response.status_code == 200

    assert orm.count("CourseComponent") == 0


def test_teacher_preference_admin_delete_endpoints(
    client,
    admin_auth_headers,
    seeded_teacher_request,
):
    delete_one_response = client.delete(
        f"/api/teacher-preferences/{seeded_teacher_request['request_id']}",
        headers=admin_auth_headers,
    )

    assert delete_one_response.status_code == 200
    assert delete_one_response.json()["deleted"] is True

    clear_response = client.delete(
        "/api/teacher-preferences",
        headers=admin_auth_headers,
    )

    assert clear_response.status_code == 200
    assert clear_response.json()["deleted"] is True
