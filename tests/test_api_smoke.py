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


def test_admin_clear_all_removes_course_components(
    client,
    admin_auth_headers,
    backend_modules,
):
    _app_module, db_module = backend_modules
    with db_module.get_connection() as connection:
        course_id = db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO courses (
                name, code, credits, hours, description, year, semester,
                department, instructor_id, instructor_name, programme,
                module_type, module_name, cycle, component, language,
                academic_year, entry_year, requires_computers
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Algorithms",
                "ALG101",
                5,
                150,
                "",
                1,
                1,
                "B057 - Информационные технологии",
                None,
                "",
                "Бизнес-информатика",
                "",
                "",
                "",
                "ОК",
                "ru",
                "",
                "",
                0,
            ),
        )
        db_module.insert_and_get_id(
            connection,
            """
            INSERT INTO course_components (
                course_id, course_code, course_name, programme, study_year,
                academic_period, semester, lesson_type, hours, weekly_classes,
                requires_computers, teacher_id, teacher_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                course_id,
                "ALG101",
                "Algorithms",
                "Бизнес-информатика",
                1,
                1,
                1,
                "lecture",
                30,
                1,
                0,
                None,
                "",
            ),
        )
        connection.commit()

    response = client.post("/api/admin/clear-all", headers=admin_auth_headers)
    assert response.status_code == 200

    with db_module.get_connection() as connection:
        remaining = db_module.query_one(connection, "SELECT COUNT(*) AS count FROM course_components")
        assert remaining["count"] == 0


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
