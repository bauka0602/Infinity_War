def default_store():
    return {
        "users": [
            {
                "email": "admin@kazatu.edu.kz",
                "password": "admin123",
                "displayName": "System Admin",
                "role": "admin",
                "token": "seed-admin-token",
            },
            {
                "email": "teacher@kazatu.edu.kz",
                "password": "teacher123",
                "displayName": "Default Teacher",
                "role": "teacher",
                "token": "seed-teacher-token",
            },
            {
                "email": "student@university.kz",
                "password": "student123",
                "displayName": "Default Student",
                "role": "student",
                "token": "seed-student-token",
            },
        ],
        "courses": [
            {
                "name": "Algorithms",
                "code": "CS201",
                "credits": 4,
                "hours": 48,
                "description": "Core algorithms course",
            },
            {
                "name": "Databases",
                "code": "CS205",
                "credits": 3,
                "hours": 36,
                "description": "Relational database systems",
            },
        ],
        "teachers": [
            {
                "name": "Aruzhan Sarsembayeva",
                "email": "a.sarsembayeva@university.kz",
                "phone": "+7 701 000 0001",
                "specialization": "Computer Science",
                "max_hours_per_week": 20,
            },
            {
                "name": "Daniyar Omarov",
                "email": "d.omarov@university.kz",
                "phone": "+7 701 000 0002",
                "specialization": "Information Systems",
                "max_hours_per_week": 18,
            },
        ],
        "rooms": [
            {
                "number": "101",
                "capacity": 40,
                "building": "Main",
                "type": "lecture",
                "equipment": "Projector, speakers",
            },
            {
                "number": "Lab-3",
                "capacity": 24,
                "building": "Engineering",
                "type": "lab",
                "equipment": "24 PCs",
            },
        ],
        "schedules": [],
        "sections": [],
    }
