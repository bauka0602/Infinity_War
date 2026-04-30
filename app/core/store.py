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
        ],
        "courses": [],
        "teachers": [],
        "rooms": [],
        "schedules": [],
        "sections": [],
    }
