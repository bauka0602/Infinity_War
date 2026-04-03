# Backend

Backend отвечает за:

- регистрацию и логин
- роли пользователей
- CRUD для `courses`, `teachers`, `rooms`, `schedules`
- генерацию расписания
- работу с SQLite/PostgreSQL

## Точка входа

Запуск:

```bash
python3 backend/server.py
```

## Структура

- `server.py` - запуск backend
- `app/config.py` - конфиг и env
- `app/db.py` - база данных
- `app/auth_service.py` - логин и регистрация
- `app/collections.py` - CRUD сущностей
- `app/scheduling.py` - алгоритм составления расписания
- `app/http_handler.py` - HTTP API

Полное описание по файлам:

- `backend/ARCHITECTURE.md`

## База данных

### Локально

Если `DATABASE_URL` не указан, используется SQLite:

```text
backend/data/timetable.db
```

### Production

Если `DATABASE_URL` указан, используется PostgreSQL.

## Переменные окружения

```env
BACKEND_HOST=0.0.0.0
BACKEND_PORT=8000
PORT=8000
ALLOWED_ORIGINS=http://localhost:5173
DATABASE_URL=
SQLITE_DB_FILE=backend/data/timetable.db
```

## Роли

- `admin`
- `teacher`
- `student`

## Регистрация

- `admin` нельзя зарегистрировать публично
- `teacher` регистрируется только с email `@kazatu.edu.kz`
- `student` регистрируется с любым email

## Тестовые аккаунты

- `admin@kazatu.edu.kz` / `admin123`
- `teacher@kazatu.edu.kz` / `teacher123`
- `student@university.kz` / `student123`

## Проверка

```bash
python3 -m py_compile backend/server.py backend/app/*.py
```
