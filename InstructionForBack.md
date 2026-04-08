# Backend

Этот backend уже приведён в совместимость с текущим frontend TimeTableG.

Что поддерживается:

- авторизация и профиль
- роли `admin`, `teacher`, `student`
- хранение ролей по таблицам:
  - `users` - только `admin`
  - `teachers` - преподаватели
  - `students` - студенты
- CRUD для:
  - `courses` / `disciplines`
  - `teachers`
  - `rooms`
  - `groups`
  - `sections`
  - `schedules`
- генерация расписания
- импорт Excel
- скачивание Excel-шаблона
- экспорт расписания в Excel
- SQLite локально и PostgreSQL в production

## Запуск

```bash
python3 backend/server.py
```

Если запускаешь из папки `backend`:

```bash
python3 server.py
```

## Структура

- `server.py` - точка входа
- `app/config.py` - env и конфигурация
- `app/db.py` - схема, миграции, подключение к БД
- `app/auth_service.py` - регистрация, логин, профиль
- `app/collections.py` - CRUD и фильтрация данных
- `app/import_service.py` - импорт/экспорт Excel
- `app/scheduling.py` - генерация расписания
- `app/http_handler.py` - HTTP API
- `app/admin_service.py` - очистка данных

## Основные API

- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/profile`
- `POST /api/profile/avatar`
- `GET /api/disciplines`
- `GET /api/teachers`
- `GET /api/rooms`
- `GET /api/groups`
- `GET /api/sections`
- `GET /api/schedules`
- `POST /api/schedules/generate`
- `GET /api/import/template`
- `POST /api/import/excel`
- `GET /api/export/schedule`
- `POST /api/admin/clear-all`

## База данных

Локально без `DATABASE_URL` используется SQLite:

```text
backend/data/timetable.db
```

В production при наличии `DATABASE_URL` используется PostgreSQL.

При старте backend:

- создаёт таблицы автоматически
- делает rename/add column миграции
- переносит старые `teacher`/`student` аккаунты из `users` в `teachers`/`students`

## Переменные окружения

```env
BACKEND_HOST=0.0.0.0
BACKEND_PORT=8000
PORT=8000
ALLOWED_ORIGINS=http://localhost:5173
DATABASE_URL=
SQLITE_DB_FILE=backend/data/timetable.db
TEACHER_EMAIL_DOMAIN=@kazatu.edu.kz
```

## Зависимости

```bash
pip install -r backend/requirements.txt
```

Сейчас backend использует:

- `psycopg[binary]`
- `openpyxl`

## Проверка

```bash
python3 -m py_compile backend/server.py backend/app/*.py
```
