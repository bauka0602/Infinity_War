# Backend

API для фронта `TimeTableG`.

Поддерживаемые режимы:

- локально: `SQLite`
- деплой: `PostgreSQL` через `DATABASE_URL`

## Локальный запуск

```bash
python3 backend/server.py
```

Сервер поднимается на `http://127.0.0.1:8000`, API доступен по префиксу `/api`.

Для запуска с другого ноутбука в одной сети backend уже слушает `0.0.0.0:8000`.
Тогда фронт должен обращаться к IP ноутбука с backend:

```env
VITE_API_URL=http://192.168.1.25:8000/api
```

## Переменные окружения

```env
BACKEND_HOST=0.0.0.0
BACKEND_PORT=8000
PORT=8000
ALLOWED_ORIGINS=http://localhost:5173
DATABASE_URL=
SQLITE_DB_FILE=backend/data/timetable.db
```

Backend читает переменные из:

- переменных окружения процесса
- корневого файла `.env`
- файла `backend/.env`

Если `DATABASE_URL` не указан, backend использует SQLite.

## База данных

- SQLite по умолчанию хранится в `backend/data/timetable.db`
- при первом запуске таблицы создаются автоматически
- если найден старый `backend/data/store.json`, данные автоматически мигрируются в SQLite
- при наличии `DATABASE_URL` backend использует PostgreSQL

## Тестовые аккаунты

- `admin@kazatu.edu.kz` / `admin123`
- `teacher@kazatu.edu.kz` / `teacher123`
- `student@university.kz` / `student123`

## Роли и регистрация

- публичная регистрация доступна только для `student` и `teacher`
- `admin` через публичную регистрацию создать нельзя
- для регистрации `teacher` email должен оканчиваться на `@kazatu.edu.kz`
- при логине выбранная роль должна совпадать с ролью аккаунта
