# Backend Architecture

Этот файл объясняет backend полностью: какие файлы есть, кто за что отвечает и куда вносить изменения.

## Общая схема

Backend построен так:

1. `server.py` запускает HTTP-сервер
2. `app/http_handler.py` принимает HTTP-запросы и маршрутизирует их
3. сервисные модули внутри `app/` выполняют конкретную бизнес-логику
4. `app/db.py` работает с базой данных

## Точка входа

### [server.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/server.py)

Это главный файл запуска backend.

Он отвечает только за:

- импорт конфигурации
- инициализацию базы
- создание `ThreadingHTTPServer`
- запуск `ApiHandler`

Если нужно:

- поменять порт
- поменять host
- понять, как backend стартует

смотри сюда.

## Папка `app/`

Вся логика backend теперь лежит здесь.

### [app/config.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/config.py)

Отвечает за конфигурацию проекта.

Здесь:

- читаются `.env`
- определяются пути проекта
- определяется `DATABASE_URL`
- выбирается тип БД: `sqlite` или `postgres`
- читаются:
  - `HOST`
  - `PORT`
  - `ALLOWED_ORIGINS`
  - `TEACHER_EMAIL_DOMAIN`
- создаётся `DB_LOCK`

Если нужно изменить:

- CORS
- порт
- домен преподавателей
- путь к SQLite

меняй этот файл.

### [app/errors.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/errors.py)

Отвечает за структуру контролируемых ошибок API.

Тут находится:

- `ApiError`

Он используется для ошибок вида:

- `401`
- `403`
- `404`
- `400`

Если хочешь добавлять новые типы backend-ошибок с `errorCode`, они строятся через этот класс.

### [app/security.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/security.py)

Отвечает за безопасность и auth-хелперы.

Тут находится:

- хеширование пароля
- проверка пароля
- `sanitize_user`
- разбор `Bearer token`

Если нужно менять:

- способ хранения паролей
- структуру user-ответа наружу
- логику извлечения токена

это делается здесь.

### [app/store.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/store.py)

Отвечает за стартовые сидовые данные.

Содержит:

- тестовых пользователей
- тестовые курсы
- тестовых преподавателей
- тестовые аудитории

Если хочешь изменить начальные данные новой пустой БД, меняй этот файл.

### [app/db.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/db.py)

Это главный модуль работы с базой данных.

Отвечает за:

- подключение к SQLite/PostgreSQL
- `query_one`, `query_all`, `query_scalar`
- `insert_and_get_id`
- SQL-адаптацию между SQLite и Postgres
- создание таблиц
- инициализацию базы
- миграцию старого `store.json`
- сидинг новой пустой БД
- миграцию старых email сидовых аккаунтов

Если тебе нужно:

- добавить новую таблицу
- изменить схему
- менять seed
- менять миграции
- поменять поведение базы

это основной файл.

### [app/auth_service.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/auth_service.py)

Отвечает за аутентификацию и регистрацию.

Здесь находится логика:

- регистрации пользователя
- логина пользователя
- проверки роли
- проверки teacher email на `@kazatu.edu.kz`
- извлечения текущего пользователя по токену

Если нужно менять:

- правила регистрации
- правила логина
- ограничения для преподавателя
- auth flow

меняй этот файл.

### [app/collections.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/collections.py)

Отвечает за CRUD-операции для сущностей.

Именно здесь живут:

- `courses`
- `teachers`
- `rooms`
- `schedules`

Функции:

- `list_collection`
- `create_collection_item`
- `update_collection_item`
- `delete_collection_item`

Если нужно менять поля у:

- курса
- преподавателя
- аудитории
- записи расписания

то менять надо здесь.

Это главный файл для business CRUD-логики.

### [app/scheduling.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/scheduling.py)

Это главный файл алгоритма составления расписания.

Здесь находятся:

- расчёт стартовой недели
- генерация расписания
- распределение:
  - курса
  - преподавателя
  - аудитории
  - дня
  - часа

Главная функция:

- `build_schedule(connection, semester, year, algorithm)`

Если нужно менять сам алгоритм составления расписания, менять нужно именно этот файл.

### [app/http_handler.py](/Users/bekzatbaibolat05/Desktop/TimeTableG/backend/app/http_handler.py)

Это HTTP-слой backend.

Он отвечает за:

- обработку `GET/POST/PUT/DELETE/OPTIONS`
- CORS headers
- JSON request/response
- роутинг:
  - `/api/auth/register`
  - `/api/auth/login`
  - `/api/auth/logout`
  - `/api/courses`
  - `/api/teachers`
  - `/api/rooms`
  - `/api/schedules`
  - `/api/schedules/generate`
  - `/api/health`
- возврат ошибок в правильном формате

Если нужно:

- добавить новый endpoint
- изменить маршрут
- поменять HTTP-поведение

это делается здесь.

## Поток запроса

Пример запроса:

### Логин

1. frontend отправляет `POST /api/auth/login`
2. `app/http_handler.py` принимает запрос
3. вызывает `login_user(...)` из `app/auth_service.py`
4. `auth_service.py` берёт пользователя из БД через `app/db.py`
5. если всё ок, backend возвращает JSON пользователя

### Получение преподавателей

1. frontend отправляет `GET /api/teachers`
2. `app/http_handler.py` проверяет токен и роль
3. открывает соединение с БД
4. вызывает `list_collection(...)` из `app/collections.py`
5. возвращает список преподавателей

### Генерация расписания

1. frontend отправляет `POST /api/schedules/generate`
2. `app/http_handler.py` проверяет, что роль `admin`
3. вызывает `build_schedule(...)` из `app/scheduling.py`
4. `build_schedule(...)` читает курсы, преподавателей и аудитории из БД
5. создаёт новые записи расписания
6. возвращает готовое расписание

## Куда лезть в зависимости от задачи

### Нужно поменять регистрацию или логин

Смотри:

- `app/auth_service.py`

### Нужно поменять роли или teacher email policy

Смотри:

- `app/auth_service.py`
- `app/config.py`

### Нужно изменить структуру таблиц или добавить новую сущность

Смотри:

- `app/db.py`
- `app/collections.py`

### Нужно изменить сам алгоритм расписания

Смотри:

- `app/scheduling.py`

### Нужно добавить новый API endpoint

Смотри:

- `app/http_handler.py`

### Нужно изменить CORS или env

Смотри:

- `app/config.py`

## Что не трогать без понимания

Есть несколько чувствительных мест:

- `app/db.py` - можно случайно сломать совместимость SQLite/Postgres
- `app/security.py` - можно сломать логин всех пользователей
- `app/http_handler.py` - можно сломать весь API routing

## Текущие ограничения backend

- алгоритм расписания сейчас базовый, без сложных ограничений
- старый засвеченный `DATABASE_URL` нужно заменить вручную в Neon/Render
