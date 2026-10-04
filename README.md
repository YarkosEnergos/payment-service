# Payment Service

Микросервис асинхронного процессинга платежей. Принимает запросы на оплату, обрабатывает
их через эмулятор платёжного шлюза и уведомляет клиента о результате через webhook.

**Стек:** Python 3.12, FastAPI + Pydantic v2, SQLAlchemy 2 (async) + asyncpg, PostgreSQL,
RabbitMQ + FastStream, Alembic, Docker Compose, uv, pytest + testcontainers, ruff.

## Соответствие требованиям

| Требование | Где реализовано |
|---|---|
| Модели и миграции `payments`, `outbox` | `src/infra/postgres/models/`, `src/infra/postgres/alembic/versions/` |
| `POST /api/v1/payments`, `GET /api/v1/payments/{id}` | `src/modules/payments/router.py`, `controller.py` |
| Статический ключ `X-API-Key` на всех эндпоинтах | `src/api/security.py`, подключён на уровне роутера `/api` |
| Idempotency-Key | уникальный индекс + `INSERT … ON CONFLICT DO NOTHING` + отпечаток тела запроса |
| Outbox pattern | запись платежа и события в одной транзакции, отдельный сервис `outbox-relay` |
| Один consumer, делающий всё | `src/modules/payments/consumer.py` → `processor.py` |
| Retry: 3 попытки, экспоненциальная задержка | delay-очереди RabbitMQ с TTL + DLX (2 с, 4 с) |
| Dead Letter Queue | `payments.dlx` → `payments.new.dlq` |
| Docker | `docker-compose.yml`: postgres, rabbitmq, api, consumer, outbox-relay, migrations, webhook-receiver |

## Архитектура

```mermaid
flowchart LR
    client([Клиент]) -- "POST /api/v1/payments" --> api[API<br/>FastAPI]
    api -- "payment + outbox<br/>в одной транзакции" --> db[(PostgreSQL)]
    relay[outbox-relay] -- "FOR UPDATE SKIP LOCKED" --> db
    relay -- "payments.new" --> mq{{RabbitMQ}}
    mq --> consumer[consumer<br/>FastStream]
    consumer -- "эмуляция шлюза,<br/>статус платежа" --> db
    consumer -- "POST webhook_url" --> hook([Клиент])
```

API не обращается к RabbitMQ вообще: он только пишет в БД. Брокер может быть недоступен,
и платежи всё равно принимаются, а события уйдут, когда он поднимется.

### Слои

Структура повторяет слоистую схему `router → controller → UnitOfWork → storage → models`:

```
src/
├── main.py                  точка входа API (FastAPI)
├── consumer.py              точка входа consumer (FastStream)
├── outbox_relay.py          точка входа outbox relay
├── app_config.py            настройки из окружения (pydantic-settings)
├── api/                     /api → /v1, проверка X-API-Key, обработчики доменных ошибок
├── modules/
│   ├── payments/
│   │   ├── router.py        HTTP-транспорт
│   │   ├── consumer.py      AMQP-транспорт: подписчик payments.new, retry/DLQ
│   │   ├── controller.py    создание и чтение платежа, идемпотентность
│   │   ├── processor.py     обработка платежа: шлюз → статус → webhook
│   │   ├── schemas.py       Pydantic-схемы запросов, ответов и событий
│   │   ├── enums.py         валюты и статусы
│   │   └── exceptions.py    доменные исключения
│   └── outbox/relay.py      выборка outbox → публикация → отметка о публикации
└── infra/
    ├── postgres/            engine, модели, storage, UnitOfWork, alembic
    ├── rabbitmq/            брокер, топология, политика retry, publisher
    ├── gateway/emulator.py  эмулятор платёжного шлюза (2–5 с, 90% успеха)
    └── webhooks/client.py   отправка webhook через httpx
tools/webhook_receiver/      тестовый приёмник webhook для демонстрации
tests/unit/                  unit-тесты на фейках, без внешних зависимостей
tests/integration/           PostgreSQL и RabbitMQ в testcontainers
```

HTTP-роутер и AMQP-подписчик тонкие: они только переводят транспорт в вызов
controller/processor. Бизнес-логика не знает про HTTP и RabbitMQ и получает зависимости
через конструктор (UoW-фабрика, шлюз, webhook-клиент), поэтому тестируется на фейках.

## Быстрый старт

```bash
docker compose up --build
```

| Сервис | Назначение | Адрес |
|---|---|---|
| `api` | REST API, Swagger UI | http://localhost:8000/docs |
| `consumer` | обработчик очереди `payments.new` | — |
| `outbox-relay` | публикация событий из outbox | — |
| `migrations` | `alembic upgrade head`, завершается после применения | — |
| `postgres` | PostgreSQL 17 | `localhost:5432` (payments / payments) |
| `rabbitmq` | RabbitMQ 4.3 + management UI | http://localhost:15672 (guest / guest) |
| `webhook-receiver` | тестовый приёмник webhook | http://localhost:8001/webhooks |

API-ключ по умолчанию: `dev-api-key`. Переопределяется так: `API_KEY=secret docker compose up`.

## Примеры

### Создание платежа

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #1001",
        "metadata": {"order_id": 1001},
        "webhook_url": "http://webhook-receiver:8001/webhooks"
      }'
```

`202 Accepted`:

```json
{
  "payment_id": "cf566382-6c7d-41f6-bd42-71f1ddaaf5b5",
  "status": "pending",
  "created_at": "2026-10-04T04:17:50.216946Z"
}
```

`webhook_url` вызывает consumer изнутри docker-сети, поэтому для демонстрации используется
адрес `http://webhook-receiver:8001/...`.

### Получение платежа

```bash
curl -s http://localhost:8000/api/v1/payments/cf566382-6c7d-41f6-bd42-71f1ddaaf5b5 \
  -H "X-API-Key: dev-api-key"
```

```json
{
  "payment_id": "cf566382-6c7d-41f6-bd42-71f1ddaaf5b5",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #1001",
  "metadata": {"order_id": 1001},
  "status": "succeeded",
  "idempotency_key": "order-1001",
  "webhook_url": "http://webhook-receiver:8001/webhooks",
  "created_at": "2026-10-04T04:17:50.216946Z",
  "processed_at": "2026-10-04T04:17:53.893358Z"
}
```

Сумма принимается строкой или числом, а возвращается строкой, чтобы не терять точность decimal.
Тело webhook совпадает с ответом `GET`. Полученные webhook можно посмотреть так:
`curl http://localhost:8001/webhooks`.

### Ошибки

| Ситуация | Ответ |
|---|---|
| Нет или неверный `X-API-Key` | `401` |
| Нет `Idempotency-Key` или невалидное тело | `422` |
| Тот же `Idempotency-Key` с другим телом | `422`, `"Idempotency key 'order-1001' was already used with a different request body"` |
| Повтор с тем же ключом и телом | `202` с тем же `payment_id`, новое событие не создаётся |
| Платёж не найден | `404` |

### Retry и DLQ вживую

Тестовый приёмник умеет имитировать проблемы на стороне клиента:

| `webhook_url` | Поведение | Что увидим |
|---|---|---|
| `http://webhook-receiver:8001/webhooks` | всегда `200` | доставка с первой попытки |
| `http://webhook-receiver:8001/webhooks/flaky?failures=2` | первые 2 раза `503` | повторы через 2 с и 4 с, затем доставка |
| `http://webhook-receiver:8001/webhooks/fail` | всегда `500` | 3 попытки, затем сообщение в `payments.new.dlq` |

```bash
docker compose logs -f consumer webhook-receiver
```

```
Payment 1a1774aa-…: attempt 1/3 failed, retrying in 2000 ms
Payment 1a1774aa-…: attempt 2/3 failed, retrying in 4000 ms
Payment 1a1774aa-…: attempt 3/3 failed, sending to DLQ
```

Сообщение в DLQ можно посмотреть в RabbitMQ UI: *Queues → payments.new.dlq → Get messages*.
В заголовках будут `x-attempt: 3` и `x-death` с причиной `rejected`.

### Outbox вживую

```bash
docker compose stop rabbitmq
# POST /api/v1/payments по-прежнему отвечает 202, событие лежит в outbox
docker compose start rabbitmq
# relay переподключается и публикует событие, consumer обрабатывает платёж
```

## Как это устроено

### Outbox pattern

1. `POST /payments` в одной транзакции вставляет строку в `payments` и событие
   `{"payment_id": …}` в `outbox`, затем делает commit. Только после этого клиент получает
   `202`. Платёж без события или событие без платежа невозможны.
2. `outbox-relay` в цикле выбирает неопубликованные события:
   `SELECT … WHERE published_at IS NULL ORDER BY created_at LIMIT n FOR UPDATE SKIP LOCKED`.
   Для этого запроса есть частичный индекс `ix_outbox_unpublished`. Благодаря
   `SKIP LOCKED` можно запускать несколько экземпляров relay, и они не опубликуют одно
   событие дважды. Это проверяется интеграционным тестом.
3. Каждое событие публикуется как persistent-сообщение с publisher confirms и
   `mandatory`. `message_id` равен id записи outbox. Опубликованные записи помечаются
   `published_at` в той же транзакции. Если публикация упала, relay останавливает пачку и
   повторяет её на следующем цикле.

Гарантия доставки — *at-least-once*. Если relay упадёт между публикацией и commit, событие
уйдёт повторно, поэтому consumer идемпотентен.

### Идемпотентность

- **API.** Создание платежа — это `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING`.
  При одновременных запросах с одним ключом вставляет только один, остальные получают
  уже существующую запись, без исключений и гонок. Чтобы тот же ключ нельзя было
  использовать для другого платежа, хранится SHA-256 отпечаток нормализованного тела
  запроса. Сумма приводится к двум знакам, ключи сортируются. Повтор с тем же телом
  возвращает исходный ответ, с другим телом — `422`, как рекомендует IETF draft
  *The Idempotency-Key HTTP Header Field*.
- **Consumer.** Статус меняется условным `UPDATE … WHERE status = 'pending' RETURNING`.
  Повторно доставленное сообщение не вызывает шлюз ещё раз, а только заново отправляет
  webhook с уже сохранённым результатом. Исход платежа никогда не «перебрасывается».

### Consumer

Обработчик `payments.new`:

1. Загружает платёж. Если он ещё в `pending`, вызывает эмулятор шлюза (2–5 с, 90% `succeeded`,
   10% `failed`).
2. Сохраняет статус и `processed_at` и сразу коммитит, до отправки webhook.
3. Отправляет webhook на `webhook_url`.

`failed` от шлюза — это бизнес-результат, а не ошибка. Платёж получает статус `failed`,
клиент получает webhook. Повторяются только технические сбои. Сообщения обрабатываются
параллельно, их число ограничено `prefetch_count` (по умолчанию 10).

### Retry и Dead Letter Queue

```mermaid
flowchart LR
    relay[outbox-relay] -- "payments.new" --> ex[payments<br/>direct]
    ex --> q[payments.new]
    q --> c[consumer]
    c -- "ошибка, попытки остались" --> rex[payments.retry<br/>direct]
    rex --> r1["payments.new.retry.2000ms<br/>TTL 2 с"]
    rex --> r2["payments.new.retry.4000ms<br/>TTL 4 с"]
    r1 -- "TTL истёк → DLX" --> ex
    r2 -- "TTL истёк → DLX" --> ex
    c -. "3-я попытка упала: reject" .-> q
    q -- "x-dead-letter-exchange" --> dlx[payments.dlx<br/>direct]
    dlx --> dlq[payments.new.dlq]
```

| Ситуация | Поведение |
|---|---|
| webhook не ответил 2xx, таймаут, сетевая ошибка, ошибка БД | копия сообщения уходит в delay-очередь, оригинал подтверждается |
| исчерпаны 3 попытки | `reject` → брокер через DLX кладёт сообщение в `payments.new.dlq` |
| платёж не найден, невалидное сообщение | сразу `reject` → DLQ, повторять бессмысленно |
| не удалось опубликовать в delay-очередь | `nack(requeue=True)`: сообщение вернётся в `payments.new`, а не уйдёт в DLQ раньше времени |

- Задержка растёт экспоненциально: `base × 2^(n−1)`, по умолчанию 2 с и 4 с. Ожидание
  происходит в брокере, consumer в это время не блокируется, а повторы переживают его
  рестарт.
- Для каждой задержки своя очередь с `x-message-ttl`, а не TTL на сообщении. RabbitMQ
  удаляет просроченные сообщения только из головы очереди, поэтому при общей очереди
  короткая задержка ждала бы длинную.
- В имени delay-очереди есть задержка. Если поменять настройки, создадутся новые очереди,
  а не возникнет `PRECONDITION_FAILED` из-за других аргументов существующей.
- Номер попытки хранится в заголовке `x-attempt`.
- Топологию идемпотентно объявляют и consumer, и relay при старте. Даже если relay
  запустился первым, сообщения не потеряются в exchange без очередей.

## Конфигурация

Все параметры задаются переменными окружения (см. `.env.example`):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | — | статический ключ для `X-API-Key` |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | —, `5432`, —, —, — | подключение к PostgreSQL |
| `RABBITMQ_URL` | — | AMQP URL, например `amqp://guest:guest@localhost:5672/` |
| `GATEWAY_MIN_DELAY_SECONDS` / `GATEWAY_MAX_DELAY_SECONDS` | `2` / `5` | время обработки в эмуляторе шлюза |
| `GATEWAY_SUCCESS_RATE` | `0.9` | доля успешных платежей |
| `CONSUMER_MAX_ATTEMPTS` | `3` | число попыток до DLQ |
| `CONSUMER_RETRY_BASE_DELAY_SECONDS` | `2` | базовая задержка retry |
| `CONSUMER_PREFETCH_COUNT` | `10` | сколько сообщений consumer обрабатывает параллельно |
| `CONSUMER_WEBHOOK_TIMEOUT_SECONDS` | `10` | таймаут запроса webhook |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1` | пауза relay, когда outbox пуст |
| `OUTBOX_BATCH_SIZE` | `100` | размер пачки relay |

## Локальная разработка

Зависимости ставятся через [uv](https://docs.astral.sh/uv/):

```bash
uv sync
cp .env.example .env
docker compose up -d postgres rabbitmq

uv run alembic upgrade head
uv run uvicorn src.main:app --reload
uv run faststream run src.consumer:app
uv run python -m src.outbox_relay
uv run uvicorn tools.webhook_receiver.main:app --port 8001
```

При локальном запуске `webhook_url` указывается как `http://localhost:8001/webhooks`.

Новая миграция: `uv run alembic revision --autogenerate -m "message"`. Сгенерированный
файл автоматически проходит `ruff format` и `ruff check --fix`.

## Тесты и линтер

```bash
uv run pytest -m "not integration"   # unit-тесты, Docker не нужен
uv run pytest                        # + интеграционные, нужен запущенный Docker
uv run ruff check .
```

- **Unit** (на фейковом UnitOfWork): идемпотентность контроллера,
  валидация схем, HTTP-уровень и аутентификация, процессор, relay, политика retry,
  webhook-клиент на `httpx.MockTransport`, подписчик на `TestRabbitBroker`.
  Проверяется, что ошибка уходит в нужную delay-очередь, а последняя попытка — в reject.
- **Integration** (PostgreSQL и RabbitMQ в testcontainers, миграции применяются
  настоящим Alembic): 10 параллельных запросов с одним ключом дают один платёж и одно
  событие; три relay конкурируют за outbox без дублей; полный путь
  API → outbox → RabbitMQ → consumer → webhook; повторы с проверкой, что интервалы растут;
  попадание в DLQ после трёх попыток.

Если Docker недоступен, интеграционные тесты пропускаются автоматически.

## Что можно улучшить

- HMAC-подпись webhook (`X-Signature`) и защита от SSRF при проверке `webhook_url`.
- Очистка или архивация опубликованных записей outbox.
- Срок жизни idempotency-ключей и их привязка к клиенту. Сейчас ключ глобальный, потому
  что клиент один (статический API-ключ).
- Метрики Prometheus и трейсинг OpenTelemetry (в FastStream есть middleware для обоих).
- Quorum-очереди для отказоустойчивости в кластере RabbitMQ.
- `LISTEN/NOTIFY` вместо поллинга outbox для меньшей задержки публикации.
