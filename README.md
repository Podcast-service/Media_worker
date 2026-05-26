# Media_worker

`media_worker` слушает Kafka topic `media`, обрабатывает входящие аудиофайлы через `ffmpeg`, загружает HLS-артефакты в S3-compatible storage и публикует результат в Kafka topic `media.worker`.

## S3

Сервис использует только S3-compatible storage. Обязательные переменные:

```env
S3_ENDPOINT_URL=https://s3.twcstorage.ru
S3_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
HLS_BUCKET=4c5face5-544c-4bc2-a2e0-57a24d243af3
S3_REGION=ru-1
S3_ACCESS_KEY_ID=<secret>
S3_SECRET_ACCESS_KEY=<secret>
PODCAST_API_BASE_URL=http://localhost:8082/podcast/v1
SUBTITLE_LANGUAGE=ru
```

Секреты кладите в локальный `.env` рядом с `compose.yml`; `.env` игнорируется git. В репозитории оставлен только безопасный `.env.example`.

`S3_CREATE_BUCKET=true` можно использовать только если окружение должно создавать бакет автоматически. Для managed S3 бакет обычно уже создан инфраструктурой.

## Запуск

```bash
docker compose up -d --build kafka kafka-init media-worker
```

Проверка:

```bash
docker compose ps
docker compose logs -f media-worker
```

## Контракт

Сервис читает:

- topic: `media`
- consumer group: `media-worker-service`

Сервис публикует:

- topic: `media.worker`
- topic: `media.subtitle` после успешной HLS-конвертации входящего `type=podcast_file`

Входящее событие `media.uploaded` обрабатывается только при `type=podcast_file`. Оно должно содержать `object_id`, `url`, `size` и `content_type`. `url` должен быть S3 locator в формате `s3://<bucket>/<object_key>`.
`object_id` используется как `file_id` и как `podcast_id`, поэтому должен быть UUID. События типов `avatar`, `podcast_cover` и `playlists` worker игнорирует.
После конвертации `podcast_id` пробрасывается в событие `media.worker.converted`, чтобы backend мог связать HLS-результат с подкастом.
Worker после успешной HLS-конвертации отправляет запрос в `media.subtitle` с S3-объектом аудиосегмента.
Перед отправкой запроса субтитров worker делает публичный `GET {PODCAST_API_BASE_URL}/podcasts/{podcast_id}/speakers`, достает число спикеров и кладет его в поле `num_speakers`.

HLS-объекты загружаются в S3 по префиксу:

```text
media/<file_id>/
```

## API

- Swagger UI: `http://localhost:8082/swagger-ui/`
- SSE progress: `GET /api/media/worker/progress/{file_id}`
