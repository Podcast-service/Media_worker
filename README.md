# Media_worker

`media_worker` слушает Kafka topic `media`, обрабатывает входящие аудиофайлы через `ffmpeg`, загружает HLS-артефакты в S3-compatible storage и публикует результаты в Kafka...

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

В backend-событии `media.worker/processed` worker публикует `audio_url` в
формате `https://s3.twcstorage.ru/<HLS_BUCKET>/media/<file_id>/master.m3u8`.
Для доступа к URL без авторизации бакет `HLS_BUCKET` должен быть публичным.

## Запуск

```bash
docker compose up -d --build kafka kafka-init media-worker
```

Проверка:

```bash
docker compose ps
docker compose logs -f media-worker
```

## Kafka Contract

Сервис читает:

- `media`, consumer group `media-worker-service`.

Сервис публикует:

- `media.worker` — backend-события обработки;
- `media.worker.events` — публичные события обработки;
- `media.subtitle.request` — запрос генерации субтитров после успешной
  HLS-конвертации при `need_subtitle=true`.

Полные JSON-контракты: [`docs/kafka-contract.md`](docs/kafka-contract.md).

HLS-объекты загружаются в S3 по префиксу:

```text
media/<file_id>/ 
```


## API

- Swagger UI: `http://localhost:8082/swagger-ui/`
- SSE progress: `GET /api/media/worker/progress/{file_id}`
