# Kafka Contract

Сообщения в Kafka передаются в JSON.

Публичный поток (`event`-discriminator) и backend-поток для `podcast_core` разнесены
по разным топикам, чтобы консьюмеры backend не падали на чужих схемах:

- Топик `media`: generic-события от `media_api`; `media_worker` обрабатывает только `type=podcast_file` + `event=uploaded`
- Топик `media.worker`: **только backend-события** для `podcast_core` (`start_processing` / `processed` / `processing_failed`)
- Топик `media.worker.events`: публичный поток `media_worker` (`converted` / `error` / `deleted`) для внешних потребителей
- Топик `media.subtitle.request`: запросы на генерацию субтитров (`media_worker` → `speech_service`)
- Топик `media.subtitle`: **только backend-результат** субтитров для `podcast_core` (с `podcast_id` и `content`)
- Топики `media.subtitle.ready` / `media.subtitle.error`: публичный поток субтитров `speech_service`

Поле `event` используется как discriminator в публичных топиках и сериализуется в `snake_case`.

## Topic `media`

### `start_upload`

```json
{
  "event": "start_upload",
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "started_at": "2026-04-07T12:00:00Z"
}
```

### `uploaded`

```json
{
  "event": "uploaded",
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "url": "s3://4c5face5-544c-4bc2-a2e0-57a24d243af3/media/uploads/podcast_file/11111111-1111-1111-1111-111111111111/22222222-2222-4222-8222-222222222222.mp3",
  "size": 123456,
  "content_type": "audio/mpeg",
  "uploaded_at": "2026-04-07T12:00:00Z"
}
```

### `error`

```json
{
  "event": "error",
  "type": "podcast_file",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "error_message": "Unsupported media type",
  "timestamp": "2026-04-07T12:00:10Z"
}
```

### `deleted`

```json
{
  "event": "deleted",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "deleted_at": "2026-04-07T12:05:00Z"
}
```

## Topic `media.worker.events` (публичный поток)

### `converted`

```json
{
  "event": "converted",
  "file_id": "11111111-1111-1111-1111-111111111111",
  "podcast_id": "podcast_123",
  "path": "/media/11111111-1111-1111-1111-111111111111/11111111-1111-1111-1111-111111111111.m3u8",
  "duration": 123.45,
  "bitrates": [64, 128, 256],
  "converted_at": "2026-04-07T12:01:30Z"
}
```

### `error`

```json
{
  "event": "error",
  "file_id": "11111111-1111-1111-1111-111111111111",
  "stage": "conversion",
  "error_message": "download source audio from s3://bucket/key failed",
  "timestamp": "2026-04-07T12:01:10Z"
}
```

### `deleted`

```json
{
  "event": "deleted",
  "file_id": "11111111-1111-1111-1111-111111111111",
  "deleted_objects": 4,
  "deleted_at": "2026-04-07T12:06:00Z"
}
```

## Topic `media.worker` (backend-поток для `podcast_core`)

В topic `media.worker` публикуются только backend-сообщения. Публичные `converted`,
`error` и `deleted` вынесены в `media.worker.events` для существующих потребителей.

Начало обработки:

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "start_processing",
  "timestamp": "2026-05-31T00:00:00Z"
}
```

Успешная обработка:

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "processed",
  "audio_url": "/media/11111111-1111-1111-1111-111111111111/master.m3u8",
  "duration_seconds": "2580",
  "audio_file_size": "11232332",
  "timestamp": "2026-05-31T00:01:00Z"
}
```

`duration_seconds` содержит полную длительность исходного аудиофайла, **округлённую
до целого числа секунд** (backend десериализует её как `Long`), а `audio_file_size` —
размер исходного аудиофайла в байтах. Оба значения сериализуются как строки.

Ошибка обработки:

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "processing_failed",
  "error": "conversion failed",
  "timestamp": "2026-05-31T00:01:00Z"
}
```

## Topic `media.subtitle.request`

Worker публикует это сообщение после успешной HLS-конвертации для входящего `media.uploaded` с `type=podcast_file`. Его потребляет `speech_service`.

```json
{
  "file_id": "11111111-1111-1111-1111-111111111111",
  "source_bucket": "4c5face5-544c-4bc2-a2e0-57a24d243af3",
  "source_object_key": "media/11111111-1111-1111-1111-111111111111/256k/seg_00000.m4s",
  "language": "ru",
  "num_speakers": 2,
  "requested_at": "2026-04-07T12:01:31Z"
}
```

## Notes

- Все timestamp-поля сериализуются как RFC 3339 UTC.
- Для `type=podcast_file` поле `object_id` используется как `file_id` и как `podcast_id`; значение должно быть UUID.
- `media_worker` игнорирует `uploaded` события типов `avatar`, `podcast_cover` и `playlists`.
- Генерация субтитров включена для всех обработанных `podcast_file` upload-событий.
- Язык запроса субтитров берется из `SUBTITLE_LANGUAGE`, по умолчанию `ru`.
- Число спикеров запрашивается публичным `GET {PODCAST_API_BASE_URL}/podcasts/{podcast_id}/speakers`; если API недоступен или `PODCAST_API_BASE_URL` не задан, `num_speakers` не отправляется.
- Для топика `media` поле `url` должно быть S3 locator в формате `s3://<bucket>/<object_key>`.
