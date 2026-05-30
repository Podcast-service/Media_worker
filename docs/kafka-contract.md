# Kafka Contract

Сообщения в Kafka передаются в JSON. Топики `media` и `media.worker` используют top-level поле `event`.

- Топик `media`: generic-события от `media_api`; `media_worker` обрабатывает только `type=podcast_file` + `event=uploaded`
- Топик `media.worker`: исходящие события, публикуемые `media_worker`
- Топик `media.subtitle`: запросы на генерацию субтитров, публикуемые `media_worker` после обработки `type=podcast_file`

Поле `event` используется как discriminator и сериализуется в `snake_case`.

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

## Topic `media.worker`

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

### Дополнительные backend-события

В topic `media.worker` также публикуются сообщения для backend. Исходные сообщения
`converted`, `error` и `deleted` сохраняются для существующих потребителей.

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
  "timestamp": "2026-05-31T00:01:00Z"
}
```

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

## Topic `media.subtitle`

Worker публикует это сообщение после успешной HLS-конвертации для входящего `media.uploaded` с `type=podcast_file`.

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
