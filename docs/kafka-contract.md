# Kafka Contract

`Media_worker` читает generic-события загрузки, конвертирует аудиофайлы в HLS
и публикует backend-события, публичные события и запросы генерации субтитров.
Сообщения передаются в JSON.

| Направление | Topic | Kafka key | Назначение |
| --- | --- | --- | --- |
| Входящий | `media` | `object_id` | Generic-события загрузки и удаления |
| Исходящий | `media.worker` | `file_id` | Backend-события для `podcast_core` |
| Исходящий | `media.worker.events` | `file_id` | Публичные события worker-домена |
| Исходящий | `media.subtitle.request` | `file_id` | Запросы генерации субтитров для `Speech_service` |

Consumer group для `media`: `media-worker-service`.

## Topic `media`

Worker принимает четыре типа событий. Для `uploaded` обрабатывается только
`type=podcast_file`; события типов `avatar`, `podcast_cover` и `playlists`
игнорируются. События `start_upload` и `error` логируются. Событие `deleted`
удаляет HLS-объекты по префиксу `media/<object_id>/`.

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
  "need_subtitle": true,
  "uploaded_at": "2026-04-07T12:00:10Z"
}
```

`url` должен быть S3 locator в формате `s3://<bucket>/<object_key>`.
`object_id` используется как `file_id` и как `podcast_id`, поэтому должен быть
UUID. Для совместимости со старыми сообщениями отсутствие `need_subtitle`
трактуется как `true`.

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

Backend-поток для `podcast_core`. В этот топик не публикуются публичные события
`converted`, `error` и `deleted`.

### `start_processing`

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "start_processing",
  "timestamp": "2026-04-07T12:00:11Z"
}
```

### `processed`

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "processed",
  "audio_url": "/media/11111111-1111-1111-1111-111111111111/master.m3u8",
  "duration_seconds": "2580",
  "audio_file_size": "11232332",
  "timestamp": "2026-04-07T12:01:30Z"
}
```

`duration_seconds` содержит полную длительность исходного аудиофайла,
округленную до целого числа секунд. `duration_seconds` и `audio_file_size`
сериализуются как строки для совместимости с backend.

### `processing_failed`

```json
{
  "object_type": "podcast_file_url",
  "object_id": "11111111-1111-1111-1111-111111111111",
  "event": "processing_failed",
  "error": "conversion failed",
  "timestamp": "2026-04-07T12:01:30Z"
}
```

## Topic `media.worker.events`

Публичный поток worker-домена для внешних потребителей.

### `converted`

```json
{
  "event": "converted",
  "file_id": "11111111-1111-1111-1111-111111111111",
  "podcast_id": "11111111-1111-1111-1111-111111111111",
  "path": "/media/11111111-1111-1111-1111-111111111111/master.m3u8",
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

## Topic `media.subtitle.request`

Worker публикует запрос после успешной HLS-конвертации, если входящее событие
`media.uploaded` содержит `type=podcast_file` и `need_subtitle=true`. При
`need_subtitle=false` сообщение не публикуется.

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

`language` берется из `SUBTITLE_LANGUAGE`, по умолчанию `ru`. Перед публикацией
worker запрашивает `GET {PODCAST_API_BASE_URL}/podcasts/{podcast_id}/speakers`.
Если API недоступен или `PODCAST_API_BASE_URL` не задан, поле `num_speakers`
не сериализуется.

## Notes

- Все timestamp-поля сериализуются как RFC 3339 UTC.
- В `media.worker` поля без значения не сериализуются.
