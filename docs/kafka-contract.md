# Kafka Contract

Все сообщения в Kafka передаются в JSON и обязаны содержать top-level поле `event`.

- Топик `media`: входящие события для `media_worker`
- Топик `media.worker`: исходящие события, публикуемые `media_worker`

Поле `event` используется как discriminator и сериализуется в `snake_case`.

## Topic `media`

### `uploaded`

```json
{
  "event": "uploaded",
  "file_id": "11111111-1111-1111-1111-111111111111",
  "author_id": "demo",
  "size_bytes": 123456,
  "original_format": "mp3",
  "temp_path": "/media_tmp/test.mp3",
  "uploaded_at": "2026-04-07T12:00:00Z"
}
```

### `deleted`

```json
{
  "event": "deleted",
  "file_id": "11111111-1111-1111-1111-111111111111",
  "deleted_at": "2026-04-07T12:05:00Z"
}
```

## Topic `media.worker`

### `converted`

```json
{
  "event": "converted",
  "file_id": "11111111-1111-1111-1111-111111111111",
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
  "error_message": "Temporary file not found: /media_tmp/test.mp3",
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

## Notes

- Все timestamp-поля сериализуются как RFC 3339 UTC.
- Для топика `media` поле `temp_path` должно указывать на локальный путь, доступный контейнеру `media_worker`.

