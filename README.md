# Media_worker

`media_worker` слушает Kafka-топик `media`, обрабатывает входящие медиафайлы через `ffmpeg`, загружает HLS-артефакты в S3-совместимое хранилище и публикует результат в Kafka-топик `media.worker`.

Сервис делает три вещи:

1. Обрабатывает событие загрузки файла и конвертирует его в HLS.
2. Обрабатывает событие удаления и чистит ранее загруженные HLS-объекты.
3. Отдает SSE-прогресс по `file_id`.

## Что требуется для работы

Из кода проекта обязательны следующие зависимости:

- Kafka с заранее созданными топиками `media` и `media.worker`
- S3-совместимое хранилище
- `ffmpeg` и `ffprobe`
- общий путь с временными файлами, доступный и сервису, который кладет исходник, и самому `media_worker`

По умолчанию воркер пишет HLS в бакет `audio-hls` и читает адрес исходного файла из поля `temp_path` события `media.uploaded`.

## Сценарии запуска

В проекте имеет смысл использовать два разных сценария:

1. Тестовый стенд для разработчика или тестировщика, где вместе с воркером поднимаются Kafka, RustFS, топики и бакет.
2. Продовый запуск, где `media_worker` стартует отдельно и подключается к уже существующим Kafka и S3.

## Сценарий 1. Тестовый стенд для разработки и QA

Этот сценарий нужен, когда надо быстро поднять полностью рабочее окружение на одной машине.

В репозиторий добавлен [`compose.yml`](./compose.yml), который поднимает:

- Kafka в KRaft-режиме
- init-контейнер для создания топиков `media` и `media.worker`
- RustFS
- сам `media_worker`

Здесь важно не путать параметры:

- у контейнера RustFS используются серверные переменные `RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY`, `RUSTFS_CONSOLE_ENABLE`, `RUSTFS_ADDRESS`;
- у `media_worker` используются клиентские переменные `RUSTFS_ENDPOINT_URL`, `RUSTFS_ACCESS_KEY_ID`, `RUSTFS_SECRET_ACCESS_KEY`, `RUSTFS_REGION`.

Общий каталог для временных файлов проброшен как `./tmp/media -> /media_tmp` внутри контейнера воркера.

### 1. Собрать и запустить стек

```bash
docker compose up -d --build media-worker
```

Эта команда:

1. собирает образ воркера;
2. поднимает Kafka и RustFS;
3. создает Kafka-топики;
4. запускает `media_worker`.

Бакет `audio-hls` в тестовом сценарии отдельно не создается: воркер сам вызывает `ensure_bucket` перед первой загрузкой HLS-файлов.

Проверить состояние:

```bash
docker compose ps
docker compose logs -f media-worker
```

После старта будут доступны:

- Worker API: `http://localhost:8082`
- Swagger UI: `http://localhost:8082/swagger-ui/`
- RustFS S3 API: `http://localhost:9000`
- RustFS Console: `http://localhost:9001`
- Kafka для клиентов с хоста: `localhost:9094`

### 2. Положить тестовый файл

Скопируйте исходный файл в общий каталог:

```bash
cp /absolute/path/to/test.mp3 ./tmp/media/test.mp3
```

Важно: внутри события в Kafka нужно передавать не хостовый путь, а путь, который видит контейнер воркера, то есть `/media_tmp/test.mp3`.


### 3. Быстро отправить тестовый файл на обработку

Минимальная последовательность для ручной проверки:

1. Подготовьте файл `./tmp/media/test.mp3`.
2. Выберите `file_id`, например `11111111-1111-1111-1111-111111111111`.
3. Отправьте событие `media.uploaded` в Kafka.
4. Смотрите SSE-прогресс и ответы в топике `media.worker`.

Готовый пример:

```bash
FILE_ID=11111111-1111-1111-1111-111111111111

printf '%s\n' \
"{\"file_id\":\"$FILE_ID\",\"author_id\":\"demo\",\"size_bytes\":123456,\"original_format\":\"mp3\",\"temp_path\":\"/media_tmp/test.mp3\",\"uploaded_at\":\"2026-04-06T12:00:00Z\"}" \
| docker compose exec -T kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic media

curl -N "http://localhost:8082/api/media/worker/progress/$FILE_ID"
```

Ожидаемое поведение:

- воркер создаст бакет `audio-hls`, если его еще нет;
- обработает файл из `temp_path`;
- загрузит HLS-артефакты в RustFS по префиксу `media/<file_id>/`;
- отправит событие в топик `media.worker`.



## Сценарий 2. Продовый запуск с внешними Kafka и S3

Этот сценарий нужен для реального окружения, где Kafka и S3 уже существуют и управляются отдельно от воркера.

### Что должно быть подготовлено заранее

- доступный Kafka broker или кластер;
- топики `media` и `media.worker`;
- S3-совместимое хранилище;
- бакет `audio-hls`;
- общий путь с временными файлами, который видит и producer события, и контейнер `media_worker`.

Хотя код пытается создать бакет сам, в проде лучше считать это обязанностью инфраструктуры и создать бакет заранее.

### Переменные окружения для продового запуска

```env
KAFKA_BROKERS=kafka-1:9092,kafka-2:9092
PORT=8082
PIPELINE_MAX_RETRIES=3
RUSTFS_ENDPOINT_URL=https://rustfs.example.internal
RUSTFS_ACCESS_KEY_ID=***
RUSTFS_SECRET_ACCESS_KEY=***
RUSTFS_REGION=us-east-1
```

Это именно переменные воркера как S3-клиента. Если вы поднимаете сам сервер RustFS отдельно, у него будут другие параметры, например `RUSTFS_ACCESS_KEY` и `RUSTFS_SECRET_KEY`.

`PIPELINE_MAX_RETRIES` задает количество попыток обработки одного файла. По умолчанию используется `3`. Если переменная не задана, не парсится в число или равна `0`, воркер пишет предупреждение в лог и возвращается к значению `3`.

### Сборка образа

```bash
docker build -t media-worker:latest .
```

### Запуск контейнера

Пример:

```bash
docker run -d \
  --name media-worker \
  -p 8082:8082 \
  -e KAFKA_BROKERS="kafka-1:9092,kafka-2:9092" \
  -e PORT="8082" \
  -e PIPELINE_MAX_RETRIES="3" \
  -e RUSTFS_ENDPOINT_URL="https://rustfs.example.internal" \
  -e RUSTFS_ACCESS_KEY_ID="access-key" \
  -e RUSTFS_SECRET_ACCESS_KEY="secret-key" \
  -e RUSTFS_REGION="us-east-1" \
  -v /srv/media/tmp:/media_tmp \
  media-worker:latest
```

Если producer, который публикует `media.uploaded`, работает отдельно, он должен передавать в `temp_path` путь, который существует внутри контейнера воркера. Для примера выше это путь вида `/media_tmp/<file_name>`.

### Что важно в проде

- воркер читает только топик `media`;
- воркер публикует результаты в `media.worker`;
- consumer group фиксированная: `media-worker-service`;
- в consumer выставлено `auto.offset.reset=latest`, поэтому новый инстанс нужно поднимать до подачи новых событий, которые он должен обработать;
- не путайте переменные сервера RustFS (`RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY`) с переменными воркера (`RUSTFS_ACCESS_KEY_ID`, `RUSTFS_SECRET_ACCESS_KEY`);
- если файл по `temp_path` недоступен внутри контейнера, обработка завершится ошибкой.

## Важное ограничение по `temp_path`

Воркер не скачивает исходный файл из S3 или HTTP. Он ожидает, что `temp_path` указывает на локальный файл, который уже существует внутри контейнера.

Если `media_api` или другой producer работает тоже в Docker, ему нужно смонтировать тот же каталог:

```yaml
volumes:
  - ./tmp/media:/media_tmp
```

И публиковать событие с путем вида `/media_tmp/<имя_файла>`.

## Почему топики создаются до запуска воркера

В consumer задано `auto.offset.reset=latest`. Это значит:

- топики лучше создать заранее;
- сам воркер нужно поднять до отправки тестовых сообщений;
- если отправить событие до первого подключения consumer-группы `media-worker-service`, оно не будет прочитано этим воркером автоматически.

## Полезные эндпоинты

- Swagger UI: `http://localhost:8082/swagger-ui/`
- SSE прогресс: `GET /api/media/worker/progress/{file_id}`
