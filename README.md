# clickhouse-kafka-trigger
Пример реализации триггера в ClickHouse с помошью Kafka и MATERIALIZED VIEW
Данная реализация может быть использована для дополнительно обработки данных, которые поступают в ClickHouse.

## Примеры использования
- Создание ошибок, которые попали в ClickHouse, в Sentry
- Реализация оповещений в Telegram или почту при появлении определенных событий

# Запуск
Для запуска примера необходимо:

## Запустить ClickHouse и Kafka
```bash
docker-compose up -d
```

## Инициализировать БД в ClickHouse
Необходимо выполнить запросы из файла init_clickhouse_db.sql

Можно подключиться к ClickHouse с помощью DBEaver или GUI самого ClickHouse по ссылке
```bash
http://localhost:8123/play?user=username&password=123
```

## Запустить скрипт-триггер
Всю логику работы триггера можно реализовать на базе скрипта read_data_from_kafka.py

Запускить скрипт можно с помощью команды
```bash
python3 kafka.py
```
Для работы скрипта необходим пакет confluent_kafka, который можно установить с помощью команды
```bash
pip install confluent-kafka
```
