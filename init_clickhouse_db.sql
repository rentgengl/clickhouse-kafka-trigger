CREATE TABLE events
(
    id UInt64,
    event Int64,
    payload String
)
ENGINE = MergeTree
ORDER BY id;

-- Подключаем kafka и топик demo1c как таблицу
CREATE TABLE kafka_sink
(
    id UInt64,
    event Int64,
    payload String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'demo1c',
    kafka_group_name = 'clickhouse_consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;


-- Это представление будет записывать в kafka все записи с event=3
CREATE MATERIALIZED VIEW mv_events_to_kafka
TO kafka_sink
AS
SELECT id, event, payload
FROM events WHERE event=3;

-- Записываем данные которые должны быть записаны в kafka
INSERT INTO events
(id, event, payload)
VALUES(4, 3, 'w34www3');
