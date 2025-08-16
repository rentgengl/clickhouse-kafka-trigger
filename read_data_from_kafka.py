from confluent_kafka import Consumer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:29092',     # адрес брокера Kafka
    'group.id': 'demo1c-consumer-group',       # ID группы потребителей
    'auto.offset.reset': 'earliest',           # читать с начала, если нет сохранённых offset'ов
    'enable.auto.commit': False                # сами будем коммитить
}

consumer = Consumer(conf)

topic = "demo1c"
consumer.subscribe([topic])

print(f"📡 Listening to Kafka topic '{topic}'...")

try:
    while True:
        msg = consumer.poll(1.0)  # ждём до 1 секунды
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # достигнут конец партиции
                continue
            else:
                print(f"❌ Kafka error: {msg.error()}")
                break

        # выводим сообщение
        print(f"✅ Received: {msg.value().decode('utf-8')} "
              f"(partition: {msg.partition()}, offset: {msg.offset()})")

        # подтверждаем обработку
        consumer.commit(message=msg)

except KeyboardInterrupt:
    print("⏹ Stopping consumer...")

finally:
    consumer.close()
