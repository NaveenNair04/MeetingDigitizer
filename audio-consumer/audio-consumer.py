from kafka import KafkaConsumer

TOPIC = "audio-stream"
BOOTSTRAP_SERVERS = "kafka:9092"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        group_id="audio-consumer",
    )
    print(f"[Audio Consumer] Listening on topic: {TOPIC}")
    for message in consumer:
        print(f"[Audio Consumer] Received: {message.value[:50]}...")


if __name__ == "__main__":
    main()
