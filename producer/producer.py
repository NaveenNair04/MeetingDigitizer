import subprocess
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading
import time
import json
import base64

KAFKA_SERVER = "kafka:9092"
VIDEO_FILE = "/input/dp_tutorial.mp4"
CHUNK_SIZE = 4096  # bytes

def create_kafka_producer():
    """Retry KafkaProducer connection until Kafka is ready."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Connected to Kafka.", flush=True)
            return producer
        except NoBrokersAvailable:
            print("Kafka not available. Retrying in 2 seconds...", flush=True)
            time.sleep(2)

def stream_to_kafka(topic: str, ffmpeg_cmd: list):
    """Run FFmpeg command and stream stdout to Kafka with timestamps"""
    producer = create_kafka_producer()

    print(f"Starting FFmpeg for {topic}...", flush=True)
    process = subprocess.Popen(
        ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    def log_stderr():
        for line in process.stderr:
            line = line.decode(errors="ignore").strip()
            if line:
                print(f"[FFmpeg:{topic}] {line}", flush=True)

    threading.Thread(target=log_stderr, daemon=True).start()
    print(f"Streaming {topic} to Kafka ...", flush=True)

    try:
        while True:
            chunk = process.stdout.read(CHUNK_SIZE)
            if not chunk:
                break

            # Base64-encode the raw bytes so we can ship them inside JSON safely
            message = {
                "timestamp": time.time(),
                "data": base64.b64encode(chunk).decode("utf-8"),
            }
            producer.send(topic, message)

            print(
                f"[Producer] Sent chunk to {topic} (size={len(chunk)} bytes, ts={message['timestamp']:.6f})",
                flush=True,
            )
    finally:
        process.stdout.close()
        process.wait()
        producer.flush()
        print(f"Finished streaming {topic}", flush=True)

if __name__ == "__main__":
    print("Starting the producer...", flush=True)

    # AUDIO: output raw PCM s16le @ 16 kHz mono (easy to decode on consumer)
    audio_cmd = [
        "ffmpeg", "-re", "-i", VIDEO_FILE,
        "-f", "s16le", "-acodec", "pcm_s16le",
        "-ac", "1", "-ar", "16000",
        "-vn", "-loglevel", "warning", "-"
    ]

    # VIDEO: unchanged (still sending H264 elementary stream)
    video_cmd = [
        "ffmpeg", "-re", "-i", VIDEO_FILE,
        "-f", "h264", "-vcodec", "h264",
        "-an", "-loglevel", "warning", "-"
    ]

    t1 = threading.Thread(target=stream_to_kafka, args=("audio-stream", audio_cmd))
    t2 = threading.Thread(target=stream_to_kafka, args=("video-stream", video_cmd))
    t1.start(); t2.start()
    t1.join(); t2.join()

    print("Done streaming audio and video.", flush=True)
