import subprocess
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import threading
import time
import sys

KAFKA_SERVER = "kafka:9092"
VIDEO_FILE = "/input/dp_tutorial.mp4"
CHUNK_SIZE = 4096  # Tune for latency vs throughput


def create_kafka_producer():
    """Retry KafkaProducer connection until Kafka is ready."""
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
            print("✅ Connected to Kafka.", flush=True)
            return producer
        except NoBrokersAvailable:
            print("⏳ Kafka not available. Retrying in 2 seconds...", flush=True)
            time.sleep(2)


def stream_to_kafka(topic: str, ffmpeg_cmd: list):
    """Run FFmpeg command and stream stdout to Kafka"""
    producer = create_kafka_producer()

    print(f"▶ Starting FFmpeg for `{topic}`...", flush=True)
    process = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,  # Capture FFmpeg logs
    )

    print(f"▶ Streaming `{topic}` to Kafka ...", flush=True)

    def log_stderr():
        for line in process.stderr:
            print(f"[FFmpeg:{topic}] {line.decode().strip()}", flush=True)

    # Run FFmpeg stderr logging in a background thread
    stderr_thread = threading.Thread(target=log_stderr, daemon=True)
    stderr_thread.start()

    try:
        while True:
            chunk = process.stdout.read(CHUNK_SIZE)
            if not chunk:
                break
            producer.send(topic, chunk)
            print(f"📤 Sent chunk to `{topic}` ({len(chunk)} bytes)", flush=True)
    finally:
        process.stdout.close()
        process.wait()
        producer.flush()
        print(f"✅ Finished streaming `{topic}`", flush=True)


if __name__ == "__main__":
    print("🎬 Starting the producer...", flush=True)

    # FFmpeg command to extract audio in AAC format
    audio_cmd = [
        "ffmpeg",
        "-re",
        "-i",
        VIDEO_FILE,
        "-f",
        "adts",
        "-acodec",
        "aac",
        "-vn",
        "-loglevel",
        "warning",  # Change to "info" for more detail
        "-",
    ]

    # FFmpeg command to extract video in H264 format
    video_cmd = [
        "ffmpeg",
        "-re",
        "-i",
        VIDEO_FILE,
        "-f",
        "h264",
        "-vcodec",
        "h264",
        "-an",
        "-loglevel",
        "warning",
        "-",
    ]

    # Launch threads
    audio_thread = threading.Thread(
        target=stream_to_kafka, args=("audio-stream", audio_cmd)
    )
    video_thread = threading.Thread(
        target=stream_to_kafka, args=("video-stream", video_cmd)
    )

    audio_thread.start()
    video_thread.start()

    audio_thread.join()
    video_thread.join()

    print("✅ Done streaming audio and video.", flush=True)
