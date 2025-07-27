from kafka import KafkaConsumer
import os
import cv2
import numpy as np

# Setup
VIDEO_TOPIC = "video-stream"
AUDIO_TOPIC = "audio-stream"
BOOTSTRAP_SERVERS = ["kafka:9092"]

# Output files (optional, for testing)
os.makedirs("output", exist_ok=True)
video_out = open("output/output.h264", "wb")
audio_out = open("output/output.aac", "wb")

# Initialize Kafka consumers
video_consumer = KafkaConsumer(
    VIDEO_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="video-consumer-group",
    enable_auto_commit=True,
)

audio_consumer = KafkaConsumer(
    AUDIO_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    group_id="audio-consumer-group",
    enable_auto_commit=True,
)

print("🎥 Listening for audio and video chunks...")


# Decode H264 stream using OpenCV and display it
def decode_and_show(frame_bytes):
    np_arr = np.frombuffer(frame_bytes, np.uint8)
    try:
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        if frame is not None:
            cv2.imshow("Video Stream", frame)
            if cv2.waitKey(1) & 0xFF == ord("q"):
                raise KeyboardInterrupt
    except Exception as e:
        print(f"⚠️ Failed to decode frame: {e}")


# Process both consumers concurrently
try:
    while True:
        for message in video_consumer.poll(timeout_ms=1000).values():
            for record in message:
                print(f"[VIDEO] Received chunk of {len(record.value)} bytes")
                video_out.write(record.value)
                decode_and_show(record.value)

        for message in audio_consumer.poll(timeout_ms=1000).values():
            for record in message:
                print(f"[AUDIO] Received chunk of {len(record.value)} bytes")
                audio_out.write(record.value)

except KeyboardInterrupt:
    print("👋 Shutting down consumer...")

finally:
    video_out.close()
    audio_out.close()
    cv2.destroyAllWindows()
