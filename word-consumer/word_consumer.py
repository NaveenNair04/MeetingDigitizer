import os
import json
import base64
import time
from kafka import KafkaConsumer
from docx import Document
from docx.shared import Inches
from datetime import datetime

OUTPUT_FILE = "/output/meeting_summary.docx"

# Messages buffer
messages = {
    "audio-transcripts": [],
    "ocr-sentences": [],
    "diagram-detections": [],
}

# Create document
document = Document()
print(f"🆕 Creating new document: {OUTPUT_FILE}")

def save_document():
    """Writes all buffered messages into the Word document, sorted by timestamp."""
    global document
    doc = Document()

    all_msgs = []

    # Normalize audio transcripts
    for m in messages["audio-transcripts"]:
        all_msgs.append({
            "timestamp": m.get("timestamp", time.time()),
            "section": "Audio Transcript",
            "content": f"[{m['speaker']} {m['start']:.2f}-{m['end']:.2f}s]: {m['text']}"
        })

    # Normalize OCR sentences
    for m in messages["ocr-sentences"]:
        all_msgs.append({
            "timestamp": m.get("detection_timestamp", time.time()),
            "section": "OCR Sentences",
            "content": f"[Line {m['line_number']}] {m['sentence']} (conf={m['confidence']:.2f})"
        })

    # Normalize diagram detections
    for m in messages["diagram-detections"]:
        all_msgs.append({
            "timestamp": m.get("detection_timestamp", time.time()),
            "section": "Diagram Detection",
            "content": f"Type: {m['diagram_type']} | Area: {m['area']} | AR: {m['aspect_ratio']}"
        })

    # Sort all by timestamp
    all_msgs.sort(key=lambda x: x["timestamp"])

    current_section = None
    for item in all_msgs:
        if item["section"] != current_section:
            doc.add_heading(item["section"], level=1)
            current_section = item["section"]

        doc.add_paragraph(item["content"])

        # Handle diagrams with image
        if item["section"] == "Diagram Detection":
            diagram_msg = next(m for m in messages["diagram-detections"]
                               if abs(m["detection_timestamp"] - item["timestamp"]) < 1e-3)
            if "diagram_image" in diagram_msg:
                try:
                    img_bytes = base64.b64decode(diagram_msg["diagram_image"])
                    img_path = "/tmp/diagram.png"
                    with open(img_path, "wb") as f:
                        f.write(img_bytes)
                    doc.add_picture(img_path, width=Inches(2))
                except Exception as e:
                    print(f"⚠️ Error inserting diagram image: {e}")

    doc.save(OUTPUT_FILE)
    print(f"💾 Saved document with {len(all_msgs)} items at {datetime.now().strftime('%H:%M:%S')}")

def main():
    consumer = KafkaConsumer(
        "audio-transcripts", "ocr-sentences", "diagram-detections",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True
    )

    print(f"📖 Word consumer started. Writing to {OUTPUT_FILE}")

    for msg in consumer:
        topic = msg.topic
        message = msg.value

        # Append normalized message
        messages[topic].append(message)
        print(f"📩 Received from {topic}: {message}")

        # Save document after every new message
        save_document()

if __name__ == "__main__":
    main()
