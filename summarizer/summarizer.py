import re
import io
import os
from datetime import datetime
from typing import Optional, Dict, List
from docx import Document
from docx.shared import Inches
from transformers import PegasusTokenizer, PegasusForConditionalGeneration
import torch
import json
from kafka import KafkaConsumer


# ======================
# 🔹 TIMESTAMP HELPERS
# ======================
_BASE_DT = None  # type: Optional[datetime]

def extract_timestamp_seconds(line: str) -> Optional[float]:
    global _BASE_DT

    m_dt = re.search(r"(?i)timestamp:\s([\d-]+\s[\d:]+)", line)
    if m_dt:
        dt = datetime.strptime(m_dt.group(1), "%Y-%m-%d %H:%M:%S")
        if _BASE_DT is None:
            _BASE_DT = dt
        return (dt - _BASE_DT).total_seconds()

    m_secs = re.search(r"(?i)timestamp:\s*([\d.]+)s\b", line)
    if m_secs:
        return float(m_secs.group(1))

    m_hms = re.search(r"(?i)timestamp:\s*(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?", line)
    if m_hms:
        hh, mm, ss, ms = m_hms.groups()
        secs = int(hh) * 3600 + int(mm) * 60 + int(ss)
        if ms:
            secs += int(ms.ljust(3, "0")) / 1000.0
        return float(secs)

    m_t = re.search(r"t=(\d+(?:\.\d+)?)s", line)
    if m_t:
        return float(m_t.group(1))

    return None


def format_timecode(seconds: float) -> str:
    seconds = max(0.0, float(seconds))
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int(round((seconds - int(seconds)) * 1000))
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"

# ======================
# 🔹 READ DOCX HELPERS
# ======================

def read_audio_docx(path):
    global _BASE_DT
    _BASE_DT = None
    doc = Document(path)
    entries = []
    current_t, speaker, transcript = None, None, None

    for para in doc.paragraphs:
        text = para.text.strip()
        if "Timestamp:" in text or "Time Range:" in text:
            t = extract_timestamp_seconds(text)
            if t is not None:
                current_t = t
        elif text.startswith("Speaker:"):
            speaker = text.split("Speaker:")[-1].strip()
        elif text.startswith("Transcript:"):
            transcript = text.split("Transcript:")[-1].strip()

        if current_t is not None and speaker and transcript:
            entries.append({
                "t": current_t,
                "type": "audio",
                "speaker": speaker,
                "content": transcript
            })
            current_t, speaker, transcript = None, None, None
    return entries


def read_text_docx(path):
    global _BASE_DT
    _BASE_DT = None
    doc = Document(path)
    entries = []
    current_t, text_content = None, None

    for para in doc.paragraphs:
        text = para.text.strip()
        if "Timestamp:" in text:
            t = extract_timestamp_seconds(text)
            if t is not None:
                current_t = t
        elif text.startswith("Detected Text:"):
            text_content = text.split("Detected Text:")[-1].strip()

        if current_t is not None and text_content:
            entries.append({
                "t": current_t,
                "type": "text",
                "content": text_content
            })
            current_t, text_content = None, None
    return entries


def read_diagram_docx(path):
    global _BASE_DT
    _BASE_DT = None
    doc = Document(path)
    entries = []
    current_t = None
    _NS_R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"

    def _extract_images_from_paragraph(paragraph):
        blobs = []
        for run in paragraph.runs:
            for elem in run._element.iter():
                if "blip" in elem.tag:
                    rid = None
                    for attr in ["embed", "link", "id", "{%s}embed" % _NS_R]:
                        v = elem.get(attr)
                        if v:
                            rid = v
                            break
                    if rid:
                        part = paragraph.part.related_parts.get(rid)
                        if part is not None and hasattr(part, "blob") and part.blob:
                            blobs.append(part.blob)
        return blobs

    paragraphs = doc.paragraphs
    i = 0
    while i < len(paragraphs):
        text = paragraphs[i].text.strip()
        if "Timestamp:" in text:
            t = extract_timestamp_seconds(text)
            if t is not None:
                current_t = t
        elif text.startswith("Diagram Image:") and current_t is not None:
            blobs = []
            for j in range(i, min(i + 4, len(paragraphs))):
                blobs.extend(_extract_images_from_paragraph(paragraphs[j]))
            entries.append({
                "t": current_t,
                "type": "diagram",
                "images": blobs if blobs else []
            })
            current_t = None
            i += 3
        i += 1
    return entries


# ======================
# 🔹 MERGE & WRITE DOCX
# ======================

def merge_entries(audio_entries, text_entries, diagram_entries, window=5):
    all_entries = audio_entries + text_entries + diagram_entries
    all_entries = [e for e in all_entries if isinstance(e.get("t"), (int, float))]
    buckets: Dict[float, List[dict]] = {}

    for e in all_entries:
        t = float(e["t"])
        key = round(t / window) * window
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(e)

    merged = [{"t": key, "group": buckets[key]} for key in sorted(buckets.keys())]
    return merged


def write_merged_docx(merged, output_path):
    doc = Document()
    for block in merged:
        doc.add_paragraph(f"🕒 {format_timecode(block['t'])}")
        for item in block["group"]:
            if item["type"] == "audio":
                doc.add_paragraph(f"🎤 {item.get('speaker', '')}: {item['content']}")
            elif item["type"] == "text":
                doc.add_paragraph(f"📄 Text: {item['content']}")
            elif item["type"] == "diagram":
                doc.add_paragraph("📊 Diagram:")
                imgs = item.get("images", [])
                if imgs:
                    for blob in imgs:
                        stream = io.BytesIO(blob)
                        doc.add_picture(stream, width=Inches(2))
                else:
                    doc.add_paragraph("[Diagram Image Placeholder]")
        doc.add_paragraph("─" * 80)
    doc.save(output_path)


# ======================
# 🔹 PEGASUS SUMMARIZATION
# ======================

def summarize_with_pegasus(merged_docx_path, model_path="pegasus_custom", output_summary_path="summary.docx"):
    print("📖 Reading merged document for summarization...")
    doc = Document(merged_docx_path)
    full_text = "\n".join([p.text for p in doc.paragraphs if p.text.strip()])

    # ✅ Check if local model weights exist; otherwise fall back to Hugging Face model
    custom_model_dir = model_path
    has_weights = any(
        os.path.exists(os.path.join(custom_model_dir, f))
        for f in ["pytorch_model.bin", "model.safetensors"]
    )

    if has_weights:
        print(f"🧠 Loading local Pegasus model from: {model_path}")
    else:
        print("⚠️ No local Pegasus weights found — using pretrained 'google/pegasus-xsum'")
        model_path = "google/pegasus-xsum"

    tokenizer = PegasusTokenizer.from_pretrained(model_path)
    model = PegasusForConditionalGeneration.from_pretrained(model_path)

    # ----------------------------
    # 🔹 Split into manageable chunks
    # ----------------------------
    max_input_tokens = 512  # Pegasus maximum input length
    words = full_text.split()
    chunk_size = 400  # safe margin below max tokens
    chunks = [" ".join(words[i:i + chunk_size]) for i in range(0, len(words), chunk_size)]

    print(f"🪶 Summarizing {len(chunks)} chunks...")

    partial_summaries = []
    for idx, chunk in enumerate(chunks, 1):
        print(f"   → Chunk {idx}/{len(chunks)}")
        inputs = tokenizer(chunk, truncation=True, padding="longest", max_length=max_input_tokens, return_tensors="pt")

        summary_ids = model.generate(
            **inputs,
            max_length=200,
            min_length=50,
            length_penalty=2.0,
            num_beams=4
        )
        partial_summary = tokenizer.decode(summary_ids[0], skip_special_tokens=True)
        partial_summaries.append(partial_summary)

    # ----------------------------
    # 🔹 Combine chunk summaries into a final summary
    # ----------------------------
    combined_summary = " ".join(partial_summaries)
    print("🧩 Combining partial summaries...")

    # Final condensation pass (optional)
    inputs = tokenizer(combined_summary, truncation=True, max_length=max_input_tokens, return_tensors="pt")
    final_ids = model.generate(**inputs, max_length=250, min_length=60, num_beams=4)
    final_summary = tokenizer.decode(final_ids[0], skip_special_tokens=True)

    print("✅ Summary generated. Saving to DOCX...")
    out_doc = Document()
    out_doc.add_heading("Meeting Summary", level=1)
    out_doc.add_paragraph(final_summary)
    out_doc.save(output_summary_path)
    print(f"📁 Saved summary to: {output_summary_path}")

    return final_summary



# ======================
# 🔹 ENTRY POINT
# ======================

if __name__ == "__main__":

    print("⏳ Waiting for summarization trigger from Kafka...")

    consumer = KafkaConsumer(
        "pipeline-status",
        bootstrap_servers="kafka:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="summarizer-group"
    )

    # Block until "done" message is received
    for msg in consumer:
        data = msg.value
        if isinstance(data, dict) and data.get("status") == "done":
            print("✅ Received summarization trigger. Proceeding...\n")
            break

    OUTPUT_DIR = "output"

    audio_path = os.path.join(OUTPUT_DIR, "audio_transcripts.docx")
    text_path = os.path.join(OUTPUT_DIR, "ocr_sentences.docx")
    diagram_path = os.path.join(OUTPUT_DIR, "diagram_detections.docx")

    merged_path = os.path.join(OUTPUT_DIR, "merged_output.docx")
    summary_path = os.path.join(OUTPUT_DIR, "meeting_summary.docx")

    print("📂 Reading DOCX files from:", OUTPUT_DIR)
    audio_entries = read_audio_docx(audio_path)
    text_entries = read_text_docx(text_path)
    diagram_entries = read_diagram_docx(diagram_path)

    print("🧩 Merging entries...")
    merged = merge_entries(audio_entries, text_entries, diagram_entries, window=5)
    write_merged_docx(merged, merged_path)
    print(f"✅ Merged file saved to {merged_path}")

    print("🧠 Summarizing merged document...")
    summary_text = summarize_with_pegasus(
        merged_path,
        model_path="pegasus_custom",
        output_summary_path=summary_path
    )

    print("\n📝 Summary Preview:\n", summary_text)

