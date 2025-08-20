import json
import base64
import numpy as np
from kafka import KafkaConsumer
from resemblyzer import VoiceEncoder, preprocess_wav
from faster_whisper import WhisperModel
from sklearn.cluster import AgglomerativeClustering

TOPIC = "audio-stream"
BOOTSTRAP_SERVERS = "kafka:9092"

SAMPLE_RATE = 16000
BYTES_PER_SAMPLE = 2  # s16le
CHANNELS = 1
WINDOW_SECONDS = 5.0
WINDOW_BYTES = int(SAMPLE_RATE * BYTES_PER_SAMPLE * CHANNELS * WINDOW_SECONDS)

EMB_WIN_SEC = 1.5
EMB_HOP_SEC = 0.75

encoder = VoiceEncoder()  # CPU by default
whisper_model = WhisperModel("base", device="cpu")  # or "cuda"


def pcm_bytes_to_float32(pcm_bytes: bytes) -> np.ndarray:
    x = np.frombuffer(pcm_bytes, dtype=np.int16).astype(np.float32)
    if CHANNELS > 1:
        x = x.reshape(-1, CHANNELS).mean(axis=1)
    x /= 32768.0
    return x


def frame_audio(wav: np.ndarray, sr: int, win_sec: float, hop_sec: float):
    win = int(win_sec * sr)
    hop = int(hop_sec * sr)
    for start in range(0, max(1, len(wav) - win + 1), hop):
        end = start + win
        if end <= len(wav):
            yield start / sr, end / sr, wav[start:end]


def diarize(wav16k: np.ndarray):
    """Perform real speaker diarization using embeddings + clustering."""
    frames = list(frame_audio(wav16k, SAMPLE_RATE, EMB_WIN_SEC, EMB_HOP_SEC))
    if not frames:
        return []

    embs = []
    times = []
    for s, e, seg in frames:
        emb = encoder.embed_utterance(seg)
        embs.append(emb)
        times.append((s, e))
    embs = np.vstack(embs)

    # Agglomerative clustering works well for unknown number of speakers
    cluster_model = AgglomerativeClustering(n_clusters=None, distance_threshold=0.75)
    labels = cluster_model.fit_predict(embs)

    diar_segments = []
    for (s, e), lab in zip(times, labels):
        diar_segments.append((s, e, f"spk{lab+1}"))
    return diar_segments


def assign_speakers_to_asr_segments(diar, asr_segments):
    if not diar:
        return [("spk?", s) for s in asr_segments]

    diar_centers = np.array([(s + e) / 2.0 for s, e, _ in diar])
    out = []
    for seg in asr_segments:
        seg_mid = (seg.start + seg.end) / 2.0
        idx = int(np.argmin(np.abs(diar_centers - seg_mid)))
        spk = diar[idx][2]
        out.append((spk, seg))
    return out


def process_window(pcm_chunk: bytes, kafka_timestamp: float):
    wav = pcm_bytes_to_float32(pcm_chunk)
    diar = diarize(wav)

    segments, _ = whisper_model.transcribe(wav, beam_size=5)
    tagged = assign_speakers_to_asr_segments(diar, list(segments))

    for spk, seg in tagged:
        text = (seg.text or "").strip()
        if text:
            print(
                f"[ASR] ts={kafka_timestamp:.6f} {spk} "
                f"{seg.start:.2f}-{seg.end:.2f}s: {text}",
                flush=True,
            )


def _parse_payload(raw):
    if isinstance(raw, (bytes, bytearray)):
        return json.loads(raw.decode("utf-8"))
    if isinstance(raw, dict):
        return raw
    raise TypeError(f"Unexpected message.value type: {type(raw)}")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        group_id="audio-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"[Audio Consumer] Listening on topic: {TOPIC}", flush=True)

    buffer = bytearray()
    last_ts = None

    for msg in consumer:
        try:
            payload = _parse_payload(msg.value)
            audio_bytes = base64.b64decode(payload["data"])
            ts = float(payload["timestamp"])
            last_ts = ts

            buffer.extend(audio_bytes)

            while len(buffer) >= WINDOW_BYTES:
                window = bytes(buffer[:WINDOW_BYTES])
                del buffer[:WINDOW_BYTES]
                process_window(window, last_ts)

        except Exception as e:
            print(f"[Audio Consumer] Error: {e}", flush=True)


if __name__ == "__main__":
    main()
