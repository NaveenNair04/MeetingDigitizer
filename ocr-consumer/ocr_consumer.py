import os
import cv2
import numpy as np
import json
import base64
import time
from collections import deque
from difflib import SequenceMatcher
from kafka import KafkaConsumer, KafkaProducer
from paddleocr import PaddleOCR
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Kafka settings
KAFKA_SERVER = "kafka:9092"
INPUT_TOPIC = "video-stream"
OUTPUT_TOPIC = "diagram-detections"  # Changed from ocr-sentences

# Output directories
OUTPUT_DIR = "diagram_output"
DEBUG_DIR = "debug_diagrams"
RAW_DIR = "raw_messages"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DEBUG_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

# Initialize OCR with your working settings
ocr = PaddleOCR(
    lang='en',
    show_log=False,
    use_angle_cls=False   # works with PaddleOCR <= 2.6
)

# Initialize Kafka Producer (your working config)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

class DiagramDetector:
    def __init__(self, min_area=5000, min_aspect_ratio=0.3, max_aspect_ratio=3.0):
        """Initialize diagram detector with area and aspect ratio thresholds"""
        self.min_area = min_area
        self.min_aspect_ratio = min_aspect_ratio
        self.max_aspect_ratio = max_aspect_ratio
        
    def detect_diagram_regions(self, frame):
        """Detect potential diagram regions in the frame"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        
        # Apply adaptive thresholding to find structured content
        adaptive_thresh = cv2.adaptiveThreshold(
            gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY_INV, 11, 2
        )
        
        # Find contours
        contours, _ = cv2.findContours(adaptive_thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        diagram_regions = []
        for contour in contours:
            area = cv2.contourArea(contour)
            if area > self.min_area:
                x, y, w, h = cv2.boundingRect(contour)
                aspect_ratio = w / h if h > 0 else 0
                
                if self.min_aspect_ratio <= aspect_ratio <= self.max_aspect_ratio:
                    diagram_regions.append({
                        'bbox': (x, y, w, h),
                        'area': area,
                        'aspect_ratio': aspect_ratio,
                        'contour': contour
                    })
        
        return diagram_regions
    
    def classify_diagram_type(self, region_image):
        """Classify the type of diagram using simple computer vision features"""
        gray = cv2.cvtColor(region_image, cv2.COLOR_BGR2GRAY)
        h, w = gray.shape
        
        # Simple edge detection
        edges = cv2.Canny(gray, 50, 150)
        edge_density = np.sum(edges > 0) / (h * w)
        
        # Detect lines using HoughLines
        lines = cv2.HoughLines(edges, 1, np.pi/180, threshold=50)
        line_count = len(lines) if lines is not None else 0
        
        # Detect circles (for pie charts, scatter plots)
        circles = cv2.HoughCircles(gray, cv2.HOUGH_GRADIENT, 1, 20,
                                  param1=50, param2=30, minRadius=5, maxRadius=50)
        circle_count = len(circles[0]) if circles is not None else 0
        
        # Simple classification rules
        if circle_count >= 2:
            return "scatter_plot"
        elif circle_count >= 1:
            return "pie_chart"
        elif line_count >= 5 and edge_density > 0.05:
            return "chart_or_graph"
        elif edge_density > 0.1:
            return "complex_diagram"
        else:
            return "simple_diagram"

class DiagramDuplicateFilter:
    def __init__(self, time_window=10.0, similarity_threshold=0.85, max_history=50):
        """Initialize duplicate filter for diagrams"""
        self.time_window = time_window
        self.similarity_threshold = similarity_threshold
        self.max_history = max_history
        self.recent_detections = deque(maxlen=max_history)
    
    def is_duplicate(self, diagram_type, bbox, timestamp):
        """Check if diagram is a duplicate of recent detections"""
        current_time = timestamp
        
        # Clean up old detections
        cutoff_time = current_time - self.time_window
        while self.recent_detections and self.recent_detections[0]['timestamp'] < cutoff_time:
            self.recent_detections.popleft()
        
        # Check for same type in similar location within time window
        for detection in self.recent_detections:
            if detection['diagram_type'] == diagram_type:
                # Simple position-based similarity
                x1, y1, w1, h1 = bbox
                x2, y2, w2, h2 = detection['bbox']
                
                # Calculate overlap
                overlap_x = max(0, min(x1 + w1, x2 + w2) - max(x1, x2))
                overlap_y = max(0, min(y1 + h1, y2 + h2) - max(y1, y2))
                overlap_area = overlap_x * overlap_y
                
                area1 = w1 * h1
                area2 = w2 * h2
                union_area = area1 + area2 - overlap_area
                
                if union_area > 0:
                    overlap_ratio = overlap_area / union_area
                    if overlap_ratio > 0.5:  # 50% overlap = duplicate
                        return True, detection, overlap_ratio
        
        return False, None, 0.0
    
    def add_detection(self, diagram_type, bbox, timestamp, frame_id):
        """Add a new detection to the history"""
        self.recent_detections.append({
            'diagram_type': diagram_type,
            'bbox': bbox,
            'timestamp': timestamp,
            'frame_id': frame_id
        })

# Initialize components
diagram_detector = DiagramDetector()
duplicate_filter = DiagramDuplicateFilter()

def send_diagram_to_kafka(diagram_data, frame_id):
    """Send diagram data to Kafka topic and log to console"""
    try:
        # Prepare the message payload
        message = {
            'frame_id': frame_id,
            'diagram_type': diagram_data['type'],
            'bbox': diagram_data['bbox'],
            'area': diagram_data['area'],
            'aspect_ratio': diagram_data['aspect_ratio'],
            'extracted_text': diagram_data.get('text', []),
            'confidence': diagram_data.get('confidence', 0.8),
            'detection_timestamp': time.time(),
            'readable_time': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),
            'source': 'diagram_detector'
        }
        
        # Create a unique key for the message
        message_key = f"frame_{frame_id}_diagram_{diagram_data['type']}"
        
        # Send to Kafka topic
        future = producer.send(
            OUTPUT_TOPIC,
            key=message_key,
            value=message
        )
        
        # Log successful send attempt
        print(f"📤 KAFKA SEND -> Topic: {OUTPUT_TOPIC}")
        print(f"   Key: {message_key}")
        print(f"   Diagram Type: {diagram_data['type']}")
        print(f"   Area: {diagram_data['area']:.0f} pixels")
        print(f"   Text Elements: {len(diagram_data.get('text', []))}")
        print(f"   Timestamp: {message['readable_time']}")
        
        # Wait for send confirmation
        try:
            record_metadata = future.get(timeout=1)
            print(f"   ✅ Sent successfully to partition {record_metadata.partition}, offset {record_metadata.offset}")
        except Exception as send_error:
            print(f"   ⚠️ Send confirmation failed: {send_error}")
        
        print("-" * 60)
        return True
        
    except Exception as e:
        print(f"❌ KAFKA SEND FAILED for frame {frame_id}: {e}")
        return False

def extract_text_from_diagram(diagram_region):
    """Extract text from diagram using OCR"""
    try:
        result = ocr.ocr(diagram_region)
        
        texts = []
        if result and result[0]:
            for detection in result[0]:
                if len(detection) >= 2:
                    text_info = detection[1]
                    if isinstance(text_info, (tuple, list)) and len(text_info) >= 2:
                        text, confidence = text_info[0], text_info[1]
                        if confidence > 0.5:  # Only high-confidence text
                            texts.append({
                                'text': text,
                                'confidence': confidence
                            })
        
        return texts
    except Exception as e:
        print(f"⚠️ OCR failed for diagram region: {e}")
        return []

def decode_frame(message_value, frame_id):
    """Decode Kafka message -> OpenCV BGR image and extract timestamp"""
    # Save raw message for debugging
    raw_path = f"{RAW_DIR}/msg_{frame_id}.bin"
    with open(raw_path, "wb") as f:
        f.write(message_value)

    # Case 1: JSON with base64 (most likely)
    try:
        data = json.loads(message_value.decode("utf-8"))
        if "frame" in data:
            frame_bytes = base64.b64decode(data["frame"])
            frame = cv2.imdecode(np.frombuffer(frame_bytes, np.uint8), cv2.IMREAD_COLOR)
            
            # Extract timestamp if available
            timestamp = data.get("timestamp", None)
            
            if frame is not None:
                return frame, timestamp
    except Exception as e:
        print(f"⚠️ JSON decode failed for frame {frame_id}: {e}")

    # Case 2: raw JPEG bytes (fallback)
    try:
        frame = cv2.imdecode(np.frombuffer(message_value, np.uint8), cv2.IMREAD_COLOR)
        timestamp = None  # No timestamp available in raw bytes
        return frame, timestamp
    except Exception as e:
        print(f"⚠️ Raw JPEG decode failed for frame {frame_id}: {e}")
        return None, None

def process_frame(message_value, frame_id):
    """Process a single frame for diagram detection"""
    try:
        frame, timestamp = decode_frame(message_value, frame_id)
        if frame is None:
            print(f"⚠️ Frame {frame_id} could not be decoded")
            return

        # Use current time if no timestamp from message
        if timestamp is None:
            timestamp = time.time()

        # Validate frame dimensions and format
        if len(frame.shape) != 3 or frame.shape[2] != 3:
            print(f"⚠️ Frame {frame_id} has invalid format: {frame.shape}")
            return

        h, w = frame.shape[:2]
        if h < 100 or w < 100:
            print(f"⚠️ Frame {frame_id} too small for diagram detection: {w}x{h}")
            return

        print(f"🔍 Frame {frame_id} analyzing for diagrams: {frame.shape}")

        # Detect diagram regions
        diagram_regions = diagram_detector.detect_diagram_regions(frame)
        
        if not diagram_regions:
            print(f"❌ Frame {frame_id}: No diagram regions detected")
            return

        print(f"🎯 Frame {frame_id}: Found {len(diagram_regions)} potential diagram regions")

        # Process each diagram region
        detected_diagrams = []
        duplicate_count = 0
        
        for i, region in enumerate(diagram_regions):
            x, y, w, h = region['bbox']
            diagram_crop = frame[y:y+h, x:x+w]
            
            # Classify diagram type
            diagram_type = diagram_detector.classify_diagram_type(diagram_crop)
            
            # Check for duplicates
            is_dup, original_detection, similarity = duplicate_filter.is_duplicate(
                diagram_type, region['bbox'], timestamp
            )
            
            if is_dup:
                duplicate_count += 1
                print(f"🔄 Duplicate diagram detected: {diagram_type} (similarity: {similarity:.3f}, original frame: {original_detection['frame_id']})")
                continue
            
            # Extract text from diagram
            extracted_text = extract_text_from_diagram(diagram_crop)
            
            # Create diagram data
            diagram_data = {
                'type': diagram_type,
                'bbox': region['bbox'],
                'area': region['area'],
                'aspect_ratio': region['aspect_ratio'],
                'text': extracted_text,
                'confidence': min(1.0, len(extracted_text) * 0.1 + 0.7)  # Simple confidence score
            }
            
            detected_diagrams.append(diagram_data)
            duplicate_filter.add_detection(diagram_type, region['bbox'], timestamp, frame_id)
            
            print(f"✨ New diagram detected: {diagram_type} (area: {region['area']:.0f}px², text elements: {len(extracted_text)})")

        # Convert timestamp to readable format
        readable_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        
        print(f"\n🕒 Frame {frame_id} - {readable_time}")
        print(f"📊 Total regions analyzed: {len(diagram_regions)}")
        print(f"🎯 New diagrams: {len(detected_diagrams)}, Duplicates: {duplicate_count}")

        # Send diagrams to Kafka if any new ones found
        if detected_diagrams:
            print(f"\n🚀 SENDING {len(detected_diagrams)} DIAGRAMS TO KAFKA:")
            print("=" * 60)
            
            successful_sends = 0
            for diagram in detected_diagrams:
                if send_diagram_to_kafka(diagram, frame_id):
                    successful_sends += 1
                    
            print(f"\n📤 KAFKA SUMMARY: {successful_sends}/{len(detected_diagrams)} diagrams sent successfully")
            
            # Print diagram summary
            print("\n📋 DIAGRAM DETECTION SUMMARY:")
            for i, diagram in enumerate(detected_diagrams, 1):
                print(f"  [{i}] {diagram['type']} - Area: {diagram['area']:.0f}px² - Text: {len(diagram['text'])} elements")
        
        else:
            print("\n🔄 No new diagrams (all were duplicates)")

        # Save debug image with detections
        debug_frame = frame.copy()
        for i, region in enumerate(diagram_regions):
            x, y, w, h = region['bbox']
            cv2.rectangle(debug_frame, (x, y), (x + w, y + h), (0, 255, 0), 2)
            cv2.putText(debug_frame, str(i + 1), (x + 5, y + 25),
                       cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 2)
        
        cv2.imwrite(f"{DEBUG_DIR}/frame_{frame_id}_diagrams.jpg", debug_frame)

        # Print processing result
        if detected_diagrams:
            print(f"✅ Frame {frame_id} processed with {len(detected_diagrams)} NEW diagrams.\n")
        else:
            print(f"✅ Frame {frame_id} processed - all diagrams were duplicates.\n")

    except Exception as e:
        print(f"❌ Error processing frame {frame_id}: {e}")
        import traceback
        traceback.print_exc()

def consume_frames():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="diagram-detector",
        value_deserializer=lambda m: m  # Keep as bytes for now
    )

    print("📡 Listening for frames...")
    print(f"📨 Input Topic: {INPUT_TOPIC}")
    print(f"📤 Output Topic: {OUTPUT_TOPIC}")
    print("🎯 Detecting: Charts, Graphs, Tables, Flowcharts, Diagrams")
    print("=" * 60)
    
    for i, message in enumerate(consumer):
        print(f"🖼 Received frame {i}")
        process_frame(message.value, i)  # THIS IS THE KEY LINE THAT WAS MISSING

if __name__ == "__main__":
    try:
        consume_frames()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down diagram detector...")
        producer.close()
        print("✅ Producer closed successfully")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        producer.close()
