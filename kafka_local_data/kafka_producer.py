from kafka import KafkaProducer
import json
import time
import os
from PIL import Image

# ✅ Kafka Configuration
KAFKA_TOPIC = "image_metadata"
KAFKA_BROKER = "localhost:9092"

# ✅ Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ✅ Folder containing images
image_folder = "./dataset/"

# ✅ Extract metadata and send to Kafka
def extract_metadata(image_path):
    try:
        img = Image.open(image_path)
        metadata = {
            "file_name": os.path.basename(image_path),
            "image_format": img.format,
            "resolution": f"{img.width}x{img.height}",
            "size_kb": round(os.path.getsize(image_path) / 1024, 2)
        }
        return metadata
    except Exception as e:
        print(f"Error processing {image_path}: {e}")
        return None

# ✅ Read images and stream metadata
image_files = [os.path.join(image_folder, f) for f in os.listdir(image_folder) if f.endswith((".jpg", ".png"))]

for img_path in image_files:
    metadata = extract_metadata(img_path)
    if metadata:
        print(f"📤 Sending: {metadata}")
        producer.send(KAFKA_TOPIC, metadata)
    time.sleep(2)  # Simulate streaming delay

producer.close()
