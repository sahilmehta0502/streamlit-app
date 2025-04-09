# updated_consumer_EfficientNetB7_model.py

from kafka import KafkaConsumer
import json
import torch
import torchvision.transforms as transforms
from torchvision import models
from PIL import Image
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Kafka Configuration
KAFKA_TOPIC = "animal_images_stream"
KAFKA_SERVER = "localhost:9092"

# Google Sheets setup
def get_gsheet_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    return client

# Save record to Google Sheet
def save_to_google_sheet(data):
    try:
        client = get_gsheet_client()
        sheet = client.open("animal_classifications").sheet1
        row = [
            data["file_name"],
            data["species"],
            data["accuracy"],
            data["file_size_kb"],
            data["resolution"],
            data["processing_speed_ms"],
            data["timestamp"]
        ]
        sheet.append_row(row)
        print("✅ Added to Google Sheet.")
    except Exception as e:
        print(f"❌ Google Sheet update failed: {e}")

# Load Pretrained EfficientNet-B7 Model
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = models.efficientnet_b7(weights=models.EfficientNet_B7_Weights.DEFAULT)
model.eval().to(device)

# Image Preprocessing
transform = transforms.Compose([
    transforms.Resize((600, 600)),
    transforms.CenterCrop(600),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
])

# Load ImageNet Labels
imagenet_labels = models.EfficientNet_B7_Weights.DEFAULT.meta["categories"]

# Classification Function
def classify_animal(image_path):
    try:
        image = Image.open(image_path).convert("RGB")
        width, height = image.size
        start_time = time.time()
        image_tensor = transform(image).unsqueeze(0).to(device)
        with torch.no_grad():
            output = model(image_tensor)
        end_time = time.time()
        processing_speed = round((end_time - start_time) * 1000, 2)  # in ms
        predicted_class = output.argmax(dim=1).item()
        confidence = torch.nn.functional.softmax(output, dim=1)[0][predicted_class].item()
        species = imagenet_labels[predicted_class]
        return species, round(confidence * 100, 2), f"{width}x{height}", processing_speed
    except Exception as e:
        print(f"❌ Error processing {image_path}: {e}")
        return "Error", 0.0, "Unknown", 0.0

# Kafka Consumer
def consume_images_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print("✅ Kafka Consumer is running... Listening for image data.")

    for message in consumer:
        data = message.value
        image_path = data.get("file_path")
        timestamp = data.get("timestamp", "Unknown")

        if not image_path or not os.path.exists(image_path):
            print(f"⚠️ Image not found: {image_path}. Skipping...")
            continue

        print(f"\n📷 Processing image: {image_path}")
        species, accuracy, resolution, processing_speed = classify_animal(image_path)
        file_size = round(os.path.getsize(image_path) / 1024, 2)  # KB

        processed_data = {
            "file_name": os.path.basename(image_path),
            "species": species,
            "accuracy": accuracy,
            "file_size_kb": file_size,
            "resolution": resolution,
            "processing_speed_ms": processing_speed,
            "timestamp": timestamp
        }

        print(f"🔍 {species} ({accuracy}%) | Size: {file_size} KB | Resolution: {resolution} | Speed: {processing_speed} ms")
        save_to_google_sheet(processed_data)

# Run the Consumer
if __name__ == "__main__":
    consume_images_from_kafka()
