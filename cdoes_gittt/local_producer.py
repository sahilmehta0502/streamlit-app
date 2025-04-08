from kafka import KafkaProducer
import json
import os
import time
from PIL import Image

# Kafka Configuration
KAFKA_TOPIC = "animal_images_stream"
KAFKA_SERVER = "localhost:9092"

# Parent folder containing species subfolders
INPUT_FOLDER = r"C:\Users\sahil\Desktop\M.tech AI sem 2\DDP_Project\archive\animals\animals"

# Function to check if Kafka is running
def check_kafka_connection():
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
        producer.close()
        print("✅ Kafka is running and reachable.")
        return True
    except Exception as e:
        print(f"❌ Error: Unable to connect to Kafka! Check if Kafka is running.\n{e}")
        return False

# Function to get image metadata
def get_image_metadata(image_path, species_name):
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            image_format = img.format  # Example: JPEG, PNG
            size_kb = round(os.path.getsize(image_path) / 1024, 2)  # File size in KB

        metadata = {
            "file_name": os.path.basename(image_path),
            "file_path": image_path,  # ✅ Send complete image path
            "species": species_name,
            "resolution": f"{width}x{height}",
            "size_kb": size_kb,
            "image_format": image_format,
            "source": "Local Dataset",
            "timestamp": time.time()
        }
        return metadata
    except Exception as e:
        print(f"❌ Error processing {image_path}: {e}")
        return None

# Main Kafka Producer Function
def send_images_to_kafka():
    if not check_kafka_connection():
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    if not os.path.exists(INPUT_FOLDER):
        print(f"❌ Error: The folder '{INPUT_FOLDER}' does not exist!")
        return

    print(f"📂 Reading images from subfolders inside: {INPUT_FOLDER}")

    # Iterate over all subfolders (each subfolder represents a species)
    for subfolder in os.listdir(INPUT_FOLDER):
        subfolder_path = os.path.join(INPUT_FOLDER, subfolder)

        # Ensure it's a directory
        if os.path.isdir(subfolder_path):
            species_name = subfolder.capitalize()  # Extract species name from subfolder
            print(f"📂 Processing species: {species_name}")

            # Get all images in the subfolder
            image_files = [f for f in os.listdir(subfolder_path) if f.lower().endswith(('.jpg', '.jpeg', '.png'))]

            if not image_files:
                print(f"⚠️ No image files found in {subfolder_path}. Skipping...")
                continue

            for image_file in image_files:
                image_path = os.path.join(subfolder_path, image_file)
                print(f"📷 Processing: {image_file} from {species_name}")

                message = get_image_metadata(image_path, species_name)
                if message:
                    try:
                        producer.send(KAFKA_TOPIC, value=message)
                        print(f"✅ Sent: {message}")
                        time.sleep(10)  # Simulating real-time streaming delay
                    except Exception as e:
                        print(f"❌ Error sending message to Kafka: {e}")

    producer.close()
    print("✅ Kafka Producer closed.")

# Run the producer function
if __name__ == "__main__":
    send_images_to_kafka()
