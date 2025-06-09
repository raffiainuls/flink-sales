import pandas as pd
from confluent_kafka import Producer
import numpy as np
import json

# Konfigurasi Kafka
KAFKA_BROKER = "localhost:9093"
producer_conf = {"bootstrap.servers": KAFKA_BROKER}
producer = Producer(producer_conf)

# Fungsi untuk membaca file daftar topic dan CSV
def read_topic_csv_mapping(file_txt):
    try:
        with open(file_txt, "r") as f:
            lines = f.readlines()
        mappings = [line.strip().split(",") for line in lines if line.strip()]
        return mappings
    except Exception as e:
        print(f"❌ Gagal membaca file '{file_txt}': {e}")
        return []

# Fungsi membaca CSV
def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"❌ Gagal membaca CSV '{file_path}': {e}")
        return None

# Buat schema dinamis berdasarkan CSV
def generate_schema(df):
    schema_fields = []
    for column in df.columns:
        if pd.api.types.is_integer_dtype(df[column]):
            field_type = "int32"
        elif pd.api.types.is_float_dtype(df[column]):
            field_type = "float"
        elif pd.api.types.is_bool_dtype(df[column]):
            field_type = "boolean"
        else:
            field_type = "string"
        schema_fields.append({
            "type": field_type,
            "optional": True,
            "field": column
        })
    return {
        "schema": {
            "type": "struct",
            "fields": schema_fields,
            "optional": False,
            "name": "dynamic_schema"
        }
    }

# Konversi NaN ke `null`
def replace_nan(obj):
    return None if (isinstance(obj, float) and np.isnan(obj)) else obj

# Callback Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Gagal mengirim pesan: {err}")
    else:
        print(f"✅ Data dikirim ke Kafka: {msg.topic()} [{msg.partition()}] dengan key: {msg.key().decode('utf-8')}")

# Fungsi untuk mengirim data ke Kafka
def send_to_kafka(topic_name, file_path):
    df = read_csv(file_path)
    if df is None or df.empty:
        print(f"❌ Tidak ada data dalam file '{file_path}'.")
        return

    schema_template = generate_schema(df)  # Buat schema berdasarkan CSV
    total_sent = 0  # Counter untuk tracking jumlah row yang berhasil dikirim

    for _, row in df.iterrows():
        row_dict = {col: replace_nan(row[col]) for col in df.columns}
        key = str(row_dict.get("id", "unknown"))  # Ambil ID sebagai key utama
        message = schema_template.copy()
        message["payload"] = row_dict

        try:
            producer.produce(
                topic_name,
                key=key.encode("utf-8"),
                value=json.dumps(message, ensure_ascii=False).encode("utf-8"),
                callback=delivery_report
            )
            total_sent += 1
        except Exception as e:
            print(f"❌ Gagal mengirim row {key}: {e}")

    producer.flush()  # Pastikan semua pesan benar-benar dikirim sebelum selesai
    print(f"✅ {total_sent}/{len(df)} data berhasil dikirim ke Kafka di topic '{topic_name}'!")

# Jalankan program dengan looping
file_txt = "list_file.txt"  # Ganti dengan path file daftar topic dan CSV
mappings = read_topic_csv_mapping(file_txt)

for topic, csv_file in mappings:
    print(f"🔄 Memproses: Topic='{topic}', File='{csv_file}'")
    send_to_kafka(topic, csv_file)
