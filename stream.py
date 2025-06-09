import json
import random
import time
import os
import yaml
import shutil
import pandas as pd
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Load config.yaml
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

KAFKA_BROKER = config["kafka"]["broker"]
TOPIC_NAME = config["kafka"]["topic_name"]

Producer_conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(Producer_conf)

ID_TRACK_FILE = "last_id.txt"
ID_TRACK_BACKUP_FILE = "last_id_backup.txt"
MAX_RETRIES = 3

CSV_DIR = "database"  # Folder penyimpanan CSV

# Helper: Baca ID terakhir
def get_last_id_from_file():
    if os.path.exists(ID_TRACK_FILE):
        try:
            with open(ID_TRACK_FILE, "r") as f:
                return int(f.read().strip())
        except Exception as e:
            print(f"⚠️ Gagal membaca file ID: {e}")
    return 0

# Helper: Simpan ID terakhir
def update_last_id_in_file(last_id):
    retries = 0
    success = False
    while retries < MAX_RETRIES and not success:
        try:
            if os.path.exists(ID_TRACK_FILE):
                shutil.copy(ID_TRACK_FILE, ID_TRACK_BACKUP_FILE)
            with open(ID_TRACK_FILE, "w") as f:
                f.write(str(last_id))
            success = True
            print(f"✅ ID terakhir {last_id} berhasil disimpan.")
        except Exception as e:
            retries += 1
            print(f"⚠️ Gagal menulis file ID (Coba {retries}/{MAX_RETRIES}): {e}")
            if retries == MAX_RETRIES:
                print("❌ Gagal menyimpan ID setelah beberapa percobaan.")
    return success

# Helper: Baca list nilai dari CSV
def get_list_from_csv(column, csv_name):
    try:
        path = os.path.join(CSV_DIR, f"{csv_name}.csv")
        df = pd.read_csv(path)
        return df[column].dropna().astype(int).tolist()
    except Exception as e:
        print(f"❌ Gagal membaca {csv_name}.csv: {e}")
        return []

# Load data referensi dari CSV
product_list = get_list_from_csv("id", "df_product")
customer_list = get_list_from_csv("id", "df_cust")
branch_list = get_list_from_csv("id", "df_branch")
payment_method_list = get_list_from_csv("id", "df_payment_method")
order_status_list = get_list_from_csv("id", "df_order_status")
payment_status_list = get_list_from_csv("id", "df_payment_status")
shipping_status_list = get_list_from_csv("id", "df_shipping_status")

# Bobot status
order_status_list_weights = [0.1, 0.9]  # contoh
payment_status_list_weights = [0.05, 0.9, 0.05]
shipping_status_list_weights = [0.05, 0.9, 0.05]

# Ambil ID terakhir
last_id = get_last_id_from_file()

# Fungsi generate data penjualan
def generate_sales_data(start_id):
    num_rows = random.randint(1, 5)
    sales_data = []

    for i in range(num_rows):
        id = start_id + i + 1
        product_id = random.choice(product_list)
        customer_id = random.choice(customer_list)
        branch_id = random.choice(branch_list)
        quantity = random.randint(1, 5)
        payment_method = random.choice(payment_method_list)
        order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_status = random.choices(order_status_list, weights=order_status_list_weights, k=1)[0]
        payment_status = None
        shipping_status = None
        is_online_transaction = random.choice([True, False])
        delivery_fee = 0
        is_free_delivery_fee = None

        if is_online_transaction:
            delivery_fee = random.randint(12000, 50000)
            is_free_delivery_fee = random.choice([True, False])

        if order_status == 2:
            payment_status = random.choices(payment_status_list, weights=payment_status_list_weights, k=1)[0]
            if payment_status == 2:
                shipping_status = random.choices(shipping_status_list, weights=shipping_status_list_weights, k=1)[0]

        if not is_online_transaction:
            shipping_status = None

        payload = {
            "id": id,
            "product_id": product_id,
            "customer_id": customer_id,
            "branch_id": branch_id,
            "quantity": quantity,
            "payment_method": payment_method,
            "order_date": order_date,
            "order_status": order_status,
            "payment_status": payment_status,
            "shipping_status": shipping_status,
            "is_online_transaction": is_online_transaction,
            "delivery_fee": delivery_fee,
            "is_free_delivery_fee": is_free_delivery_fee,
            "created_at": order_date,
            "modified_at": None
        }

        schema = {
            "schema": {
                "fields": [
                    {"type": "int32", "optional": False, "field": "id"},
                    {"type": "int32", "optional": False, "field": "product_id"},
                    {"type": "int32", "optional": False, "field": "customer_id"},
                    {"type": "int32", "optional": False, "field": "branch_id"},
                    {"type": "int32", "optional": False, "field": "quantity"},
                    {"type": "int32", "optional": True, "field": "payment_method"},
                    {"type": "string", "optional": True, "field": "order_date"},
                    {"type": "int32", "optional": True, "field": "order_status"},
                    {"type": "int32", "optional": True, "field": "payment_status"},
                    {"type": "int32", "optional": True, "field": "shipping_status"},
                    {"type": "boolean", "optional": True, "field": "is_online_transaction"},
                    {"type": "int32", "optional": True, "field": "delivery_fee"},
                    {"type": "boolean", "optional": True, "field": "is_free_delivery_fee"},
                    {"type": "string", "optional": True, "field": "created_at"},
                    {"type": "string", "optional": True, "field": "modified_at"},
                ],
                "optional": False,
                "name": "sales_schema",
                "type": "struct"
            },
            "payload": payload
        }

        sales_data.append((id, schema))
    return sales_data

# Callback Kafka
def delivery_report(err, msg, id_sent):
    if err is not None:
        print(f"❌ Gagal mengirim pesan ID {id_sent}: {err}")
    else:
        print(f"✅ Data ID {id_sent} dikirim ke Kafka: {msg.topic()} [{msg.partition()}]")
        update_last_id_in_file(id_sent)

# Main loop
while True:
    sales_data = generate_sales_data(last_id)
    for id_sent, record in sales_data:
        producer.produce(
            TOPIC_NAME,
            json.dumps(record).encode("utf-8"),
            callback=lambda err, msg, id_sent=id_sent: delivery_report(err, msg, id_sent)
        )
        last_id = id_sent
    producer.flush()

    print(f"✅ {len(sales_data)} data berhasil dikirim ke Kafka!")
    time.sleep(10)
