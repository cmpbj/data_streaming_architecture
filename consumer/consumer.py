from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
import io
import os
import time
import json

load_dotenv()

tenant_id = os.environ['TENANT_ID']
client_id = os.environ['CLIENT_ID']
client_secret = os.environ['CLIENT_SECRET']
account_name = "testdataeng"

credential = ClientSecretCredential(tenant_id, client_id, client_secret)

blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=credential)


container_name = "rawdata"
folder_path = "search_content_data/"  

consumer_conf = {
    'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ['SASL_USERNAME'],
    'sasl.password': os.environ['SASL_PASSWORD'],
    'group.id': 'content-search-consumer-2',
    'auto.offset.reset':'earliest'
}


def upload_to_azure(parquet_data, blob_name):
    """Uploads Parquet file to Azure Blob Storage"""
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(parquet_data, overwrite=True)
    print(f"✅ Uploaded: {blob_name}")

def consume_and_upload_parquet():
    consumer = Consumer(consumer_conf)
    consumer.subscribe([os.environ['TOPIC']])

    batch_size = 100
    batch_interval = 5
    messages = []
    start_time = time.time()

    try:
        print("Consuming Kafka messages...")
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka Error: {msg.error()}")
                continue

            
            value = json.loads(msg.value().decode('utf-8'))
            value["ingestion_time"] = datetime.utcnow().isoformat()
            messages.append(value)

           
            if len(messages) >= batch_size or (time.time() - start_time) >= batch_interval:
                print(f"Processing {len(messages)} messages...")

                
                table = pa.Table.from_pylist(messages)
                parquet_buffer = io.BytesIO()
                pq.write_table(table, parquet_buffer)
                parquet_buffer.seek(0) 

                
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                file_name = f"{folder_path}kafka_data_{timestamp}.parquet"

                
                upload_to_azure(parquet_buffer.getvalue(), file_name)
                messages = []
                start_time = time.time()

    except KeyboardInterrupt:
        print("🛑 Stopping Kafka consumer.")
    finally:
        consumer.close()
        print("✅ Kafka consumer closed.")

# Run Consumer
if __name__ == "__main__":
    consume_and_upload_parquet()

