FROM python:3.9-slim

COPY . /app

WORKDIR /app

RUN pip install azure-identity azure-storage-blob confluent_kafka dotenv pyarrow

CMD ["python", "consumer.py"]
