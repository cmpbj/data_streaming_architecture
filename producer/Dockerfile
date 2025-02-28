FROM python:3.9-slim

COPY . /app

WORKDIR /app

RUN pip install faker confluent_kafka dotenv

CMD ["python", "producer.py"]
