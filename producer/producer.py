import time
import random
from confluent_kafka import Producer
import uuid
from faker import Faker
import json
import os
from dotenv import load_dotenv
import os

fake = Faker('pt_BR')

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

load_dotenv()

producer_conf = {
    'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ['SASL_USERNAME'],
    'sasl.password': os.environ['SASL_PASSWORD']
}


def generate_unique_id():
    return fake.uuid4()[:8]

producer = Producer(producer_conf)

grocery_products = [
    "Maçã",
    "Banana",
    "Laranja",
    "Uvas",
    "Abacaxi",
    "Morangos",
    "Melancia",
    "Mamão",
    "Limão",
    "Abacate",
    "Cenoura",
    "Tomate",
    "Cebola",
    "Batata",
    "Alface",
    "Pepino",
    "Espinafre",
    "Alho",
    "Brócolis",
    "Couve-Flor",
    "Leite",
    "Iogurte",
    "Manteiga",
    "Queijo",
    "Cream Cheese",
    "Creme de Leite",
    "Queijo Cottage",
    "Chantilly",
    "Água",
    "Suco de Laranja",
    "Refrigerante",
    "Chá",
    "Café",
    "Bebida Energética",
    "Milkshake",
    "Chá Gelado",
    "Batata Frita",
    "Pipoca",
    "Barra de Chocolate",
    "Biscoitos",
    "Bolachas",
    "Castanhas",
    "Barra de Granola",
    "Pão",
    "Croissant",
    "Baguete",
    "Muffin",
    "Bolo",
    "Rosquinha",
    "Pastelaria",
    "Pizza Congelada",
    "Sorvete",
    "Vegetais Congelados",
    "Batata Congelada",
    "Carne Congelada",
    "Peito de Frango",
    "Carne Moída",
    "Costeletas de Porco",
    "Salsichas",
    "Bife",
    "Costeletas de Cordeiro",
    "Salmão",
    "Camarão",
    "Atum",
    "Bacalhau",
    "Lagosta",
    "Caranguejo",
    "Ketchup",
    "Mostarda",
    "Molho de Soja",
    "Maionese",
    "Molho Barbecue",
    "Molho Picante",
    "Arroz",
    "Farinha",
    "Açúcar",
    "Sal",
    "Feijão Preto",
    "Aveia"
]


def generate_content_search_data(id):
    lat = random.uniform(-33.0, 5.0)
    long = random.uniform(-73.0, -34.0)
    data = {
        'id': id,
        'content_search': random.choice(grocery_products),
        'lat': lat,
        'long': long
    }
    return data

if __name__ == '__main__':
    producer_id = generate_unique_id()
    while True:
        content_data = generate_content_search_data(producer_id)
        producer.produce(os.environ['TOPIC'], key=str(producer_id), value=json.dumps(content_data), callback=delivery_report)
        producer.poll(1)
        time.sleep(5)

