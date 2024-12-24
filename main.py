from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from faker import Faker
import json
import random
import datetime
import time

# Configurações do Kafka
KAFKA_BROKER = 'localhost:9092'  # Endereço do servidor Kafka
KAFKA_TOPIC = 'pratica-kafka-vendas'  # Nome do tópico

# Configurações do MongoDB
MONGO_URI = "mongodb://localhost:27017/"  # URI de conexão ao MongoDB
MONGO_DB = "kafka-vendas"  # Nome do banco de dados
MONGO_COLLECTION = "vendas"  # Nome da coleção

# Inicializar Faker para gerar dados fictícios
fake = Faker()

# Conectar ao MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

# Criar o Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serializar mensagens em JSON
)


# Função para gerar vendas aleatórias
def gerar_venda():
    produtos = [
        {"nome": "Smartphone", "preco": random.uniform(500, 2000)},
        {"nome": "Notebook", "preco": random.uniform(2000, 5000)},
        {"nome": "Teclado", "preco": random.uniform(100, 500)},
        {"nome": "Monitor", "preco": random.uniform(600, 1500)},
        {"nome": "Fone de Ouvido", "preco": random.uniform(50, 300)}
    ]
    # Escolher produtos aleatoriamente
    produtos_selecionados = random.sample(produtos, k=random.randint(1, 3))

    # Montar a venda
    venda = {
        "usuario": {
            "nome": fake.name(),
            "email": fake.email()
        },
        "produtos": [
            {"nome": p["nome"], "preco": round(p["preco"], 2)} for p in produtos_selecionados
        ],
        "data_compra": datetime.datetime.now().isoformat()
    }
    return venda


# Gerar e enviar vendas aleatórias para o Kafka
def gerar_vendas_para_kafka():
    for _ in range(10):  # Enviar 10 vendas aleatórias
        venda = gerar_venda()
        producer.send(KAFKA_TOPIC, venda)
        print(f"Venda enviada para Kafka: {venda}")
        time.sleep(1)  # Esperar 1 segundo entre as mensagens


# Criar o Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='grupo_consumidor_vendas',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Decodificar mensagens JSON
)


# Consumir mensagens do Kafka e salvar no MongoDB
def consumir_e_salvar_no_mongodb():
    print(f"Conectado ao tópico Kafka: {KAFKA_TOPIC}")
    print(f"Salvando dados na coleção MongoDB: {MONGO_COLLECTION}")
    try:
        for message in consumer:
            venda = message.value  # Conteúdo da mensagem Kafka
            print(f"Recebido: {venda}")

            # Salvar no MongoDB
            collection.insert_one(venda)
            print("Venda salva no MongoDB!")
    except KeyboardInterrupt:
        print("\nEncerrando consumer...")
    finally:
        consumer.close()
        client.close()


# Executar o pipeline
if __name__ == "__main__":
    print("Gerando vendas aleatórias e enviando para Kafka...")
    gerar_vendas_para_kafka()
    print("Consumindo mensagens do Kafka e salvando no MongoDB...")
    consumir_e_salvar_no_mongodb()
