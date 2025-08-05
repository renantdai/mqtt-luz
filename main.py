from fastapi import FastAPI, Query
from kafka import KafkaConsumer
import paho.mqtt.publish as publish
from pydantic import BaseModel
import asyncio
import json
import base64
import uuid
import mysql.connector
import os
from aiokafka import AIOKafkaConsumer

# --- Configurações do MySQL e Kafka (idealmente de variáveis de ambiente) ---
MYSQL_CONFIG = {
    'user': os.getenv('MYSQL_USER', 'seu_usuario'),
    'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),
    'host': os.getenv('MYSQL_HOST', 'seu_host_mysql'),
    'database': os.getenv('MYSQL_DB', 'seu_banco')
}
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = 'mqtt'

app = FastAPI()

# --- Funções de Persistência no MySQL ---

def get_db_connection():
    """Cria e retorna uma nova conexão com o MySQL."""
    return mysql.connector.connect(**MYSQL_CONFIG)

def create_table_if_not_exists():
    """Cria a tabela no MySQL se ela ainda não existir."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS kafka_messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                topic VARCHAR(255),
                payload TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Erro ao criar a tabela: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def save_to_mysql(message_topic, message_payload):
    """Salva a mensagem processada no MySQL."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = "INSERT INTO kafka_messages (topic, payload) VALUES (%s, %s)"
        
        # Converte o payload para uma string JSON para salvar no banco
        json_payload = json.dumps(message_payload, ensure_ascii=False)
        
        cursor.execute(sql, (message_topic, json_payload))
        conn.commit()
        print(f"Mensagem salva no MySQL: {message_payload}")
    except mysql.connector.Error as err:
        print(f"Erro no MySQL: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- Nova Função para Consumir Kafka em Segundo Plano ---
async def kafka_listener():
    """Função que escuta o Kafka continuamente e salva no banco de dados."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="fastapi-continuous-consumer",
        auto_offset_reset='earliest'
    )
    
    await consumer.start()
    
    try:
        async for msg in consumer:
            print(f"Recebida mensagem do tópico {msg.topic}")
            processed_data = {}
            
            try:
                decoded = msg.value.decode('utf-8')
                try:
                    processed_data = json.loads(decoded)
                except json.JSONDecodeError:
                    processed_data = {"raw_string": decoded}
            except UnicodeDecodeError:
                processed_data = {
                    "raw_base64": base64.b64encode(msg.value).decode('utf-8')
                }
            
            save_to_mysql(msg.topic, processed_data)
            
    finally:
        await consumer.stop()

# --- Eventos de Inicialização e Desligamento do FastAPI ---

@app.on_event("startup")
async def startup_event():
    """Cria a tabela no MySQL e inicia a tarefa de escuta do Kafka."""
    print("Iniciando a aplicação...")
    create_table_if_not_exists()
    asyncio.create_task(kafka_listener())
    
@app.on_event("shutdown")
async def shutdown_event():
    """Lógica para fechar recursos, se necessário."""
    print("Desligando a aplicação e o consumidor Kafka.")

# --- Rotas Originais ---

@app.get("/")
def root():
    return {"message": "API funcionando!"}


@app.get("/mensagens")
def ler_mensagens_old(qtd: int = Query(1, ge=1, le=100)):
    kafka_topic = 'mqtt'  # aqui é o tópico no Kafka, que pode ser qualquer string

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=str(uuid.uuid4()),  # novo group_id para não conflitar
        consumer_timeout_ms=15000
    )

    mensagens = []
    totalmensagem = 0
    for i, msg in enumerate(consumer):
        try:
            decoded = msg.value.decode('utf-8')
            try:
                # Tenta converter para JSON se possível
                mensagens.append(json.loads(decoded))
            except json.JSONDecodeError:
                mensagens.append(decoded)
        except UnicodeDecodeError:
            # Se não for UTF-8, envia como base64
            mensagens.append({
                "raw_base64": base64.b64encode(msg.value).decode('utf-8')
            })
        totalmensagem = i
        if i + 1 >= qtd:
            break

    consumer.close()

    return {"mensagens": mensagens, "total": totalmensagem}


class MsgRequest(BaseModel):
    msg: str


@app.post("/enviar-mqtt")
def enviar_mqtt(request: MsgRequest):
    publish.single(
        topic="/imbe/v1/uplink/A0DD6C711F70",
        payload=request.msg,
        hostname="mqtt.kntsys.com.br",
        port=1880,
        auth={
            'username': 'imbe',
            'password': 'prefeitura'
        }
    )
    return {"status": "mensagem enviada"}