from fastapi import FastAPI, Query, HTTPException
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
from decode_lumi import decode_lumi
from fastapi.responses import JSONResponse
from datetime import datetime
import lumi_commands
import mqtt_publisher

# --- Configurações do MySQL e Kafka (idealmente de variáveis de ambiente) ---
MYSQL_CONFIG = {
    'user': os.getenv('DB_USER', 'user'),
    'password': os.getenv('DB_PASS', 'pass'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'mensagens'),
    'port': int(os.getenv('DB_PORT', 3306)),
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
            CREATE TABLE IF NOT EXISTS dados_lumi (
                id INT AUTO_INCREMENT PRIMARY KEY,
                kafka_partition VARCHAR(255),
                kafka_offset BIGINT,
                kafka_topic VARCHAR(255),
                kafka_key VARCHAR(255),
                device VARCHAR(255),
                comando INT,
                estado_rele BOOLEAN,
                dimmer_rele INT,
                tensao_rele FLOAT,
                corrente_rele FLOAT,
                potencia_ativa FLOAT,
                fator_potencia INT,
                potencia_aparente FLOAT,
                tempo_ligada INT,
                consumo_energia INT,
                versao_firmware INT,
                lux INT,
                modo_foto_celula BOOLEAN,
                timestamp INT,
                data_hora DATETIME,
                rssi INT,
                latitude FLOAT,
                longitude FLOAT,
                UNIQUE KEY uk_kafka_message (kafka_partition, kafka_offset, kafka_topic)
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
    """Função que escuta o Kafka continuamente e processa mensagens."""
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="fastapi-continuous-consumer-novo-test",
        auto_offset_reset='earliest'
    )

    await consumer.start()

    try:
        async for msg in consumer:
            print(f"Recebida mensagem do tópico {msg.topic}")
            try:
                decoded = msg.value.decode('utf-8')
                payload_json = json.loads(decoded)

                # Verifica se a estrutura está correta
                if isinstance(payload_json, dict) and 'payload' in payload_json:
                    payload_base64 = payload_json['payload']
                    print(f"Payload Base64: {payload_base64}")
                    try:
                        # 1. Obtenha os dados decodificados do payload
                        decoded_data = decode_lumi(payload_base64)
                        
                        # 2. ADICIONE OS METADADOS DO KAFKA AO DICIONÁRIO
                        decoded_data['kafka_partition'] = msg.partition
                        decoded_data['kafka_offset'] = msg.offset
                        decoded_data['kafka_topic'] = msg.topic
                        decoded_data['kafka_key'] = msg.key
                        decoded_data['device'] = payload_json['device']
                        
                        # 3. Agora o dicionário está completo para ser salvo
                        salvar_dados_lumi(decoded_data)
                        
                    except Exception as e:
                        print(f"Erro ao decodificar ou salvar payload: {e}")
                else:
                    print("Mensagem ignorada: formato inesperado")

            except Exception as e:
                print(f"Erro ao processar mensagem: {e}")

    finally:
        await consumer.stop()


def consultar_dados_lumi(limit: int = 100):
    """Consulta os últimos registros da tabela dados_lumi."""
    conn = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT *
            FROM dados_lumi
            ORDER BY data_hora DESC
            LIMIT %s
        """, (limit,))

        resultados = cursor.fetchall()

        # Converte datetime para string
        for row in resultados:
            if isinstance(row.get("data_hora"), datetime):
                row["data_hora"] = row["data_hora"].isoformat()

        return resultados

        # Converte datetime para string
        for row in resultados:
            if isinstance(row.get("data_hora"), datetime):
                row["data_hora"] = row["data_hora"].isoformat()

        return resultados

    except mysql.connector.Error as err:
        print(f"Erro ao consultar dados_lumi: {err}")
        return []

    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
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


def salvar_dados_lumi(decoded_data: dict):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        sql = """
            INSERT INTO dados_lumi (
                kafka_partition, kafka_offset, kafka_topic, kafka_key, device,
                comando, estado_rele, dimmer_rele, tensao_rele, corrente_rele,
                potencia_ativa, fator_potencia, potencia_aparente, tempo_ligada,
                consumo_energia, versao_firmware, lux, modo_foto_celula,
                timestamp, data_hora, rssi, latitude, longitude
            ) VALUES (
                %(kafka_partition)s, %(kafka_offset)s, %(kafka_topic)s, %(kafka_key)s, %(device)s,
                %(comando)s, %(estado_rele)s, %(dimmer_rele)s, %(tensao_rele)s, %(corrente_rele)s,
                %(potencia_ativa)s, %(fator_potencia)s, %(potencia_aparente)s, %(tempo_ligada)s,
                %(consumo_energia)s, %(versao_firmware)s, %(lux)s, %(modo_foto_celula)s,
                %(timestamp)s, %(data_hora)s, %(rssi)s, %(latitude)s, %(longitude)s
            )
        """

        cursor.execute(sql, decoded_data)
        conn.commit()
        print("Dados salvos no MySQL com sucesso.")

    except mysql.connector.IntegrityError as err:
        if err.errno == 1062:
            print("Mensagem Kafka já registrada, ignorando duplicata.")
        else:
            print(f"Erro ao salvar dados decodificados: {err}")
    except mysql.connector.Error as err:
        print(f"Erro geral no MySQL: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

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

@app.get("/dados-lumi")
def route_consultar_dados_lumi(limit: int = Query(100, ge=1, le=1000)):
    """Retorna os últimos registros da tabela dados_lumi."""
    dados = consultar_dados_lumi(limit)
    return JSONResponse(content=dados)

# --- Endpoints para Envio de Comandos (Downlink) ---

@app.post("/comando/{master_id}/{device_id}/ligar", tags=["Comandos"])
def ligar_rele(master_id: str, device_id: str):
    """Envia um comando para LIGAR o relé."""
    payload = lumi_commands.gerar_comando_ligar()
    return mqtt_publisher.publicar_comando(master_id, device_id, payload)

@app.post("/comando/{master_id}/{device_id}/desligar", tags=["Comandos"])
def desligar_rele(master_id: str, device_id: str):
    """Envia um comando para DESLIGAR o relé."""
    payload = lumi_commands.gerar_comando_desligar()
    return mqtt_publisher.publicar_comando(master_id, device_id, payload)

@app.post("/comando/{master_id}/{device_id}/dimerizar", tags=["Comandos"])
def dimerizar_rele(master_id: str, device_id: str, percentual: int = Query(..., ge=0, le=100)):
    """Envia um comando para DIMERIZAR o relé a um certo percentual."""
    try:
        payload = lumi_commands.gerar_comando_dimerizacao(percentual)
        return mqtt_publisher.publicar_comando(master_id, device_id, payload)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=500, detail=str(e))