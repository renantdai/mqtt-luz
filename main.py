from fastapi import FastAPI, Query
from kafka import KafkaConsumer
import paho.mqtt.publish as publish
from typing import List
from pydantic import BaseModel
import json
import base64
import uuid

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API funcionando!"}


@app.get("/mensagens")
def ler_mensagens(qtd: int = Query(1, ge=1, le=100)):
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
        hostname="mosquitto",
        port=1883,
        auth={
            'username': 'imbe',
            'password': 'prefeitura'
        }
    )
    return {"status": "mensagem enviada"}
