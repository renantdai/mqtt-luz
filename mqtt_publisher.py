import paho.mqtt.publish as publish
import json
import os

"""
Módulo para gerenciar a publicação de mensagens em um broker MQTT.
"""

MQTT_HOSTNAME = os.getenv('MQTT_HOST', 'mqtt.kntsys.com.br')
MQTT_PORT = int(os.getenv('MQTT_PORT', 1880))
MQTT_AUTH = {
    'username': os.getenv('MQTT_USER', 'imbe'),
    'password': os.getenv('MQTT_PASS', 'prefeitura')
}
LOCALIDADE = os.getenv('MQTT_LOCALIDADE', 'imbe')


def publicar_comando(master_id: str, device_id: str, payload_b64: str):
    """
    Publica uma mensagem JSON formatada no tópico de downlink do dispositivo.
    """
    # Tópico de downlink: /{LOCALIDADE)/v1/downlink/masterID
    topic = f"/{LOCALIDADE}/v1/downlink/{master_id}"
    
    # Payload JSON: {"device": "ID DO DEVICE", "payload": "COMANDO EM BASE64"}
    message = {
        "device": device_id,
        "payload": payload_b64
    }
    
    try:
        publish.single(
            topic=topic,
            payload=json.dumps(message),
            hostname=MQTT_HOSTNAME,
            port=MQTT_PORT,
            auth=MQTT_AUTH
        )
        print(f"Comando enviado com sucesso para o tópico: {topic}")
        return {"status": "sucesso", "topic": topic, "message": message}
    except Exception as e:
        print(f"Falha ao enviar comando para o tópico {topic}: {e}")
        raise ConnectionError(f"Não foi possível publicar no MQTT: {e}")