import base64
import time
from struct import unpack
from datetime import datetime

def decode_lumi(lumi_payload):
    dados = {}
# Decodifica o payload de base64 para bytes e converte para string hexadecimal
    decode01 = base64.b64decode(lumi_payload).hex()
    # Extrai o comando
    cmd = int(decode01[:2], 16)
    print("Comando:", cmd)
    dados["comando"] = cmd
    # Lê o estado do relé
    status_rele = int(decode01[34:36], 16)
    print("Estado do Relé:", status_rele)
    dados["estado_rele"] = bool(status_rele)
    # Lê o valor do dimmer
    dimmer_rele = int(decode01[74:76], 16)
    print("Dimmer:", dimmer_rele)
    dados["dimmer_rele"] = dimmer_rele
    # Lê o valor da tensão do relé
    tensao_rele_hex = decode01[36:40]
    tensao_rele = int(tensao_rele_hex[2:4] + tensao_rele_hex[:2], 16)
    print("Tensão:", tensao_rele / 100.0) # Ajustar a divisão conforme a unidade esperada
    dados["tensao_rele"] = tensao_rele / 100.0
    # Lê o valor da corrente do relé
    corrente_rele_hex = decode01[40:44]
    corrente_rele = int(corrente_rele_hex[2:4] + corrente_rele_hex[:2],
    16)
    print("Corrente:", corrente_rele / 100.0) # Ajustar a divisão conforme a unidade esperada
    dados["corrente_rele"] = corrente_rele / 100.0
    # Lê a potência ativa
    real_potencia_hex = decode01[44:48]
    real_potencia = int(real_potencia_hex[2:4] + real_potencia_hex[:2],
    16)
    print("Potência Ativa:", real_potencia / 10.0) # Ajustar a divisão conforme a unidade esperada
    dados["potencia_ativa"] = real_potencia / 10.0
    # Lê o fator de potência
    fp_potencia = (decode01[52:56])
    fp_potencia = int(fp_potencia[2:4] + fp_potencia[:2], 16)
    print("Fator de Potência:", fp_potencia)
    dados["fator_potencia"] = fp_potencia
    # Lê a potência aparente
    if fp_potencia == 100:
        aparente_potencia_hex = real_potencia
        print("Potência Aparente:", aparente_potencia_hex/10.0) #Ajustar a divisão conforme a unidade esperada
        dados["potencia_aparente"] = aparente_potencia_hex/10.0
    else:
        aparente_potencia_hex = fp_potencia
        print("Potência Aparente:", aparente_potencia_hex )
        dados["potencia_aparente"] = aparente_potencia_hex

    # Tempo Lâmpada Ligada
    tempo_ligada_hex = decode01[76:84]
    tempo_ligada = int(tempo_ligada_hex[6:8] + tempo_ligada_hex[4:6] +
    tempo_ligada_hex[2:4] + tempo_ligada_hex[:2], 16)
    print("Tempo Lâmpada Ligada:", tempo_ligada)
    dados["tempo_ligada"] = tempo_ligada
    #Comsumo de Energia
    consumo_hex = decode01[56:64]
    consumo = int(consumo_hex[6:8] + consumo_hex[4:6] + consumo_hex[2:4]
    + consumo_hex[:2], 16)
    print("Consumo de Energia:", consumo)
    dados["consumo_energia"] = consumo
    #versao do firmware
    versao_hex = decode01[64:72]
    versao = int(versao_hex[6:8] + versao_hex[4:6] + versao_hex[2:4] +
    versao_hex[:2])
    print("Versão do Firmware", versao)
    dados["versao_firmware"] = versao
    #LUX
    lux = decode01[8:12]
    lux = int(lux[2:4] + lux[:2], 16)
    print("LUX:", lux)
    dados["lux"] = lux
    #modo foto celula
    modo_foto_celula = decode01[12:14]
    print("Modo Foto Celula:", bool(int(modo_foto_celula)))
    dados["modo_foto_celula"] = bool(int(modo_foto_celula))
    #Timestamp
    timestamp_hex = decode01[84:88]
    timestamp_hex_2 = decode01[4:8]
    timestamp = int(timestamp_hex[2:4] + timestamp_hex[:2] +
    timestamp_hex_2[2:4] + timestamp_hex_2[:2], 16)
    print("Timestamp:", timestamp)
    dados["timestamp"] = timestamp
    # Convert timestamp to datetime
    timestamp_datetime = datetime.fromtimestamp(timestamp)
    print("Data e Hora:", timestamp_datetime)
    dados["data_hora"] = str(timestamp_datetime)
    # Lê o RSSI
    rssi_lumi = int(decode01[88:90], 16)
    print("RSSI:", rssi_lumi-256)
    dados["rssi"] = rssi_lumi-256
    # Lê latitude
    lat_hex = decode01[16:24]
    byte_obj = bytes.fromhex(lat_hex)
    lat_f = unpack('<f', byte_obj)[0]
    print("Latitude:", lat_f)
    dados["latitude"] = lat_f
    #Lê longitude
    long_hex = decode01[24:32]
    byte_obj = bytes.fromhex(long_hex)
    long_f = unpack('<f', byte_obj)[0]
    print("Longitude:", long_f)
    dados["longitude"] = long_f
    time.sleep(0.1)
    
    return dados

# Teste da função com um exemplo de payload em base64
lumi_payload ="DgD7NgcAAQBeigDBj9cLwrgBHFcwADgEOARkALAnAAAAAQEAAGQ63QAAzma4"
print("Payload original:", lumi_payload)
recebe = decode_lumi(lumi_payload)
print("--------------------------------")
print("Dados decodificados:", recebe)