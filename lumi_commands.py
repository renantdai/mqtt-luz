import base64
from pydantic import BaseModel, Field
from typing import List

"""
Este módulo é responsável por gerar os payloads de comando (downlink)
para os relés inteligentes Lumi Net, codificados em Base64, conforme a documentação.
"""

def gerar_comando_ligar() -> str:
    """Gera o payload para ligar o relé."""
    # O comando "Ligar" corresponde a dimerizar em 100%.
    # "BgEBZA==" (Base64) -> 06 01 64 (Hex) -> onde 64 (hex) = 100 (decimal).
    return "BgEBZA"

def gerar_comando_desligar() -> str:
    """Gera o payload para desligar o relé."""
    # "BgAAAA==" (Base64) -> 06 00 00 00 (Hex).
    return "BgAAAA=="

def gerar_comando_dimerizacao(percentual: int) -> str:
    """
    Gera o payload para dimerizar o relé a um percentual específico.
    """
    if not 0 <= percentual <= 100:
        raise ValueError("O percentual de dimerização deve estar entre 0 e 100.")

    # A documentação indica o formato "06 01 01 50" para 80% (0x50).
    # O payload "BgEBUA==" decodifica para 0x06 0x01 0x01 0x50.
    hex_string = f"060101{percentual:02x}"
    byte_array = bytes.fromhex(hex_string)
    return base64.b64encode(byte_array).decode('utf-8')

def gerar_comando_ativar_fotocelula() -> str:
    """Gera o payload para ativar o modo fotocélula."""
    return "EwAB=="

def gerar_comando_desativar_fotocelula() -> str:
    """Gera o payload para desativar o modo fotocélula."""
    return "EwAA=="

# -- Os demais comandos podem ser adicionados seguindo a mesma lógica --