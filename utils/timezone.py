"""
Utilitário para configuração de fuso horário
Configura o sistema para usar horário de Brasília (Brazil/East)
"""
import os
from datetime import datetime, timezone, timedelta

# Configurar fuso horário para Brasília, Brasil (UTC-3)
BRASILIA_TZ = timezone(timedelta(hours=-3))

def configure_timezone():
    """Configura o fuso horário do sistema para Brasília"""
    os.environ['TZ'] = 'America/Sao_Paulo'
    
def get_brasilia_now():
    """Retorna a data/hora atual no fuso horário de Brasília"""
    return datetime.now(BRASILIA_TZ)

def format_brasilia_datetime(dt_format='%Y-%m-%d %H:%M:%S'):
    """Retorna a data/hora atual de Brasília formatada"""
    return get_brasilia_now().strftime(dt_format)

def get_utc_now():
    """Retorna a data/hora atual em UTC"""
    return datetime.now(timezone.utc)

def convert_to_brasilia(utc_dt):
    """Converte datetime UTC para horário de Brasília"""
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(BRASILIA_TZ)

# Configurar o timezone na importação
configure_timezone()