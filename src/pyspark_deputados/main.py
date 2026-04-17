from connectors.processing_api import DataPipeline
from connectors.etl.etl_process import executar_etl
import sys
import os

# Adiciona o diretório onde a main.py está ao path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

menager = DataPipeline(url='https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome')
menager.save_file()
executar_etl(menager)