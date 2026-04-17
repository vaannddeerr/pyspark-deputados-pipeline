from pyspark_deputados.connectors.processing_api import DataPipeline

import sys
import os

# Pega o caminho do diretório onde o main.py está localizado
current_dir = os.path.dirname(os.path.abspath(__file__))
# Adiciona esse caminho ao sistema para que ele enxergue as pastas vizinhas
sys.path.append(current_dir)


menager = DataPipeline(url='https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome')
menager.save_file()