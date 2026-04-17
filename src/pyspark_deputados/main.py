from pyspark_deputados.connectors.processing_api import DataPipeline

import sys
import os

# Isso adiciona a pasta 'src' ao caminho de busca do Python
sys.path.append(os.path.abspath('./src'))


menager = DataPipeline(url='https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome')
menager.save_file()