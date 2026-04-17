from connectors.processing_api import DataPipeline
from etl.etl_process import executar_etl

menager = DataPipeline(url='https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome')
menager.save_file()
executar_etl(menager)