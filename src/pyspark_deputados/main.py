from connectors.processing_api import DataPipeline
from connectors.etl.etl_process import executar_etl


menager = DataPipeline(url='https://dadosabertos.camara.leg.br/api/v2/deputados?ordem=ASC&ordenarPor=nome')
menager.save_file()
df = executar_etl(menager)
menager.write_df(df)