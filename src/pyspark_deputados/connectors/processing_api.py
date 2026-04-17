from pyspark.sql import SparkSession
import requests
import json


class DataPipeline:
    def __init__(self, url:str, spark:None):
        self.url = url
        self.spark = spark
        self.df = None
        self.response = None
        self.error = None
        
    
    def consummer_api(self):
        try:
            res = requests.get(self.url, timeout=30) # Função da biblioteca requests que executa o pedido de acesso na API
                                                     # timeout=30 parametro que diz ao python para aguardar no máximo 30 seg pela resposta.
            res.raise_for_status() # verifica se o site retorno algum erro como "404 Not Found". Se estiver tudo ok ele não faz nada.
            self.response = res.json()  # Aqui pega o  conteúdo do site que geralmente vem em um formato dectexto em JSON, transfomando em formato que python entenda no caso JSON. 
            self.error = "Nenhum" # Como tudo deu certo , ai qualquer registro de erro é limpo de erros anteriores.
            return self.response # A função termina aqui entrgando os dados para quem chamou.
        
        except Exception as e:
            self.error = str(e)         # Aqui preenchemos o erro, se der ruim
            self.response = None
            return None

    def save_file(self):
        resultado = self.url

        if resultado:
            # 2. Abre (ou cria) um arquivo chamado 'saida.json' em modo de escrita ('w')
            file_name = 'dadosabertos.json'
            path = f'/Volumes/workspace/default/landing_zone/{file_name}'
            with open(path, 'w', encoding='utf-8') as arquivo:
                # 3. Salva o dicionário no arquivo
                # indent=4 serve para o arquivo ficar "bonitinho" e legível
                json.dump(resultado, arquivo, indent=4, ensure_ascii=False)
            
            print(f"Arquivo salvo com sucesso como '{file_name}'!")
        else:
            print(f"Não foi possível salvar. Erro: {self.error}")


    def read_df(self,  is_path: bool = False):
        if is_path:
            self.df = self.spark.read.format('delta').load()
        else:
            self.df = self.spark.read.table()

        print(f"Dados carregados. Linhas: {self.df.count()}🔝")
        return self.df
    
    def write_df(self, name_table:str, modo:str='overwrite'):
        """Grava o DataFrame atual como uma tabela Delta."""
        if self.df is None:
            raise ValueError("❌Não há dados carregados para gravar! Use ler_tabela primeiro.")
        
        self.df.write \
            .format("delta") \
            .mode(modo) \
            .saveAsTable(name_table)
        print(f"Tabela {name_table} gravada com sucesso✔️.")

