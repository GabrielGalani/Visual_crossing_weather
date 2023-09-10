
#Importando bibliotecas e o hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import BaseOperator, DAG, TaskInstance
import requests
import json
from datetime import datetime, timedelta
from airflow.macros import ds_add
from os.path import join
import pendulum
import sys
sys.path.append('/home/gabrielgalani/Documents/Airflow')
from hook.VisualCrossingHook import VisualCrossingHook
from pathlib import Path
import pandas as pd
from io import StringIO
from airflow.utils.timezone import datetime as timezone_datetime

#Definindo a classe do hook
class VisualCrossingOperator(BaseOperator):
    
    #Definindo os templases para o Airflow
    template_fields = ["cidades", "file_path", "start_time", "end_time"]
   # owner = 'Gabriel'
    
    #definindo o inicializador recebendo os parametros
    def __init__(self, file_path, cidades, start_time, end_time, **kwargs):
        self.cidades = cidades
        self.start_time = pendulum.parse(start_time).astimezone(pendulum.timezone('America/Sao_Paulo'))
        self.end_time = pendulum.parse(end_time).astimezone(pendulum.timezone('America/Sao_Paulo'))
        self.file_path = file_path
        
        #Parametro obrigatório do BaseOperator Airflow
        super().__init__(**kwargs)

    #Função para criar pastas caso ainda não estejam criadas    
    def create_parent_folder(self): 
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)
    
    #Função que une as execuções do hook e funções e retorna o arquivo csv
    def execute(self, context):
        cidades = self.cidades
        start_time = self.start_time 
        end_time = self.end_time
        
        TIMESTAMP_FORMAT = '%Y-%m-%d'
        start_time = start_time.strftime(TIMESTAMP_FORMAT)
        end_time = end_time.strftime(TIMESTAMP_FORMAT)
            
        self.create_parent_folder()

        dataframes = []

        for cidade in cidades:
            visual_crossing_hook = VisualCrossingHook(cidade, start_time, end_time)
            csv_data_list = visual_crossing_hook.run()  
            
            for csv_data in csv_data_list:
                csv_data_str = csv_data.decode('utf-8')
                data = pd.read_csv(StringIO(csv_data_str))
                dataframes.append(data)

        dataframe = pd.concat(dataframes, ignore_index=True)

        dataframe.to_csv(self.file_path, index=False)






#Teste
#Para rodar o teste, exportar a variável do airflow e executar com python
if __name__ == "__main__":
    cidades = ['Boston', 'NewYork', 'Chicago', 'Florida', 'Orlando']
    TIMESTAMP_FORMAT = '%Y-%m-%d'
    start_time = datetime.now().date()
    end_time = start_time + timedelta(days=7)
    start_time = start_time.strftime(TIMESTAMP_FORMAT)
    end_time = end_time.strftime(TIMESTAMP_FORMAT)
    
    BASE_FOLDER = join(
        str(Path("~/Documents").expanduser()),
        "Airflow/datalake/{stage}/Weather/{partition}",
    )
    
    PARTITION_FOLDER_EXTRACT = "extact_date={{ data_interval_start.strftime('%Y-%m-%d') }}"
    
    TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S'
    
    with DAG(
        dag_id='weather_test',
        start_time=pendulum.datetime(2023,5,29, tz="UTC"),
    ) as dag:
        to = VisualCrossingOperator(file_path = join(BASE_FOLDER.format(stage="Bronze", 
                                        partition = PARTITION_FOLDER_EXTRACT), 
                                        ".json"),
                                        cidades = cidades, start_time = start_time, end_time = end_time, task_id='weather_test'
        )
        ti = TaskInstance(task=to)
        to.execute(ti.task_id)
