#Importando bibliotecas necessárias e operadores.
import sys
sys.path.append('/home/gabrielgalani/Documents/Airflow')
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import BaseOperator, DAG, TaskInstance
from Operators.VisualCrossingOperator import VisualCrossingOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
from airflow.utils.dates import days_ago
from airflow.utils import timezone
from pathlib import Path
import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import argparse


#Criando a dag
with DAG(dag_id = "VisualCrossingDag",
         start_date=days_ago(14),
         schedule_interval='@daily'
         ) as dag: 
    
    #Definando variável de base path, onde retorna todo caminho, independente da máquina, até documentos.
    BASE_FOLDER = join(
        str(Path("~/Documents").expanduser()),
        "Airflow/Datalake/{stage}/Weather/{partition}",
    )
    
    #Definindo varáveis que serão administradas pelo Airflow 
    start = datetime.now().date()
    end= start + timedelta(days=7)
    
    PARTITION_FOLDER_EXTRACT = f"extract_date={start.strftime('%Y-%m-%d')}"
    
    TIMESTAMP_FORMAT = '%Y-%m-%d'
    
    query = ['Boston', 'New York', 'Chicago', 'Orlando', 'Sao Paulo', 'Rio de Janeiro', 'Brasilia', 'Belo Horizonte']

    
    #Montando operador que será administrado pelo airflow, onde fará as extrações da camada bronze do datalake
    extract_data = VisualCrossingOperator(file_path = join(BASE_FOLDER.format(stage="Bronze", 
                                        partition = PARTITION_FOLDER_EXTRACT), 
                                        f"weather_{start.strftime('%Y%m%d')}.csv"),
                                        cidades = query, 
                                        start_time = start.strftime(TIMESTAMP_FORMAT), 
                                        end_time = end.strftime(TIMESTAMP_FORMAT),
                                        task_id = "extract_data_cities") 
    ti = TaskInstance(task=extract_data)
    extract_data.execute(ti.task_id)
    
    #Montando operador que será administrado pelo SPARKSUBMITE passando os parametros pelo argsparse para tratamento da camada silver so datalake
    transformation_silver = SparkSubmitOperator(task_id = 'transform_weather_silver',
                                       application='/home/gabrielgalani/Documents/Airflow/Scr/Scripts/Pandas/SilverTransformationWeather.py',
                                       name='transformation_silver_data',
                                       application_args= [
                                           '--path', BASE_FOLDER.format(stage='Bronze', partition = PARTITION_FOLDER_EXTRACT),
                                           '--file-name', f"weather_{start.strftime('%Y%m%d')}.csv",
                                           '--date-extract', start.strftime(TIMESTAMP_FORMAT),
                                           '--path-temp', join(BASE_FOLDER.format(stage='Silver', partition = ''), 'Temperatura'),
                                           '--path-cond', join(BASE_FOLDER.format(stage='Silver', partition = ''), 'Condicoes')
                                       ] )
    
    
    extract_data >> transformation_silver
    
    