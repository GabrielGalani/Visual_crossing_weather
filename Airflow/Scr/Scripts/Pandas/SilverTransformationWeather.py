#Importando bibliotecas
import pandas as pd
from datetime import datetime
from os.path import join
from pathlib import Path
import argparse


#Função que fará a leitura do csv em dataset
def read_csv(file_path, file_name):

    file_path = join(file_path, file_name)
    dataset = pd.read_csv(file_path)
    return dataset
  
#Função para criar pastas caso não exista 
def create_parent_folder(file_path): 
    (Path(file_path).parent).mkdir(parents=True, exist_ok=True)
      
#Função que recebe o dataset para fazer as transformações
def transformation(dataset, date_extract, path_temp, path_cond):
    column_list_temp = [
    'name',
    'datetime',
    'tempmin',
    'temp',
    'feelslikemax',
    'feelslikemin',
    'feelslike',
    'dew'
    ]

    column_list_temp_condicoes = [
        'name',
        'datetime',
        'humidity',
        'precip',
        'snow',
        'windspeed',
        'cloudcover',
        'uvindex',
        'sunrise',
        'sunset',
        'conditions',
        'description',
        'icon'
    ]

    dataset_temp = dataset[column_list_temp]
    datasaet_condicoes = dataset[column_list_temp_condicoes]
    
    #Exportando o dataset para camada silver
    file = f'extract_date={date_extract}.csv'
    folder = join(path_temp, f'process_date={date_extract}/')
    (Path(folder + file).parent).mkdir(parents=True, exist_ok=True)
    dataset_temp.to_csv(folder+file)

    #Exportando o dataset para camada silver
    file = f'extract_date={date_extract}.csv'
    folder = join(path_cond, f'process_date{date_extract}/')
    (Path(folder + file).parent).mkdir(parents=True, exist_ok=True)
    datasaet_condicoes.to_csv(folder+file)

def run(path, file_name, file_date_extract, path_temp, path_cond):
    
    dataset_csv = read_csv(path, file_name)
    transformation(dataset_csv, file_date_extract, path_temp, path_cond)
    
        



#Teste
#Para rodar o teste tem que exportar a variável do Airflow, bem como a do Spark e ter o SparkSubmite. 
#Para rodar tem que passar os argumentos do argsparse diretamente no terminal.
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(
        description='Spark Weather Transformation'
    )
    
    parser.add_argument('--path', required=True)
    parser.add_argument('--file-name', required=True)
    parser.add_argument('--date-extract', required=True)
    parser.add_argument('--path-temp', required=True)
    parser.add_argument('--path-cond', required=True)
    
    args = parser.parse_args()
    
    run(args.path, args.file_name, args.date_extract, args.path_temp, args.path_cond)

