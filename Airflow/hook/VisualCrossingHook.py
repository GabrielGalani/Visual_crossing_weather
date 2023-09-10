#Fazendo importção das bibliotecas necessárias
from airflow.providers.http.hooks.http import HttpHook
import requests
import csv
from datetime import datetime, timedelta
from airflow.macros import ds_add
import pendulum
import os
from io import StringIO

#Criando a classe hook que faz a conexão com a api e retorna o csv
class VisualCrossingHook(HttpHook):
    
    #Definindo inicializador passando os parametros
    def __init__(self, cidade, start_date, end_date, conn_id=None):
        self.cidade = cidade
        self.start_date = start_date
        self.end_date = end_date
        self.conn_id = conn_id or 'visualcrossing_api'
        super().__init__(http_conn_id = self.conn_id)
    
    #Definindo os endpoints da api
    def create_url(self):
        cidade = self.cidade
        end_date = self.end_date
        start_date = self.start_date
        key = 'KFK2M79BN53HQANEAXTL9KY89'
        

        url_requisica = f'{self.base_url}/VisualCrossingWebServices/rest/services/timeline/{cidade}/{start_date}/{end_date}?unitGroup=metric&include=days&key={key}&contentType=csv'

        return url_requisica
    
    #Conectando ao endpoint da api
    def connect_to_end(self, url, session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f'URL: {url}')
        return self.run_and_check(session, prep, {})

    #retornando o arquivo csv
    def list_csv(self, url_raw, session):
        list_csv_response = []
        
        response = self.connect_to_end(url_raw, session)
        if response.status_code == 200:
            try:
                csv_data = response.content  
                list_csv_response.append(csv_data)
            except ValueError:
                print("Erro ao decodificar a resposta como CSV")
        
        return list_csv_response
    
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        
        return self.list_csv(url_raw, session)

#teste de execução
#Para testar exportar a variável do airflow e rodar pelo python
if __name__ == "__main__":
    cidades = ['Boston', 'NewYork', 'Chicago', 'Florida', 'Orlando']
    TIMESTAMP_FORMAT = '%Y-%m-%d'
    start_date = datetime.now().date()
    end_date = start_date + timedelta(days=7)
    start_date = start_date.strftime(TIMESTAMP_FORMAT)
    end_date = end_date.strftime(TIMESTAMP_FORMAT)

    for i in cidades:
        visual_crossing_hook = VisualCrossingHook(i, start_date, end_date)
        for row in visual_crossing_hook.run():
            # Convert each row (dictionary) to a CSV string
            csv_string = StringIO()
            csv_writer = csv.DictWriter(csv_string, fieldnames=row.keys())
            csv_writer.writeheader()
            csv_writer.writerow(row)
            csv_output = csv_string.getvalue()
            print(csv_output)