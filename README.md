# Projeto de Extração e Transformação de Dados Climáticos com Airflow, PySpark e Power BI

## Descrição do projeto:  
Este projeto tem como objetivo a extração de dados climáticos da API do VisualCrossing (https://www.visualcrossing.com/weather-api) e a transformação desses dados em um Data Lake organizado em estrutura de medalhas. A orquestração do fluxo de dados é realizada utilizando o Apache Airflow e o PySpark.

##Fluxo de Dados
O fluxo de dados é dividido em várias etapas:

Extração de Dados: Utilizando a biblioteca Python Requests, os dados climáticos são extraídos da API do VisualCrossing diariamente.

Transformação de Dados: Os dados extraídos são processados utilizando as bibliotecas Python Pandas, Numpy, csv, Timedelta, pendulum, sys, StringIO, e argparse para estruturar os dados em formato de medalhas.

Orquestração com Airflow: O Apache Airflow é usado para orquestrar o fluxo de dados, definindo as dependências entre as tarefas, agendando a execução diária e gerenciando erros.

Processamento com PySpark: A biblioteca PySpark é utilizada para realizar transformações de alto desempenho nos dados climáticos, garantindo eficiência e escalabilidade.

Entrega de Relatórios no Power BI: Os dados processados são entregues no Power BI, onde são atualizados diariamente e disponibilizados online através de um link público para acesso fácil e conveniente.


| Utilizados | Softwares |
| ---------- | --------- |
| PySpark | Apache Airflow |
|Python| Power Bi |
| Dax & M | |

| Airflow | Bibliotecas | 
| -------| ---------- |
| HttpHook |  |
| BaseOperator |  |
| datetime |  |
| DAG |  |
| TaskInstance | | 
| ds_add | |

| Python | Bibliotecas | 
| -------| ---------- |
| Pandas |  |
| Numpy |  |
| datetime |  |
| Requests |  |
| csv | | 
| Timedelta | |
| Pendulum | | 
| Sys | | 
| StringIO | | 
| Argparse | |

| Spark | Bibliotecas | 
| -------| ---------- |
| SparkSubmit |  |
| Functions |  |
| Session |  |


##Entrega do Relatório 
O relatório final é entregue através do Power BI, onde os dados climáticos são visualizados e atualizados diariamente. Você pode acessar o relatório online através do seguinte link: https://app.powerbi.com/view?r=eyJrIjoiYjdjZjczZDgtOGNkMi00ZDQ0LWIwMWYtZTk3ZDdlODJiNTdlIiwidCI6IjJhMzIyY2YxLWM5NTktNGNhYy04ZDhmLWQ3OTY4NGMwZmRjNSJ9
