from datetime import datetime
import pymsteams

from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.python_operator import PythonOperator

with DAG(
    'airflow_workshop',
    description='DAG exemplo para o workshop de Apache AirFlow',
    schedule_interval="@daily",
    start_date=datetime(2022,11,23),
    tags=['SemantixDataEngineers'],
) as dag:
    
    def response_api(response, **context):
        task_instance = context['task_instance']
        task_instance.xcom_push(key='return_value', value=response.json())
        print(response.json())
        return True
    
    def send_card(ti):
        xcom_return = ti.xcom_pull(key='return_value', task_ids='get_api_data')
        countries = []
        for register in xcom_return:
            countries.append(register['nome']['abreviado'])
        msteams_card = pymsteams.connectorcard('https://opengalaxyio.webhook.office.com/webhookb2/3e1bea36-4d9c-4d10-b878-670caa9dd7c4@65ce7fd8-a5b8-4103-997e-da068cd85fd5/IncomingWebhook/f53a6aaa5cbc426287812b7931bc4538/91ee7e5e-5040-4322-86b8-f681a2c619a3')
        msteams_card.title("Exemplo de cartão - AirFlow Workshop")
        msteams_card.text(f"{countries}")
        msteams_card.color("#08A0FF")
        msteams_card.send()
        
    get_api_data = HttpSensor(
        task_id='get_api_data',
        method='GET',
        http_conn_id='http_ibge_treinamento',
        endpoint='api/v1/paises/',
        response_check=response_api
    )
    
    send_msteams_card = PythonOperator(
        task_id='send_msteams_card',
        python_callable=send_card
    )
    
    get_api_data >> send_msteams_card