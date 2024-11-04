from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'minhthuan',
    'start_date': datetime(2024, 10, 20, 20, 00)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    
    res = res.json()
    res = res['results'][0]
    
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}" \
                    f"{str(location['city'])} {location['state']} {location['country']}"


    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data





def stream_data():
    # Lấy dữ liệu
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    res = get_data()
    res = format_data(res)

    producer  = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60:
            logging.info("Finished streaming data after 60 seconds.")
            break
        try:
            res = get_data()
            res = format_data(res)
            logging.info(f"Sending data to Kafka: {res}")
            producer.send('user_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'streaming_data_from_api',
        python_callable=stream_data
    )
