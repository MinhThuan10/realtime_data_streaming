from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'minhthuan',
    'start_date': datetime(2024, 11, 10, 10, 00)
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
    
    logging.info("Starting stream_data function.")

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()
    timeout = 60  # Timeout after 60 seconds

    while time.time() - curr_time < timeout:
        try:
            res = get_data()
            res = format_data(res)
            logging.info(f"Sending data to Kafka: {res}")
            producer.send('user_created', json.dumps(res).encode('utf-8'))
            time.sleep(5)  # To control the flow and avoid overloading Kafka
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(5)  # Wait before retrying on error
    logging.info("Finished streaming data.")



with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'streaming_data_from_api',
        python_callable=stream_data
    )
