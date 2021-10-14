import time
import requests
import json
from kafka import KafkaProducer
from configparser import ConfigParser

config = ConfigParser()
config.read('config.properties')

if __name__ == '__main__':
    print('Kafka Producer Application Started ...')

    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=config['kafka']['bootstrap-servers'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    print('Printing before while loop start ...')
    # while True:
    try:
        stream_api_response = requests.get(config['default']['stream-url'],
                                           stream=True,
                                           proxies={'http': '',
                                                    'https': '',
                                                    'ftp': ''})
        if stream_api_response.status_code == 200:
            for api_response_message in stream_api_response.iter_lines():
                print("Message received")
                print(api_response_message)
                print(type(api_response_message))

                api_response_message = json.loads(api_response_message)
                print('Message to be sent:')
                print(api_response_message)
                print(type(api_response_message))

                kafka_producer_obj.send(config['kafka']['topic'],
                                        api_response_message)
                time.sleep(1)
    except Exception as ex:
        print(ex)
        print('Connection to meetup stream api could not etablished')

    print('Printing after while loop complete.')
