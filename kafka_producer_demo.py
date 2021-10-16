import logging
import time
import requests
import json
from configparser import ConfigParser
from kafka import KafkaProducer

from app_log import init_log


log = init_log(log_name="kafka-producer-demo", level=logging.INFO)

if __name__ == '__main__':
    log.info("Load configuration")
    config = ConfigParser()
    config.read('config.properties')

    log.info('Kafka Producer Application Started ...')
    kafka_producer_obj = KafkaProducer(
        bootstrap_servers=config['kafka']['bootstrap-servers'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    try:
        stream_api_response = requests.get(config['default']['stream-url'],
                                           stream=True,
                                           proxies={'http': '',
                                                    'https': '',
                                                    'ftp': ''})
        if stream_api_response.status_code == 200:
            for api_response_message in stream_api_response.iter_lines():
                log.info(f"Message received: {api_response_message}")

                api_response_message = json.loads(api_response_message)

                log.info(f"send to kafka topic: {config['kafka']['topic']} - "
                         f"msg: {api_response_message}")

                kafka_producer_obj.send(config['kafka']['topic'],
                                        api_response_message)
                time.sleep(1)
    except Exception as ex:
        log.error(ex)
        log.error('Connection to meetup stream api could not etablished')

    log.info('End.')
