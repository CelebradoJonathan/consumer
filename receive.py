import pika
import configparser
import logging.config
import yaml
import os
import time


def setup_logging(
        default_path='logging.yaml',
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """Setup logging configuration from a yaml file.
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    else:
        logging.basicConfig(level=default_level)


def receive_metadata(hostname, queue, secs):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=hostname))
    channel = connection.channel()

    channel.queue_declare(queue=queue)
    while True:
        method, properties, body = channel.basic_get(queue=queue, auto_ack=True)
        if method:
            logger.info("Received :"+str(body))
        else:
            time.sleep(int(secs))


def main():
    config = configparser.ConfigParser()
    conf_dir = os.path.join(os.path.dirname(__file__), 'conf.ini')
    config.read(conf_dir)
    hostname = config['args']['hostname']
    queue = config['args']['queue']
    secs = config['args']['secs']

    receive_metadata(hostname, queue, secs)


if __name__ == '__main__':
    setup_logging()
    logger = logging.getLogger(__name__)
    main()