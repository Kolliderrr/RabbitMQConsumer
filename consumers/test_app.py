import sys
import os

# Добавляем корневой каталог проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import asynccontextmanager
from test_publisher import RabbitConsumer, FastStream
from fastapi import FastAPI, Depends, HTTPException, status, Header, Request
import asyncio
from typing import Dict, List, Union, Optional, Any
import json, logging
from configs import APIMessage, APIResponse


logger = logging.getLogger()
logger.addHandler(logging.FileHandler('inserver.log'))


# Функция для чтения конфигурационных файлов
def load_config(file_path: str) -> Dict[str, Any]:
    """_summary_

    Args:
        file_path (str): _description_

    Returns:
        Dict[str, Any]: _description_
    """
    with open(file_path, 'r') as file:
        return json.load(file)

consumers_config = load_config(os.path.join(r'C:\Users\user\RabbitMQ_project','configs/consumers.json'))
db_config = load_config(os.path.join(r'C:\Users\user\RabbitMQ_project','configs/db_config.json'))
consumers = {}
tasks = {}

async def consumer(name: str) -> RabbitConsumer:
    """_summary_

    Args:
        name (str): _description_

    Returns:
        RabbitConsumer: _description_
    """
    config = consumers_config[name]
    consumer_params: Dict[str, Any] = {
        'broker':config['broker_url'],
        'exchange':config['exchange_name'],
        'queue':config['queue_name'],
        'routing_key':config['routing_key'],
        'db':config['db'],
        'table_name':config['table'],
        'name':name
    }
    consumer_instance = RabbitConsumer(
        exchange=consumer_params['exchange'], 
    )
    try:
        await consumer_instance.run()
        return consumer_instance
    except Exception as e:
        consumer_instance.app.logger.log(e)
        await consumer_instance.stop()