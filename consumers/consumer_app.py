import sys
import os

# Добавляем корневой каталог проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import json
import os
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.wsgi import WSGIMiddleware
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitMessage
from faststream.rabbit.fastapi import Logger

import asyncio
from contextlib import asynccontextmanager
from pydantic import BaseModel, ValidationError
from typing import Dict, Any
from consumers import RequestModel
from resources.pg_models import parse_table_model, create_pydantic_model

from flask_main import create_app


# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Установите уровень логгирования на INFO


# Обработчик для записи логов в файл
file_handler = logging.FileHandler('new.log')
file_handler.setLevel(logging.INFO)

consumers = {}
middlewares = {}  # Словарь для хранения экземпляров MyMiddleware

class ConsumerState:
    def __init__(self, name, queue_name, routing_key, sql_model, pydantic_model):
        self.name = name
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.is_running = False
        self.start_time = None
        self.message_count = 0
        self.sql_model = sql_model
        self.pydantic_model = pydantic_model

    def start(self):
        self.is_running = True
        self.start_time = datetime.now()

    def stop(self):
        self.is_running = False

    def increment_message_count(self):
        self.message_count += 1

    def get_state(self):
        return {
            "is_running": self.is_running,
            "start_time": self.start_time,
            "message_count": self.message_count
        }

async def handle_message(consumer_state: ConsumerState, msg: RabbitMessage):
    # Validate message
    # print(msg.body)
    # print(msg.decoded_body)
    # message_body = json.load(msg.decoded_body)
    try:
        consumer_state.pydantic_model(**msg.decoded_body)
    except Exception as e:
        logger.error(f"Message validation failed: {e}")
        return

    # Increment message count
    consumer_state.increment_message_count()

    logger.info(f"Message received: {msg.body}")

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

broker = RabbitBroker('amqp://admin:Emperor011@192.168.20.243:5672')


@asynccontextmanager
async def lifespan(app: FastAPI):
    print('Starting RabbitMQ connection...')
    await broker.connect()
    print('Connection started!')
    yield
    await broker.close()
    print('FastAPI is shutting down...')

app = FastAPI(lifespan=lifespan)

@app.post('/consumer/', include_in_schema=True)
async def start_new_consumer(request: RequestModel):
    try:
        if request.action == 'start':
            try:
                if not request.params:
                    config = consumers_config[request.name]
                else:
                    config = {
                        'queue_name': request.params.queue_name,
                        'routing_key': request.params.routing_key,
                        'db': request.params.db,
                        'table': request.params.table_name,
                    }
                sql_model = parse_table_model(table_name=config['table'])
                pydantic_model = create_pydantic_model(table_name=config['table'])
                consumer_state = ConsumerState(request.name,
                                            config['queue_name'],
                                            config['routing_key'],
                                            sql_model=sql_model,
                                            pydantic_model=pydantic_model)
                
                
                async def custom_handler(msg: RabbitMessage):
                    await handle_message(consumer_state, msg)
                    
                consumer = broker.subscriber(
                    RabbitQueue(config['queue_name'], durable=True, routing_key=config['routing_key'])
                )
                
                consumer(custom_handler)
                consumer_state.start()
                broker.setup_subscriber(consumer)
                await consumer.start()
                logger.info('Consumer %s started', request.name)
                consumers[request.name] = consumer
                middlewares[request.name] = consumer_state
            except Exception as e:
                logger.error(e)
                print(e)
                return HTTPException(status_code=422, detail=str(e))
                
        elif request.action == 'stop':
            consumer = consumers.pop(request.name)
            consumer_state: ConsumerState = middlewares[request.name]
            await consumer.close()
            consumer_state.stop()
            logger.info('Consumer %s stopped', request.name)
        elif request.action == 'check':
            
            try:
                consumer_state: ConsumerState = middlewares[request.name]
                return consumer_state.get_state()
            except Exception:
                return HTTPException(status_code=404, detail="Consumer not found")
    except (KeyboardInterrupt, asyncio.CancelledError):
        for consumer in consumers.values:
            await consumer.close()
        
flask_app = create_app()

app.mount("/flask", WSGIMiddleware(flask_app))

if __name__ == '__main__':
    import uvicorn
    try:
        uvicorn.run(app, host='127.0.0.1', port=8008, log_level="error")
    except KeyboardInterrupt:
        print('Shutting down server...')