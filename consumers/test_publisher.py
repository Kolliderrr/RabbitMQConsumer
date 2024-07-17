from fastapi import FastAPI, HTTPException
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitMessage

from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import Dict, Any

import logging, json, os
async def handle_message(msg: RabbitMessage):
    print(msg.body)

logger = logging.getLogger()
logger.addHandler(logging.FileHandler('inserver.log'))

class RequestModel(BaseModel):
    name: str
    # action: str


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

consumers = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.connect()
    yield
    await broker.close()

app = FastAPI(lifespan=lifespan)

@app.get('/consumer/')
async def start_consumer():
    consumer = broker.subscriber(RabbitQueue('test_queue_1', durable=True, routing_key='test_key') )
    consumer(handle_message)
    broker.setup()
    await consumer.start()
    
@app.post('/consumer/')
async def start_new_consumer(request: RequestModel):
    config = consumers_config[request.name]
    consumer = broker.subscriber(RabbitQueue(config['queue_name'], durable=True, routing_key=config['routing_key']))
    consumer(handle_message)

    broker.setup_subscriber(consumer)
    await consumer.start()
    consumers[request.name] = consumer
    
if __name__ == '__main__':
    import uvicorn
    
    uvicorn.run(app, host='127.0.0.1', port=8008)