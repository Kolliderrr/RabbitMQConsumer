"""_summary_

Returns:
    _type_: _description_
"""
import logging
from datetime import datetime
from contextlib import asynccontextmanager
import uvicorn
import json
import os

from faststream import BaseMiddleware
from faststream.rabbit import RabbitBroker, RabbitQueue, RabbitMessage

from starlette.applications import Starlette
from starlette.endpoints import WebSocketEndpoint
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket
from starlette.middleware.wsgi import WSGIMiddleware
from typing import Dict, Any, Optional, List

from pydantic import BaseModel, ValidationError

from resources.pg_models import parse_table_model, create_pydantic_model

from flask_main import create_app

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


logger = logging.getLogger(__name__)

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

class WSMessage(BaseModel):
    name: Optional[str]
    action: str

async def handle_message(consumer_state: ConsumerState, msg: RabbitMessage):
    
    try:
        consumer_state.pydantic_model(**msg.decoded_body)
    except Exception as e:
        logger.error(f"Message validation failed: {e}")
        return

    # Increment message count
    consumer_state.increment_message_count()

    logger.info(f"Message received: {msg.body}")

control_sockets = []

class WSEndpointMain(WebSocketEndpoint):
    encoding = "json"
    
    async def on_connect(self, websocket: WebSocket):
        await websocket.accept()
        self.status = 'Connected'
        logger.info(f"Connected: {websocket}")
        
    async def on_receive(self, websocket: WebSocket, data: Dict[str, Any]):
        try:
            result = await compute_message(WSMessage(**data))
            await websocket.send_text('Message accepted!')
        except ValidationError:
            await websocket.send_text('Message not accepted!')

        logger.info("websockets.received")
        
    async def on_disconnect(self, websocket: WebSocket, close_code: int):
        logger.info(f"Disconnected: {websocket}")



@asynccontextmanager
async def lifespan(app: Starlette):
    print('Starting RabbitMQ connection...')
    await broker.connect()
    print('Connection started!')
    yield
    await broker.close()
    print('Starlette is shutting down...')

instance = Starlette(
    routes=(
        WebSocketRoute("/ws", WSEndpointMain, name="ws"),
    ),
    lifespan=lifespan
)

consumers = {}
middlewares = {}

async def compute_message(message: WSMessage):
    if message.action == 'start':
        if not message.name in consumers.keys():
            config = consumers_config[message.name]
            sql_model = parse_table_model(table_name=config['table'])
            pydantic_model = create_pydantic_model(table_name=config['table'])
            consumer_state = ConsumerState(message.name,
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
            logger.info('Consumer %s started', message.name)
            consumers[message.name] = consumer
            middlewares[message.name] = consumer_state
            return f'{message.name} создан'
        else:
            return f'Already exists'
            
            
flask_app = create_app()

instance.mount("/flask", WSGIMiddleware(flask_app)) 
    
if __name__ == '__main__':
    
    uvicorn.run(instance, port=8785, host='localhost')