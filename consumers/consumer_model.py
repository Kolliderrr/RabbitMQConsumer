"""

"""
import sys
import os

# Добавляем корневой каталог проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


import asyncio
from faststream import FastStream, Logger
from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue, RabbitMessage
from typing import Any, Dict, Optional, Callable
from resources.pg_models import main_models, create_pydantic_model, parse_table_model
from sqlalchemy.exc import IntegrityError, DBAPIError, ResourceClosedError, InvalidRequestError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import insert
from configs.patterns_validation import rabbitmq_url_pattern, RabbitMQUrl, PostgreSQLModel, ValidationError
from typing import Optional
from uuid import uuid4
import json

with open(os.path.join(r'C:\Users\user\RabbitMQ_project','configs/consumers.json'), 'r') as consumer_file:
    consumers_config = json.load(consumer_file)
    
with open(os.path.join(r'C:\Users\user\RabbitMQ_project','configs/db_config.json'), 'r') as db_json:
    db_configs = json.load(db_json)

class RabbitConsumer:
    def __init__(self,
                 queue: str,
                 exchange: str,
                 broker: str,
                 routing_key: str,
                 name: str = 'consumer' + str(uuid4().hex),
                 db: Optional[str] = None,
                 table_name: str = None,
                 params: Optional[Dict[str, Any]] = None) -> None:
        try:
            broker_url = RabbitMQUrl(url=broker).url
            self.broker = RabbitBroker(broker_url)
        except ValidationError as e:
            raise e
        self.exchange = exchange
        self.status = 'не работает'
        self.name = name
        self.table_model = parse_table_model(table_name)
        self.queue = RabbitQueue(queue, routing_key=routing_key, durable=True)
        if params:
            self.validation_model = create_pydantic_model(table_name, params=params)
        else:
            self.validation_model = create_pydantic_model(table_name)
        if db:
            try:
                db_url = PostgreSQLModel(db=db).db
                self.db_engine = create_async_engine(url=str(db_url))
            except ValidationError:
                try:
                    self.db_engine = create_async_engine(url=db_configs[db])
                except KeyError as key_e:
                    raise key_e
            except InvalidRequestError:
                self.db_engine = create_async_engine(url=db_configs[db])
        else:
            self.db_engine = create_async_engine(url=db_configs[consumers_config[self.name]['db']])
        self.app = FastStream(self.broker)
    
    async def process_message(self, msg: RabbitMessage):
        self.status = f"получил сообщение: {msg}"
        try:
            self.validation_model.model_validate(msg)
            async with self.db_engine.begin() as conn:
                try:
                    stmt = insert(self.table_model).values(**msg)
                    await conn.execute(stmt)
                except (IntegrityError, DBAPIError):
                    primary_key = self.table_model.primary_key
                    query = f"""
                    DELETE FROM {self.table_model.name}
                    WHERE {primary_key} = '{msg.body[primary_key]}'
                    """
                    stmt = insert(self.table_model).values(**msg)
                    try:
                        await conn.execute(query)
                        await conn.execute(stmt)
                    except Exception as e1:
                        self.app.logger.log(msg=e1)
        except ValidationError as e:
            self.app.logger.log(msg=e)
            self.app.logger.log(msg=msg.body)
        
    async def get_status(self):
        return f"Потребитель {self.name} сейчас {self.status}"
        
    async def run(self):
        @self.broker.subscriber(self.queue)
        async def handler(msg):
            await self.process_message(msg)
            
        while True:
            try:
                self.status = 'начал работу'
                await self.app.run()
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    await self.app.stop()
                    break
                elif isinstance(e, ValidationError):
                    self.status = 'работает с ошибками'
                    pass
        
    async def stop(self):
        await self.app.stop()
        
