import aio_pika
from aio_pika.channel import Channel
from aio_pika.queue import Queue
from aio_pika.robust_connection import RobustConnection
from pg_models import main_models
from message_model import Message
from queue_methods import branch_task, db_write
import logging
import ujson as json
import asyncio
from typing import Dict
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import \
    create_async_engine, AsyncEngine
from sqlalchemy.schema import Table
import sys
import os

NAME_QUEUE = os.getenv('NAME_QUEUE')
AMQP_URL = os.getenv('AMQP_URL')
PORSGRES_URL = os.getenv('POSTGRES_URL')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Меняем уровень на DEBUG, чтобы видеть больше логов

handler = logging.FileHandler(f"{__name__}.log", mode='a')
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")

# Настройка логгера

handler.setLevel(logging.DEBUG)  # Логи пишутся в файл

# Хендлер для вывода в консоль
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)  # Логи выводятся в консоль контейнера

# Формат логов
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Добавление хендлеров в логгер
logger.addHandler(handler)
logger.addHandler(console_handler)


async def read_message(
    message: aio_pika.Message, 
    table_models: Dict[str, Table], 
    engine: AsyncEngine) -> None:
    """Основной метод чтения сообщения

    Args:
        message (aio_pika.Message): сам экземпляр сообщения
        table_models (Dict[str, Table]): список моделей таблиц БД
        engine (AsyncEngine): экземпляр асинхронного движка для подключения к БД
    """
    body = message.body.decode()
    json_data = json.loads(body)
    try:
        Message(**json_data)
    except ValidationError as e:
        logger.error(e)
    table_name = await branch_task(json_data['table_name'])
    if table_name:
        try:
            await db_write(json_data['payload'], table_models[table_name], engine, table_models)
            logger.info(f'{json_data['name']} - {json_data['accepted_timestamp']}')
        except Exception as e:
            logger.error('Произошла ошибка {}. \n Сообщение \n {}'.format(e, json_data['payload']))
            raise

async def connect_to_queue(channel: RobustConnection) -> Queue:
    """Подключение к очереди

    Args:
        channel (RobustConnection): Подключение к RabbitMQ

    Returns:
        Queue: Очередь
    """
    queue: Queue = await channel.declare_queue(NAME_QUEUE, durable=True, auto_delete=False)
    print(f'Connected to queue {NAME_QUEUE}')
    return queue
    
    
async def main() -> None:
    """Основной цикл работы
    """
    # connection = await aio_pika.connect_robust('amqp://admin:Emperor011@192.168.20.122:5672')
    connection: RobustConnection = await aio_pika.\
        connect_robust(AMQP_URL)
    channel: Channel = await connection.channel()
    await channel.set_qos(prefetch_count=2)

    engine: AsyncEngine = create_async_engine\
        (PORSGRES_URL, future=True, isolation_level="AUTOCOMMIT")
    table_models: Dict[str, Table] = main_models()
    queue: Queue = await connect_to_queue(channel)
    while True:
        try:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    try:
                        await read_message(message, table_models, engine)
                        await message.ack()
                        logger.info(f'Записано сообщение')
                    except Exception as e:
                        logger.error(e)
                        logger.info(message.body.decode())
                        await message.reject(requeue=True)
                        raise      
        except KeyboardInterrupt:
            print('Exiting...')
            await channel.close()
            await connection.close()
            await engine.dispose()
            break
        

        
if __name__ == '__main__':
    asyncio.run(main())
