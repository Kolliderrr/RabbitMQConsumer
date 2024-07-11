import sys
import os

# Добавляем корневой каталог проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import asynccontextmanager
from consumers.consumer_model import RabbitConsumer, FastStream
from fastapi import FastAPI, Depends, HTTPException, status, Header, Request
import asyncio
from typing import Dict, List, Union, Optional, Any
import json, logging
from configs.patterns_validation import APIMessage, APIResponse


logger = logging.getLogger()
logger.addHandler(logging.FileHandler('inserver.log'))

# Функция для чтения конфигурационных файлов
def load_config(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as file:
        return json.load(file)

consumers_config = load_config(os.path.join(r'C:\Users\user\RabbitMQ_project','configs/consumers.json'))
db_config = load_config(os.path.join(r'C:\Users\user\RabbitMQ_project','configs/db_config.json'))
consumers = {}
tasks = {}


# Основная функция для создания и запуска потребителей
async def consumer(name: str) -> RabbitConsumer:
    config = consumers_config[name]
    consumer_instance = RabbitConsumer(
        broker=config['broker_url'],
        exchange=config['exchange_name'],
        queue=config['queue_name'],
        routing_key=config['routing_key'],
        db=config['db'],
        table_name=config['table'],
        name=name
    )
    try:
        await consumer_instance.run()
        return consumer_instance
    except Exception as e:
        consumer_instance.app.logger.log(e)
        await consumer_instance.stop()



@asynccontextmanager
async def startup_event(app: FastAPI):
    loop_events['loop'] = asyncio.set_event_loop(asyncio.BaseEventLoop())
    asyncio.set_event_loop(loop_events['loop'])
    logger.info(f'{loop_events['loop'].is_running()}')
    loop_events['loop'].run_forever()
    yield
    
loop_events = {}

app = FastAPI(docs_url="/documentation", redoc_url=None, lifespan=startup_event)


@app.post("/consumer_manage/", response_model=APIResponse)
async def consumer_manage(message: APIMessage):
    event_loop = loop_events['loop']
    if message.action == 'start':
        if message.name in consumers:
            raise HTTPException(status_code=400, detail="Consumer already running")
        else:
            try:
                task = event_loop.create_task(consumer(message.name))
                consumers[message.name] = task
                return APIResponse(status="success", message=f"Consumer {message.name} started")
            except Exception as e:
                return HTTPException(status_code=500, detail=f"Failed to start consumer {message.name}: {e}")

    elif message.action == 'stop':
        if message.name not in consumers:
            raise HTTPException(status_code=400, detail="Consumer not running")
        
        task = consumers.pop(message.name)
        try:
            await task
            await task.stop()
            return APIResponse(status="success", message=f"Consumer {message.name} stopped")
        except asyncio.CancelledError:
            return APIResponse(status="success", message=f"Consumer {message.name} stopped")
        except Exception as e:
            return HTTPException(status_code=500, detail=f"Failed to stop consumer {message.name}: {e}")

    elif message.action == 'status':
        if message.name not in consumers:
            raise HTTPException(status_code=400, detail="Consumer not running")
        
        task = consumers[message.name]
        try:
            status = await task.get_status()
            return APIResponse(status="success", message=status)
        except Exception as e:
            return HTTPException(status_code=500, detail=f"Failed to get status of consumer {message.name}: {e}")

    else:
        raise HTTPException(status_code=400, detail="Invalid action")



