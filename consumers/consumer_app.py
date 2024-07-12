"""_summary_

    Raises:
        HTTPException: _description_
        HTTPException: _description_
        HTTPException: _description_
        HTTPException: _description_
        http_exc: _description_
        HTTPException: _description_

    Returns:
        _type_: _description_
        
"""


import sys
import os

# Добавляем корневой каталог проекта в sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import asynccontextmanager
from consumers import RabbitConsumer, FastStream
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


# Основная функция для создания и запуска потребителей
async def consumer(name: str) -> RabbitConsumer:
    """_summary_

    Args:
        name (str): _description_

    Returns:
        RabbitConsumer: _description_
    """
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

async def get_event_loop():
    """_summary_

    Returns:
        _type_: _description_
    """
    return asyncio.get_running_loop()

app = FastAPI(docs_url="/documentation", redoc_url=None)


@app.post("/consumer_manage/", response_model=APIResponse)
async def consumer_manage(message: APIMessage, event_loop: asyncio.AbstractEventLoop = Depends(get_event_loop)):
    """_summary_

    Args:
        message (APIMessage): _description_
        event_loop (asyncio.AbstractEventLoop, optional): _description_. Defaults to Depends(get_event_loop).

    Raises:
        HTTPException: _description_
        HTTPException: _description_
        HTTPException: _description_
        HTTPException: _description_
        http_exc: _description_
        HTTPException: _description_

    Returns:
        _type_: _description_
    """
    try:
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
                consumer_instance = await task
                await consumer_instance.stop()
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
                consumer_instance = await task
                status = await consumer_instance.get_status()  # Await here once
                return APIResponse(status="success", message=status)
            except Exception as e:
                return HTTPException(status_code=500, detail=f"Failed to get status of consumer {message.name}: {e}")

        else:
            raise HTTPException(status_code=400, detail="Invalid action")
    except HTTPException as http_exc:
        # Handle HTTP exceptions raised within the endpoint
        raise http_exc
    except Exception as e:
        # Handle any unexpected exceptions and return a generic server error
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")



