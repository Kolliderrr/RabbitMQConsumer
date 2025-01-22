"""Методы для работы с сообщениями RabbitMQ

Raises:
    ValueError: Ошибка типа данных
    MissingTable: Если такой таблицы не существует
"""

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.sql import text, Insert, Update, Delete
from sqlalchemy.exc import IntegrityError, \
    DBAPIError, NoSuchTableError, ProgrammingError
from sqlalchemy.schema import Table, Column
from sqlalchemy.dialects import postgresql \
    as dialect_postgres
from sqlalchemy import select, insert, update
from pg_models import validate_json, main_models
from typing import \
    List, Any, Dict, Union, Optional, Literal, Tuple
import pandas as pd
import logging
from datetime import datetime
import sys
from config import TABLE_NAME, python_reverse_types, table_names

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Меняем уровень на DEBUG, чтобы видеть больше логов

# Хендлер для записи в файл
file_handler = logging.FileHandler(f"{__name__}.log", mode='w')
file_handler.setLevel(logging.DEBUG)  # Логи пишутся в файл

# Хендлер для вывода в консоль
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)  # Логи выводятся в консоль контейнера

# Формат логов
formatter = logging.Formatter("%(name)s %(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Добавление хендлеров в логгер
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class MissingTable(Exception):
    pass

class MissingFields(Exception):
    pass

async def db_write(
    payload: Union[Dict[str, Any],List[Dict[str, Any]]],
    table_model: Table,
    engine: AsyncEngine,
    table_models: Dict[str, Table]
    ) -> Union[str, Tuple[str, str], None]:
    """Запись в нужную таблицу. Можно записывать весь массив (необходимо дописать обработку записи) или поочерёдно вызывать process_message()

    Args:
        payload (Union[Dict[str, Any],List[Dict[str, Any]]]): Массив данных
        table_model (Table): Модель SQLAlchemy для описания таблицы
        engine (AsyncEngine): Движок соединения с БД

    Returns:
        Union[str, Tuple[str, str], None]: Ошибка вернётся
    """
    pass

async def branch_task(
    table_name: str
    ) -> Optional[TABLE_NAME]:
    """Выбор таблицы на основе event-name

    Args:
        event_name (str): Берётся из JSON-массива, путь data['event']['name']

    Raises:
        MissingTable: Если такой таблицы нет

    Returns:
        Optional[TABLE_NAME]: название таблицы в БД
    """
    try:
        return table_names[table_name]
    except (ValueError, AttributeError):
        raise MissingTable(
            'Таблицы для имени события {} не существует или не указана'\
                .format(table_name))
    

async def on_conflict_do(
    row: Dict[str, Any],
    engine: AsyncEngine,
    table_models: Dict[str, Table]) -> None:
    """
    On-conflict operation: pass
    
    Args:
        row (Dict[str, Any]): record to write
        engine (AsyncEngine): Engine for DB-API
        table_models (Dict[str, Table]): table models SQLAlchemy
    """
    pass        
                
       
async def process_message(
    payload_raw: Union[Dict[str, Any],List[Dict[str, Any]]],
    table_model: Table,
    engine: AsyncEngine) -> Optional[Union[Dict[str, Any],List[Dict[str, Any]]]]:
    """Запись сообщения для справочника
       Processing input data for complicated steps.

    Args:
        payload (Union[Dict[str, Any],List[Dict[str, Any]]]): JSON-массив с ключом UID
        table_model (Table): Модель SQLAlchemy для описания таблицы
        engine (AsyncEngine): Движок соединения с БД

    Returns:
        Optional[Union[Dict[str, Any],List[Dict[str, Any]]]]: В случае ошибки будет возвращать результат ошибки
    """
    pass
