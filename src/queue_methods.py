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
from config import TABLE_NAME, python_reverse_types, event_names

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

async def format_message(json_data: dict, model: Table) -> pd.DataFrame:
    """Форматирование сообщения для остатков по складам

    Args:
        json_data (dict): массив данных
        model (Table): табличная модель SQLAlchemy

    Raises:
        ValueError: Если неправильный тип данных

    Returns:
        pd.DataFrame: Pandas.Dataframe отформатированный
    """
    json_data['load_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Проверка, является ли json_data['data'] строкой
    if isinstance(json_data['date'], str):
        json_data['date'] = datetime.strptime(json_data['date'], "%Y-%m-%d %H:%M:%S")
    elif isinstance(json_data['date'], datetime):
        pass  # уже datetime, ничего не нужно делать
    else:
        if json_data['date'] is None:
            json_data['date'] = datetime.now()
        else:
            raise ValueError(f"Unexpected data type for 'data': {type(json_data['date'])}")

    json_data['load_date'] = datetime.strptime(json_data['load_date'], "%Y-%m-%d %H:%M:%S")
    
    if 'message_id' in json_data:  # Удаляем 'message_id' из исходного словаря
        del json_data['message_id']
    
    if model.name == 'remains_updated':
        df = pd.DataFrame([json_data])
        # df.drop(columns='message_id', inplace=True)
        return pd.DataFrame([json_data])
    
    elif model.name == 'remains_updated_new':
        df = pd.DataFrame([json_data])
        df.rename(columns={'uid': 'warehouse_uid'}, inplace=True)
        df.dropna(subset=['products'], axis=0, inplace=True)
        df = df.explode('products')
        df = pd.concat([df, df['products'].apply(pd.Series)], axis=1)
        df.reset_index(drop=True, inplace=True)
        df.drop(columns='products', inplace=True)
        # df.drop(columns='message_id', inplace=True)
        return df
    
    elif model.name == 'remains_updated_short':
        df = pd.DataFrame([json_data])
        df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%d %H:%M:%S")  # Исправлено здесь
        df.rename(columns={'uid':'warehouse_uid'}, inplace=True)
        df.dropna(subset=['products'], axis=0, inplace=True)
        df = df.explode('products')
        df = pd.concat([df, df['products'].apply(pd.Series)], axis=1)
        df.reset_index(drop=True, inplace=True)
        df.drop(columns='products', inplace=True)
        # df.drop(columns='message_id', inplace=True)
        df['uid_new'] = df['warehouse_uid'] + ':' + df['uid']
        df.drop_duplicates(subset=['uid_new'], inplace=True)
        return df

async def db_write(
    payload: Union[Dict[str, Any],List[Dict[str, Any]]],
    table_model: Table,
    engine: AsyncEngine,
    table_models: Dict[str, Table]
    ) -> Union[str, Tuple[str, str], None]:
    """Запись в нужную таблицу

    Args:
        payload (Union[Dict[str, Any],List[Dict[str, Any]]]): Массив данных
        table_model (Table): Модель SQLAlchemy для описания таблицы
        engine (AsyncEngine): Движок соединения с БД

    Returns:
        Union[str, Tuple[str, str], None]: Ошибка вернётся
    """
    if isinstance(payload, dict):
        if 'date' in payload:
            if isinstance(payload['date'], str):
                payload['date'] = datetime.strptime(payload['date'], "%Y-%m-%d %H:%M:%S")
            elif isinstance(payload['date'], datetime):
                pass
        elif 'data' in payload:
            if isinstance(payload['data'], str):
                payload['data'] = datetime.strptime(payload['data'], "%Y-%m-%d %H:%M:%S")
            elif isinstance(payload['data'], datetime):
                pass
            
        if table_model.name in ['sales_updates', 'sales_auto']:
            await process_sales_data(payload, table_model, engine, table_models=table_models)
        elif table_model.name == 'remains_updated_new':
            models = main_models()
            for table in [models['remains_updated_new'], models['remains_updated_short']]:
                df = await format_message(payload, table)
                await process_remains_data(df, table, engine)
        else:
            await process_message(payload, table_model, engine)
    elif isinstance(payload, list):
        for item in payload:
            if 'date' in item:
                if isinstance(item['date'], str):
                    item['date'] = datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S")
                elif isinstance(item['date'], datetime):
                    pass
            elif 'data' in item:
                if isinstance(item['data'], str):
                    item['data'] = datetime.strptime(item['data'], "%Y-%m-%d %H:%M:%S")
                elif isinstance(item['data'], datetime):
                    pass
                
            if table_model.name in ['sales_updates', 'sales_auto']:
                await process_sales_data(item, table_model, engine, table_models=table_models)
            elif table_model.name == 'remains_updated_new':
                models = main_models()
                for table in [models['remains_updated_new'], models['remains_updated_short']]:
                    df = await format_message(item, table)
                    await process_remains_data(df, table, engine)
            else:
                await process_message(item, table_model, engine)


async def branch_task(
    event_name: str
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
        return event_names[event_name]
    except (ValueError, AttributeError):
        raise MissingTable(
            'Таблицы для имени события {} не существует или не указана'\
                .format(event_name))
    
async def process_remains_data(
    df: pd.DataFrame, 
    table_model: Table, 
    engine: AsyncEngine) -> None:
    """Запись данных остатков на складах

    Args:
        df (pd.DataFrame): Предобработанное сообщение в виде таблицы Pandas
        table_model (Table): Модель SQLAlchemy для описания таблицы
        engine (AsyncEngine): Движок соединения с БД
    """
    rows = df.to_dict(orient='records')
    
    async with engine.begin() as conn:
        for record in rows:
            payload = dict()
            for key in record.keys():
                if key in table_model.columns.keys():
                    payload[key] = record[key]
            insert_stmt = table_model.insert().values(payload)
            try:
                await conn.execute(insert_stmt)
                logging.debug(f"Inserted record: {payload}")
            except IntegrityError:
                try:
                    update_stmt = update(table_model).where(table_model.c.uid_new == payload['uid_new'])\
                        .values(**{k: v for k, v in payload.items() if k != 'uid_new'})
                    await conn.execute(update_stmt)
                except Exception as er1:
                    logging.error(f"Error inserting record {payload}: {er1}")
                    raise       

async def on_conflict_do(
    row: Dict[str, Any],
    engine: AsyncEngine,
    table_models: Dict[str, Table]) -> None:
    """
    Метод для внесения 
    """
    constrains = {
        'product_uid': table_models['product_updates_1'],
        'manufacturer_uid': table_models['manufacturer_updates_1'],
        'agent_uid': table_models['agent_updates_1'],
        'category_uid': table_models['category_updates_1'],
        'department_uid': table_models['department_updates_1'],
        'user_uid': table_models['user_updates_1']
    }
    for column, table_model in constrains.items():
        update_stmt = insert(table_model).values(uid=row[column])
        async with engine.begin() as conn:
            try:
                await conn.execute(update_stmt)
            except Exception:
                continue
        logger.info('Wrote constraint {}'.format(column))         
                
async def process_sales_data(
    payload: Union[Dict[str, Any],List[Dict[str, Any]]],
    table_model: Table,
    engine: AsyncEngine,
    table_models: Dict[str, Table]
) -> None:
    """Запись данных продаж

    Args:
        payload (Union[Dict[str, Any],List[Dict[str, Any]]]): Словарь с вложенным массивом products
        table_model (Table): Модель SQLAlchemy для описания таблицы
        engine (AsyncEngine): Движок соединения с БД
    """
    async with engine.begin() as conn:
        if 'uid' in payload:
            payload['document_uid'] = payload.pop('uid')
        for product in payload['products']:
            row = {k: v for k, v in payload.items() if k != 'products'}
            for key, value in product.items():
                if key == 'uid':
                    key = 'product_uid'
                row[key] = value
            await validate_json(row, table_model)
            stmt = table_model.insert().values(row)
            try:
                await conn.execute(stmt)
                logging.info('Записан {} таблицы {}'.format(row['document_uid'], table_model.name))
            except IntegrityError:
                try:
                    update_stmt = table_model.update()\
                        .values({k: v for k, v in row.items() if k != 'document_uid'})\
                            .where((table_model.c.document_uid == row['document_uid']) & \
                                (table_model.c.product_uid == row['product_uid']))
                    try:
                        await conn.execute(update_stmt)
                        logging.info('Обновлен {} таблицы {}'.format(row['document_uid'], table_model.name))
                    except IntegrityError as interror:
                        await on_conflict_do(row, engine, table_models)
                        try:
                            await conn.execute(stmt)
                            logging.info('Записан {} таблицы {}'.format(row['document_uid'], table_model.name))
                        except IntegrityError:
                            await conn.execute(update_stmt)
                except (DBAPIError, ProgrammingError) as db_error:
                    try:
                        delete_stmt = table_model.delete()\
                            .where((table_model.c.document_uid == row['document_uid']) & \
                                (table_model.c.product_uid == row['product_uid']))
                        stmt = table_model.insert().values(row)
                        await conn.execute(delete_stmt)
                        await conn.execute(stmt)
                        logging.info('Обновлен {} таблицы {}'.format(row['document_uid'], table_model.name))
                    except Exception as error:
                        logging.error('Произошла ошибка: {}'.format(error))
                        raise
       
async def process_message(
    payload_raw: Union[Dict[str, Any],List[Dict[str, Any]]],
    table_model: Table,
    engine: AsyncEngine) -> Optional[Union[Dict[str, Any],List[Dict[str, Any]]]]:
    """Запись сообщения для справочника

    Args:
        payload (Union[Dict[str, Any],List[Dict[str, Any]]]): JSON-массив с ключом UID
        table_model (Table): Модель SQLAlchemy для описания таблицы
        engine (AsyncEngine): Движок соединения с БД

    Returns:
        Optional[Union[Dict[str, Any],List[Dict[str, Any]]]]: В случае ошибки будет возвращать результат ошибки
    """
    async with engine.begin() as conn:
        payload = dict()
        for key in payload_raw.keys():
            if key in table_model.columns.keys():
                payload[key] = payload_raw[key]
        existing_record = await conn.execute(select(table_model).where(table_model.c.uid == payload['uid']))
        
        if existing_record.rowcount > 0:
            if not 'parent' in payload.keys():
                try:
                    await validate_json(payload, table_model)
                    update_stmt = update(table_model).where(table_model.c.uid == payload['uid'])\
                        .values(**payload)
                    await conn.execute(update_stmt)
                except Exception as e:
                    logging.error('Ошибка в UPDATE')
                    logging.error(e)
                    raise
            else:
                try:
                    row = {k: v for k, v in payload.items() if k != 'parent'}
                    if payload['parent'] is not None:
                        row['parent'] = payload['parent']['uid']
                    await validate_json(row, table_model)
                    update_stmt = update(table_model).where(table_model.c.uid == row['uid'])\
                        .values(**row)
                    await conn.execute(update_stmt)
                except Exception as e:
                    logging.error('Ошибка в UPDATE 2')
                    logging.error(e)
                    raise
        else:
            if not 'parent' in payload.keys():
                try:
                    await validate_json(payload, table_model)
                    insert_stmt = insert(table_model).values(**payload)
                    await conn.execute(insert_stmt)
                except Exception as e:
                    logging.error('Ошибка в INSERT')
                    logging.error(e)
                    raise
            else:
                try:
                    row = {k: v for k, v in payload.items() if k != 'parent'}
                    if payload['parent'] is not None:
                        row['parent'] = payload['parent']['uid']
                    await validate_json(row, table_model)
                    insert_stmt = insert(table_model).values(**row)
                    await conn.execute(insert_stmt)
                except Exception as e:
                    logging.error('Ошибка в INSERT 2')
                    logging.error(e)
                    raise
