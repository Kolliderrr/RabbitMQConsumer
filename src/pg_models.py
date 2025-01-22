"""


"""
from sqlalchemy import create_engine, select, insert, update, text, URL
from sqlalchemy.schema import MetaData, Table, Column
from sqlalchemy.dialects import postgresql as dialect_postgres

from typing import Dict, List, Union

import pandas as pd

import os

POSTGRES_URL = os.getenv("POSTGRES_URL")
DB_NAME = os.getenv("DB_NAME")

types = {'bigint' : dialect_postgres.BIGINT,
         'boolean': dialect_postgres.BOOLEAN,
         'bool': dialect_postgres.BOOLEAN,
         'int4': dialect_postgres.INTEGER,
         'int8': dialect_postgres.INTEGER,
         'date': dialect_postgres.DATE,
         'float': dialect_postgres.FLOAT,
         'varchar': dialect_postgres.VARCHAR,
         'numeric': dialect_postgres.NUMERIC,
         'float8': dialect_postgres.FLOAT,
         'double precision': dialect_postgres.DOUBLE_PRECISION,
         'timestamp': dialect_postgres.TIMESTAMP,
         'jsonb': dialect_postgres.JSONB,
         'text': dialect_postgres.TEXT,
         'uuid': dialect_postgres.UUID
         }
class MissingData(Exception):
    pass

def main_models(
    TABLES: Union[List[str], None] = None
    ) -> Dict[str, Table]:
    """Table model generator

    Args:
        TABLES (Union[List[str], None], optional): List of needed tables for recording data. Defaults to None.

    Returns:
        Dict[str, Table]: Dict with table names and sqlalchemy.schema.Table entities.
    """
    def create_tablemodels(
        table_parsed: List, 
        columns: pd.DataFrame) -> Dict[str, Table]:
        out = {}
        metadata = MetaData()
        for table_name in table_parsed:
            model = Table(table_name,
                          metadata)
            for _, row in columns[columns['table_name'] == table_name].iterrows():
                col_values = row.values.tolist()
                column = Column(col_values[1], types[col_values[2]])
                model.append_column(column)
            out[table_name] = model

        return out

    def parse_info(
        TABLES: Union[List[str], None] = None
        ) -> pd.DataFrame:
        engine = create_engine(POSTGRES_URL)
        query = None
        if TABLES:
            q_value = f"""
        select *
from {DB_NAME}.information_schema."columns" c 
WHERE table_name IN ({", ".join(TABLES)})
            """
        else:
            q_value = f"""
        select *
from {DB_NAME}.information_schema."columns" c 
            """
        
        with engine.connect() as conn:
            
            query = text(q_value)
            df = pd.read_sql(sql=query, con=conn)
            if df.empty:
                raise MissingData(f"There is no tables in {DB_NAME} on {POSTGRES_URL}")
            return df
    
    df = parse_info(TABLES)
    return create_tablemodels(
        df['table_name'].unique().tolist(), 
        df[['table_name', 'COLUMN_NAME'.lower(), 
            'udt_name'.lower()]]
        )

def str2bool(v):
    return v.lower() in ('true')

import asyncio
from datetime import datetime

async def validate_json(message: Dict, table_model: Table):
    python_types = {
        'bigint' : int,
        'int4': int,
        'boolean': bool,
        'bool': bool,
        'date': datetime,
        'float': float,
        'varchar': str,
        'numeric': float,
        'float8': float,
        'double precision': float,
        'uuid': str
    }
    
    async def validate_key_value(key, value, column_type):
        if not isinstance(value, column_type):
            try:
                if column_type == bool:
                    message[key] = str2bool(value)
                elif column_type == str:
                    message[key] = str(value)
                elif column_type == float:
                    message[key] = float(value)
                elif column_type == int:
                    message[key] = int(value)
                elif column_type == datetime:
                    if table_model.name == 'product_updates_1': #if time in date:
                        message[key] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    else:
                        #if no time in date:
                        message[key] = datetime.strptime(value, '%d.%m.%Y')
                    
            except Exception as e:
                message[key] = value

    tasks = []
    for key, value in message.items():
        column_type = python_types[table_model.columns[key].type.__repr__().replace('()', '').lower()]
        tasks.append(validate_key_value(key, value, column_type))
    
    await asyncio.gather(*tasks)

                
                