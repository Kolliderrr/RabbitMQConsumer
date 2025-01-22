"""


"""
from sqlalchemy import create_engine, select, insert, update, text, URL
from sqlalchemy.schema import MetaData, Table, Column
from sqlalchemy.dialects import postgresql as dialect_postgres

from typing import Dict, List

import pandas as pd

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


def main_models() -> Dict[str, Table]:

    def create_tablemodels(table_parsed: List, columns: pd.DataFrame) -> Dict[str, Table]:
        out = {}
        metadata = MetaData()
        for table_name in table_parsed:
            model = Table(table_name,
                          metadata)
            for index, row in columns[columns['table_name'] == table_name].iterrows():
                col_values = row.values.tolist()
                column = Column(col_values[1], types[col_values[2]])
                model.append_column(column)
            out[table_name] = model

        return out

    def parse_mariadbinfo():
        engine = create_engine(URL.create('postgresql+psycopg2',
                                          database='analytics_v2',
                                          username='first_user',
                                          password='Emperor011',
                                          host='192.168.20.122',
                                          port=8085
                                          ))
        with engine.connect() as conn:
            query = text(f"""
        select *
from analytics_v2.information_schema."columns" c 
where (table_name like '%updates%' or table_name like '%remains%' or table_name like '%auto%' or table_name like '%updated%')
            """)
            df = pd.read_sql(sql=query, con=conn)
        return df

    df = parse_mariadbinfo()
    return create_tablemodels(df['table_name'].unique().tolist(), df[['table_name', 'COLUMN_NAME'.lower(), 'udt_name'.lower()]])

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

                
                