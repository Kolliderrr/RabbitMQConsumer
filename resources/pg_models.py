"""


"""
from sqlalchemy import create_engine, select, insert, update, text, URL
from sqlalchemy.schema import MetaData, Table, Column, ForeignKey
from sqlalchemy.dialects import postgresql as dialect_postgres
from sqlalchemy.orm import registry

from typing import Dict, List, Any, Optional

from pydantic import BaseModel, create_model
import pandas as pd

types = {'bigint' : dialect_postgres.BIGINT,
         'boolean': dialect_postgres.BOOLEAN,
         'bool': dialect_postgres.BOOLEAN,
         'date': dialect_postgres.DATE,
         'float': dialect_postgres.FLOAT,
         'varchar': dialect_postgres.VARCHAR,
         'numeric': dialect_postgres.NUMERIC,
         'float8': dialect_postgres.FLOAT,
         'double precision': dialect_postgres.DOUBLE_PRECISION,
         'int4': dialect_postgres.INT4RANGE,
         'text': dialect_postgres.TEXT,
         'timestamp': dialect_postgres.TIMESTAMP,
         
         }


def main_models() -> Dict[str, Table]:

    def create_tablemodels(table_parsed: List, columns: pd.DataFrame) -> Dict[str, Table]:
        out = {}
        metadata = registry()
        for table_name in table_parsed:
            model = Table(table_name,
                          metadata.metadata)
            for index, row in columns[['table_name', 'column_name', 'udt_name']].loc[columns['table_name'] == table_name].iterrows():
                col_values = row.values.tolist()
                column = Column(col_values[1], types[col_values[2]])
                model.append_column(column)
            out[table_name] = model

        return out

    def parse_mariadbinfo() -> pd.DataFrame:
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
where table_schema = 'public';
            """)
            df = pd.read_sql(sql=query, con=conn)
        return df

    df = parse_mariadbinfo()
    return create_tablemodels(df['table_name'].unique().tolist(), df[['table_name', 'COLUMN_NAME'.lower(), 'udt_name'.lower()]])

def str2bool(v):
    return v.lower() in ('true')

def parse_table_model(table_name: str,
                      params: Optional[Dict[str, Any]]=None) -> Table:
    if params:
        engine = create_engine(
            URL.create(
                drivername=params['drivername'],
                host=params['host'],
                port=params['port'],
                database=params['database'],
                username=params['login'],
                password=params['password']
            )
        )
    else:
        engine = create_engine(
            URL.create('postgresql+psycopg2',
                database='analytics_v2',
                username='first_user',
                password='Emperor011',
                host='192.168.20.122',
                port=8085
                )
            )
    with engine.connect() as conn:
        query = text(f"""
SELECT
    c.table_name,
    c.column_name,
    c.udt_name,
    (SELECT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND kcu.column_name = c.column_name
        AND kcu.table_name = c.table_name
    )) AS is_primary_key,
    COALESCE((
        SELECT CONCAT(kcu2.table_name, '.', kcu2.column_name)
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_name = kcu.table_name
        JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
        JOIN information_schema.key_column_usage kcu2
        ON ccu.table_name = kcu2.table_name
        AND ccu.column_name = kcu2.column_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND kcu.column_name = c.column_name
        AND kcu.table_name = c.table_name
    ), 'NULL') AS foreign_key_reference
FROM
    information_schema.columns c
WHERE
    c.table_schema = 'public' and c.table_name = '{table_name}'
ORDER BY
    c.table_name, c.column_name

        """)
        df = pd.read_sql(sql=query, con=conn, dtype={
            'table_name': str,
            'column_name': str,
            'udt_name': str,
            'is_primary_key': bool,
            'foreign_key_reference': str
            
        })
    
    metadata = MetaData()
    
    model = Table(table_name,
                    metadata)
    for index, row in df[['table_name', 'column_name', 'udt_name', 'is_primary_key', 'foreign_key_reference']].iterrows():
        if row['is_primary_key']:
            col_values = row.values.tolist()
            column = Column(col_values[1], types[col_values[2]], primary_key=True)
        elif row['foreign_key_reference'] != 'NULL':
            col_values = row.values.tolist()
            column = Column(col_values[1], ForeignKey(col_values[-1]), types[col_values[2]])
        else:
            col_values = row.values.tolist()
            column = Column(col_values[1], types[col_values[2]])
        model.append_column(column)
    
    return model
    
def model_to_dict_alch(model: Table) -> Dict[str, Any]:
    return {
        column.name: (column.type.python_type, ...) for column in model.columns
    }

def create_pydantic_model(table_name: str,
                          params: Optional[Dict[str, Any]]=None) -> BaseModel:
    if params:
        model = parse_table_model(table_name=table_name, params=params)
    else:
        model = parse_table_model(table_name=table_name)
    fields = model_to_dict_alch(model)
    
    # Extract the annotations and create the model dynamically
    annotations = {key: (value[0], ...) for key, value in fields.items()}
    pydantic_model = create_model('DynamicPydanticModel', **annotations)
    
    return pydantic_model

# import asyncio
# from datetime import datetime

# async def validate_json(message: Dict, table_model: Table, table_name: str = None):
#     python_types = {
#         'bigint' : int,
#         'boolean': bool,
#         'bool': bool,
#         'date': datetime,
#         'float': float,
#         'varchar': str,
#         'numeric': float,
#         'float8': float,
#         'double precision': float
#     }

#     async def validate_key_value(key, value, column_type):
#         if not isinstance(value, column_type):
#             try:
#                 if column_type == bool:
#                     message[key] = str2bool(value)
#                 elif column_type == str:
#                     message[key] = str(value)
#                 elif column_type == float:
#                     message[key] = float(value)
#                 elif column_type == int:
#                     message[key] = int(value)
#                 elif column_type == datetime:
#                     if table_name == 'product_updates_1': #if time in date:
#                         message[key] = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
#                     else:
#                         #if no time in date:
#                         message[key] = datetime.strptime(value, '%d.%m.%Y')
                    
#             except Exception as e:
#                 message[key] = value

#     tasks = []
#     for key, value in message.items():
#         column_type = python_types[table_model.columns[key].type.__repr__().replace('()', '').lower()]
#         tasks.append(validate_key_value(key, value, column_type))
    
#     await asyncio.gather(*tasks)



                
                