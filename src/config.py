"""Конфиги потребителя
"""


import sqlalchemy.dialects.postgresql as dialect_postgres
from typing import Literal


TABLE_NAME = Literal[
    'sales_doc', 
    'sales_updates',
    'agent_updates_1',
    'category_updates_1',
    'department_updates_1',
    'manufacturer_updates_1',
    'product_updates_1',
    'remain_prices',
    'remain_updated_new',
    'remain_updated_short',
    'sales_auto',
    'user_updates_1',
    'agent_remains',
    'warehouse_updated'
    ]

        
event_names = {
    'sales.document.updated': 'sales_updates',
    'car.sales.document.updated': 'sales_auto',
    'agent.updated': 'agent_updates_1',
    'manufacturer.updated': 'manufacturer_updates_1',
    'category.updated': 'category_updates_1',
    'product.updated': 'product_updates_1',
    'department.updated': 'department_updates_1',
    'remains.updated': 'remains_updated_new',
    'user.updated': 'user_updates_1',
    'warehouse.updated': 'warehouse_updated'
}

python_reverse_types = {
        'int' : dialect_postgres.BIGINT,
        'bool': dialect_postgres.BOOLEAN,
        'datetime': dialect_postgres.TIMESTAMP,
        'float': dialect_postgres.FLOAT,
        'str': dialect_postgres.VARCHAR,
        'float': dialect_postgres.FLOAT
    }