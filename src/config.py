"""Конфиги потребителя
"""

import sqlalchemy.dialects.postgresql as dialect_postgres
import os
import importlib.util
from typing import Literal, Optional, Dict, Any

# Переменные по умолчанию
TABLE_NAME: Optional[Literal[True]] = None
table_names: Optional[Dict[str, str]] = {}

# Проверяем, существует ли custom_config.py
custom_config_path = os.path.join(os.getcwd(), 'custom_config.py')

if os.path.exists(custom_config_path):
    # Динамически импортируем custom_config
    spec = importlib.util.spec_from_file_location("custom_config", custom_config_path)
    custom_config = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(custom_config)

    # Обновляем переменные, если они определены в custom_config
    if hasattr(custom_config, 'TABLE_NAME'):
        TABLE_NAME = getattr(custom_config, 'TABLE_NAME')
        print(f"TABLE_NAME переопределён: {TABLE_NAME}")

    if hasattr(custom_config, 'event_names'):
        table_names = getattr(custom_config, 'event_names')
        print(f"event_names переопределён: {table_names}")
    
else:
    print("Файл custom_config.py не найден.")

# Текущие значения переменных
print(f"Итоговые значения:\nTABLE_NAME: {TABLE_NAME}\nevent_names: {table_names}")

python_reverse_types = {
        'int' : dialect_postgres.BIGINT,
        'bool': dialect_postgres.BOOLEAN,
        'datetime': dialect_postgres.TIMESTAMP,
        'float': dialect_postgres.FLOAT,
        'str': dialect_postgres.VARCHAR,
        'float': dialect_postgres.FLOAT
    }