"""_summary_

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
"""

import re
from pydantic import BaseModel, field_validator, PostgresDsn
from typing import Union,  List

rabbitmq_url_pattern = re.compile(
    r"^amqp:\/\/"              # Протокол amqp://
    r"(?P<username>[^:]+)?"    # Имя пользователя (необязательное, может быть пустым)
    r"(?::(?P<password>[^@]+))?"  # Пароль (необязательное, может быть пустым)
    r"@(?P<host>[^:\/]+)"      # Хост (обязательно)
    r"(?::(?P<port>\d+))?$"    # Порт (необязательное, может быть пустым)
)

class RabbitMQUrl(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
    """
    url: str

    @field_validator('url')
    def validate_url(self, value):
        """_summary_

        Args:
            value (_type_): _description_

        Raises:
            ValueError: _description_

        Returns:
            _type_: _description_
        """
        if not rabbitmq_url_pattern.match(value):
            raise ValueError('Invalid RabbitMQ URL format')
        return value

class PostgreSQLModel(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_

    Returns:
        _type_: _description_
    """
    db: PostgresDsn

    @field_validator('db')
    def check_db_name(self, v):
        """_summary_

        Args:
            v (_type_): _description_

        Returns:
            _type_: _description_
        """
        assert v.path and len(v.path) > 1, 'database must be provided'
        return v

class APIMessage(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_
    """
    name: Union[str, List[str]]
    action: str

class APIResponse(BaseModel):
    """_summary_

    Args:
        BaseModel (_type_): _description_
    """
    status: str
    message: str
