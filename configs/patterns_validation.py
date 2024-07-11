import re
from pydantic import BaseModel, field_validator, HttpUrl, ValidationError, PostgresDsn
from typing import Union, Any, Optional, List, Dict, Required

rabbitmq_url_pattern = re.compile(
    r"^amqp:\/\/"              # Протокол amqp://
    r"(?P<username>[^:]+)?"    # Имя пользователя (необязательное, может быть пустым)
    r"(?::(?P<password>[^@]+))?"  # Пароль (необязательное, может быть пустым)
    r"@(?P<host>[^:\/]+)"      # Хост (обязательно)
    r"(?::(?P<port>\d+))?$"    # Порт (необязательное, может быть пустым)
)

class RabbitMQUrl(BaseModel):
    url: str

    @field_validator('url')
    def validate_url(cls, value):
        if not rabbitmq_url_pattern.match(value):
            raise ValueError('Invalid RabbitMQ URL format')
        return value
    
    
class PostgreSQLModel(BaseModel):
    db: PostgresDsn

    @field_validator('db')
    def check_db_name(cls, v):
        assert v.path and len(v.path) > 1, 'database must be provided'
        return v


class APIMessage(BaseModel):
    name: Union[str, List[str]]
    action: str
    
    
class APIResponse(BaseModel):
    status: str
    message: str
    