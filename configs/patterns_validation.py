import re
from pydantic import BaseModel, field_validator, PostgresDsn, ValidationError, model_validator
from typing import Union, List, Optional, Literal

rabbitmq_url_pattern = re.compile(
    r"^amqp:\/\/"              # Протокол amqp://
    r"(?P<username>[^:]+)?"    # Имя пользователя (необязательное, может быть пустым)
    r"(?::(?P<password>[^@]+))?"  # Пароль (необязательное, может быть пустым)
    r"@(?P<host>[^:\/]+)"      # Хост (обязательно)
    r"(?::(?P<port>\d+))?$"    # Порт (необязательное, может быть пустым)
)

class RabbitMQUrl(BaseModel):
    url: str

    @field_validator('url', mode='before')
    @classmethod
    def validate_url(cls, value):
        if not rabbitmq_url_pattern.match(value):
            raise ValueError('Invalid RabbitMQ URL format')
        return value

class PostgreSQLModel(BaseModel):
    db: PostgresDsn

    @field_validator('db', mode='before')
    @classmethod
    def check_db_name(cls, v):
        assert v.path and len(v.path) > 1, 'database must be provided'
        return v

from typing import Optional, Literal
from pydantic import BaseModel

class Params(BaseModel):
    queue_name: Optional[str] = None
    routing_key: Optional[str] = None
    db: Optional[str] = 'postgresql+asyncpg://first_user:Emperor011@192.168.20.122:8085/analytics_v2'
    table_name: Optional[str] = None

class CheckParams(BaseModel):
    health: Optional[bool] = False
    messages_left: Optional[bool] = False
    lifetime: Optional[bool] = False

class RequestModel(BaseModel):
    name: str
    action: Literal['start', 'stop', 'check']
    params: Optional[Params] = None

    @classmethod
    def check_params(cls, values):
        action = values.get('action')
        params = values.get('params')

        if action == 'stop' and params is not None:
            raise ValueError(f"'params' should not be provided when action is 'stop'")
        
        if action == 'check':
            if params is None:
                raise ValueError(f"'params' must be provided when action is 'check'")
            if not isinstance(params, CheckParams):
                raise ValueError(f"'params' must match CheckParams when action is 'check'")
        return values

    class Config:
        from_attributes = True

    @classmethod
    def validate(cls, values):
        return cls.check_params(values)

