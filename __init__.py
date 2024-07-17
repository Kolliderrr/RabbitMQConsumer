"""Импорт и инициализация модулей Flask и FastAPI


    """
from .flask_main import create_app
from .server import main_app
from .configs import RabbitMQUrl, RequestModel, PostgreSQLModel

# Example of global initialization code
print("Initializing RabbitMQ project package")
