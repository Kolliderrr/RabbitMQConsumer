"""_summary_


    """
from .consumers.consumer_model import RabbitConsumer
from .resources.pg_models import parse_table_model, create_pydantic_model, main_models
from .configs.patterns_validation import RabbitMQUrl, APIMessage, APIResponse, PostgreSQLModel
from .resources.forms import DynamicForm, PostgresForm, RabbitMQForm, Adress

# Example of global initialization code
print("Initializing RabbitMQ project package")
