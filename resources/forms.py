from flask_wtf import FlaskForm
from sqlalchemy import create_engine, inspect
from wtforms import StringField, SubmitField, SelectField, FieldList, FormField
from wtforms.validators import DataRequired
from pydantic import BaseModel, Field
import os, json

if os.path.isfile(r'configs\db_config.json') is False:
    with open(r'configs\db_config.json', 'w') as db_configs:
        json.dump({''}, db_configs)
    
with open(r'configs\db_config.json', 'r') as db_config_file:
    db_configs = json.load(db_config_file)

db_choices = [(value, key) for key, value in db_configs.items()]

def return_tables(database_url):
    engine = create_engine(database_url)
    inspector = inspect(engine)
    return inspector.get_table_names()


class Adress(BaseModel):
    host: str = Field(pattern=r'^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$')


class RabbitMQForm(FlaskForm):
    consumer_name = StringField('Consumer name (for Dash)', validators=[DataRequired()])
    broker_url = StringField('Broker URL', validators=[DataRequired()])
    queue_name = StringField('Queue Name', validators=[DataRequired()])
    exchange_name = StringField('Exchange Name', validators=[DataRequired()])
    routing_key = StringField('Routing Key', validators=[DataRequired()])
    db = SelectField('Database', choices=db_choices, validators=[DataRequired()])
    table = StringField('Table Name', validators=[DataRequired()])
    submit = SubmitField('Save')

class PostgresForm(FlaskForm):
    database_type = SelectField('Database Type / Провайдер БД', choices=['PostgreSQL', 'MariaDB (in progress)'])
    host = StringField('Host / Хост (адрес, IP-адрес)', validators=[DataRequired()])
    port = StringField('Port / Порт', validators=[DataRequired()])
    database_name = StringField('Database name / Имя экземпляра БД', validators=[DataRequired()])
    login = StringField('Login / логин', validators=[DataRequired()])
    password = StringField('Password / Пароль', validators=[DataRequired()])
    submit = SubmitField('Save')
    
class DynamicFieldForm(FlaskForm):
    field_name = StringField('Название', validators=[DataRequired()])
    field_type = SelectField('Тип', choices=[('text', 'Text'), ('number', 'Number'), ('date', 'Date'), ('bool', 'Boolean')], validators=[DataRequired()])

class DynamicForm(FlaskForm):
    dynamic_fields = FieldList(FormField(DynamicFieldForm), min_entries=1)
    submit = SubmitField('Submit')
    
    
