"""_summary_

    Returns:
        _type_: _description_
        
        
    
"""
import json
import logging
import os
from flask import Flask, render_template, \
    redirect, url_for, flash, request
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL
from pydantic import ValidationError

from .resources import RabbitMQForm, PostgresForm, Adress, DynamicForm




def get_all_routes(app, mount_prefix="/flask"):
    """_summary_

    Args:
        app (_type_): _description_
        mount_prefix (str, optional): _description_. Defaults to "/flask".

    Returns:
        _type_: _description_
    """
    routes = []
    for rule in app.url_map.iter_rules():
        # Оставляем только те маршруты, которые являются функциями
        if "GET" in rule.methods and hasattr(app.view_functions[rule.endpoint], "__call__"):
            url = rule.rule
            # Исключаем маршруты, начинающиеся с /static/
            if not url.startswith("/static/"):
                routes.append((mount_prefix + url, rule.endpoint))
    return routes


def create_app():
    """_summary_

    Returns:
        _type_: _description_
    """
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    app = Flask(__name__, static_url_path='/static')

    drivernames = {
        'PostgreSQL': 'postgresql+psycopg2',
        'MariaDB': 'mariadb+pymysql'
    }

    # dash_app = create_dash(server=app)

    app.config['SECRET_KEY'] = 'txkclgObc1fb7Q=='

    @app.route('/rabbitmq_create', methods=['GET', 'POST'], endpoint='rabbitmq_create')
    def rabbitmq_create():
        """_summary_

        Returns:
            _type_: _description_
        """
        form = RabbitMQForm()
        if form.validate_on_submit():
            if not os.path.isfile(r'configs\consumers.json'):
                with open(r'configs\consumers.json', 'w', encoding='cp1251'):
                    pass

            with open(r'configs\consumers.json', 'r', encoding='cp1251') as old_broker_config:
                old_config = json.load(old_broker_config)

            with open(r'configs\consumers.json', 'w', encoding='cp1251') as broker_config:
                config = {
                    'broker_url': form.broker_url.data,
                    'queue_name': form.queue_name.data,
                    'exchange_name': form.exchange_name.data,
                    'routing_key': form.routing_key.data,
                    'db': form.db.data,
                    'table': form.table.data
                }
                old_config[form.consumer_name.data] = config
                json.dump(old_config, broker_config)

            flash('RabbitMQ settings saved successfully!', 'success')
            logger.info('success: %s', config.values())
            return redirect(url_for('rabbitmq_create'))
        return render_template('rabbitmq.html', form=form)

    @app.route('/postgres', methods=['GET', 'POST'], endpoint='postgres')
    def postgres():
        """_summary_

        Returns:
            _type_: _description_
        """
        form = PostgresForm()
        tables = []
        if form.validate_on_submit():
            # Обработка данных формы Postgres
            try:
                Adress.model_validate({'host': form.host.data})

                database_url = URL.create(
                    drivername=drivernames[form.database_type.data],
                    username=form.login.data,
                    password=form.password.data,
                    host=form.host.data,
                    port=form.port.data,
                    database=form.database_name.data)
                engine = create_engine(database_url)
                inspector = inspect(engine)
                tables = inspector.get_table_names()

                if os.path.isfile(r'configs\db_config.json') is False:
                    if os.name != 'posix':
                        with open(r'configs\db_config.json', 'w', encoding='cp1251') as db_file:
                            json.dump({}, db_file)

                with open(r'configs\db_config.json', 'r', encoding='cp1251') as db_json:
                    old_db_data = json.load(db_json)

                old_db_data[str(form.database_name.data + \
                    '@' + form.host.data + ':' + form.port.data)] = \
                        str(f"{drivernames[form.database_type.data]}://{form.login.data}:" +
                            f"{form.password.data}@{form.host.data}:{form.port.data}/" +
                            f"{form.database_name.data}")

                with open(r'configs\db_config.json', 'w', encoding='cp1251') as db_to_load:
                    json.dump(old_db_data, db_to_load)

                flash('Postgres settings saved successfully!', 'success')
                logger.info('success: %s',engine.url)
            except Exception as e:
                if isinstance(e, ValidationError):
                    flash('Неверно указан ip-адрес', 'danger')
                    logger.error(e)
                else:
                    flash(f'Error connecting to database: {str(e)}', 'danger')
                    logger.error(e)

        return render_template('postgres.html', form=form, tables=tables)

    @app.route('/dynamic', methods=['GET', 'POST'], endpoint='generate_table_model')
    def generate_table_model():
        """_summary_

        Returns:
            _type_: _description_
        """
        form = DynamicForm()

        # Проверяем наличие файла и создаем его, если он отсутствует
        if not os.path.isfile('configs/tables.json'):
            with open('configs/tables.json', 'w', encoding='cp1251') as db_tables:
                json.dump({}, db_tables)

        if request.method == 'POST':
            logger.info('POST-query.')
            if 'add' in request.form:
                form.dynamic_fields.append_entry()
            elif 'remove' in request.form and len(form.dynamic_fields) > 0:
                form.dynamic_fields.pop_entry()

            elif form.validate_on_submit():
                with open('configs/tables.json', 'r', encoding='cp1251') as old_db_tables:
                    tables_data = json.load(old_db_tables)

                # Извлекаем данные из формы
                new_fields = []
                for subfield in form.dynamic_fields:
                    field_data = {
                        'name': subfield.field_name.data,
                        'type': subfield.field_type.data
                    }
                    new_fields.append(field_data)

                # Добавляем новые данные в таблицы
                # Пример генерации имени таблицы
                new_table_name = "Table_" + str(len(tables_data) + 1)
                tables_data[new_table_name] = new_fields

                # Записываем обновленные данные обратно в файл
                with open(os.path.join(r'configs/tables.json'),
                          'w', encoding='cp1251') as db_tables:
                    json.dump(tables_data, db_tables)

                flash('Form submitted successfully!', 'success')
                logger.info('success: %s', new_table_name)
                return redirect(url_for('generate_table_model'))

        return render_template('dynamic.html', form=form)

    # @app.route('/dash_table', methods=['GET'])
    # def dash_table():
    #     with app.app_context():
    #         return dash_app.index()

    @app.route('/favicon.ico')
    def favicon():
        """_summary_

        Returns:
            _type_: _description_
        """
        return app.send_static_file('favicon.png')

    @app.context_processor
    def inject_routes():
        """_summary_

        Returns:
            _type_: _description_
        """
        return {'routes': get_all_routes(app)}

    return app
