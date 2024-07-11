"""
Created on Fri 01.03.2024
by Valiev Artem

Модель подключения к БД

"""
import pandas as pd
import sqlalchemy
import sqlalchemy.sql.expression
from sqlalchemy import create_engine, select, Row, Select, Insert
from sqlalchemy.schema import Table, MetaData, Column
from sqlalchemy.sql.expression import Join
from sqlalchemy.sql import text
from sqlalchemy.types import String, Date
from typing import Any, Dict, List, Union, Optional
import logging
from pandas import DataFrame
from resources.pg_models import main_models

logging.basicConfig(filename='SQL_log.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')


class SQLengine:
    """
    Описание класса для соединения с таблицей БД

    На вход подаётся словарь с параметрами, SQLAlchemy ORM собирает запрос и передаёт его
    в таблицу PostgreSQL.


    :func load_pandas(): Загрузка DataFrame в инициированную БД

    :func load_data(): Выгрузка из БД словаря python.

    :func insert_data(): Загрузка словаря в инициированную БД.
    """
    def __init__(self,
                 db_type: str = 'postgresql+psycopg2:',
                 credentials =None,
                 host: str = '192.168.20.122',
                 port: int = 8085,
                 db: str = 'analysis',
                 engine_url: str = None) -> None:
        """
        :param db_type: Тип БД SQL. Вводится как URL-head. Пример: 'postgresql+psycopg2'

        :param credentials: Словарь типа {username: <USERNAME>, password: <PASSWORD>}

        :param host: URL или IP-адрес хоста БД.

        :param port: Входящий порт БД

        :param db: Наименование экземпляра БД, грубо говоря - имя БД.
        """

        if credentials is None:
            self.credentials = {
                'user': 'first_user',
                'password': 'Emperor011'
            }
        else:
            self.credentials = credentials

        self.username = self.credentials['user']
        self.password = self.credentials['password']

        if engine_url is None:
            self.engine = create_engine(f'{db_type}//{self.username}:{self.password}@{host}:{port}/{db}')
        else:
            self.engine = create_engine(engine_url)

        self.models = main_models()
        self.table = None

    def __repr__(self) -> str:
        return f'Коннектор SQL с базой {self.engine.dialect.name}:{self.table.name if self.table is not None else "schema"}'

    def select_table(self, table_name: str):
        if self.table is None:
            self.table: Table = self.models[table_name]
        return self

    def _construct_a_query(self,
                           table,
                           query: Union[Dict[str, Any], None] = None,
                           columns: Union[List[str], None] = None):
        """

        :param table:
        :type table:
        :param query:
        :type query:
        :param columns:
        :type columns:
        :return:
        :rtype:
        """
        if table is not None:
            query_stmt = select()
            if not columns:
                if query:
                    for param, value in query.items():
                        conditions = []
                        if isinstance(value, list):
                            conditions.append(getattr(table.c, param).in_(value))
                        else:
                            conditions.append(getattr(table.c, param) == value)
                        return query_stmt.select_from(table).where(*conditions)
                else:
                    return select(table)
            else:
                for col in columns:
                    if hasattr(self.table.c, col):
                        query_stmt = query_stmt.add_columns(getattr(self.table.c, col))
                if query:
                    for param, value in query.items():
                        conditions = []
                        if isinstance(value, list):
                            conditions.append(getattr(table.c, param).in_(value))
                        else:
                            conditions.append(getattr(table.c, param) == value)
                        return query_stmt.select_from(table).where(*conditions)
                else:
                    return query_stmt
        else:
            raise ValueError('Не выбрана таблица!')

    def insert_pandas(self,
                      dataframe: DataFrame,
                      table: Table = None,
                      if_exists: str = 'append'):
        """

        :param dataframe:
        :type dataframe:
        :param table:
        :type table:
        :param flag:
        :type flag:
        :return:
        :rtype:
        """
        def _check_columns(df: DataFrame):
            """

            :param df:
            :type df:
            :return:
            :rtype:
            """
            col_df = df.columns.tolist().sort()
            sql_table_cols = [col.name for col in self.table.columns].sort()
            if col_df != sql_table_cols:
                raise ValueError(f"Столбцы{df.__repr__()} "
                                 f"не совпадают со столбцами таблицы {self.table.name}")
            else:
                return True

        if _check_columns(dataframe):

            if table is None:
                table = self.table

            with self.engine.connect() as conn:
                if len(dataframe) <= 1000:
                    dataframe.to_sql(table.name, conn, if_exists=if_exists, method='multi', index=False)
                    conn.commit()
                else:
                    dataframe.to_sql(table.name, conn, if_exists=if_exists, method='multi', index=False, chunksize=1000)
                    conn.commit()

    def load_data(self,
                  query: Union[Dict, None] = None,
                  table: Table = None,
                  columns: Union[List[str], None] = None) -> List[Optional[Row]]:
        """

        :param query:
        :type query:
        :param table:
        :type table:
        :param columns:
        :type columns:
        :return:
        :rtype:
        """
        if table is None:
            table = self.table
        with self.engine.connect() as conn:
            result = conn.execute(self._construct_a_query(table, query, columns))
            rows = [row for row in result]
            return rows

    def insert_data(self,
                    data: Dict,
                    table: Table = None) -> Union[bool, None]:
        """

        :param data: Словарь с данными для записи в БД.
        :type data: Dict. Пример словаря: {param: value, param1: value1, param2: value2, ...}
        :param table: Таблица для записи.
        :type table: sqlalchemy.schema.Table
        :return: Union[Bool, None]
        """
        if table is None:
            table = self.table

        insert_query = table.insert().values(data)

        with self.engine.connect() as connection:
            try:
                connection.execute(insert_query)
                return True
            except Exception as e:
                logging.error(e)
                return False
            finally:
                connection.commit()

    def raw_query(self, stmt: Union[str, Join, Select, Insert]) -> Optional[List[Optional[Row]]]:
        with self.engine.connect() as connection:
            if isinstance(stmt, str):
                if 'SELECT' in stmt:
                    result = connection.execute(text(stmt))
                    rows = [row for row in result]
                    return rows
                else:
                    try:
                        connection.execute(text(stmt))
                        connection.commit()
                    except Exception as e:
                        raise
                    except sqlalchemy.exc.IntegrityError:
                        print('Дубликат!')

            elif isinstance(stmt, Join) or isinstance(stmt, Select):
                result = connection.execute(stmt)
                rows = [row for row in result]
                return rows

            elif isinstance(stmt, Insert):
                try:
                    connection.execute(stmt)
                    connection.commit()
                except Exception as e:
                    raise
                except sqlalchemy.exc.IntegrityError:
                    print('Дубликат!')

    def load_pandas(self, query: Union[str, Join, Select, Insert]) -> pd.DataFrame:
        if isinstance(query, str):
            with self.engine.connect() as connection:
                df = pd.read_sql(sql=text(query), con=connection)
                return df
        elif isinstance(query, Join) or isinstance(query, Select) or isinstance(query):
            df = pd.DataFrame(data=self.raw_query(query))
            return df

    def get_engine(self):
        return self.engine
    
    def create_table(self, table_name: str, fields: Dict[str, str]):
        if table_name not in self.models.keys():
            query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {','.join([f'{key} {value}' for key, value in fields.items()])}
            )
            """
            with self.engine.begin() as conn:
                conn.execute(text(query))
        else:
            raise AttributeError('Данная таблица уже существует.')        
        

if __name__ == "__main__":
    sql_resource = SQLengine