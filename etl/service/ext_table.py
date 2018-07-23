from sqlalchemy import create_engine, inspect, func, select, table
from etl.models.ext_table_info import ExtTableInfo
from etl.dao.dao import session_scope
import json


class ExtTableService(object):

    def __init__(self):
        self.engine = None
        self.inspector = None
        self.conn = None

    def connect_test(self, **kwargs):
        db_type = kwargs.get('db_type')
        db_name = kwargs.get('db_name')
        username = kwargs.get('username')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port')

        if db_type == 'mssql':
            db_type += r'+pymssql'
        elif db_type == 'postgrssql':
            db_type += r'+psycopg2'
        elif db_type == 'oracle':
            db_type += r'+cx_oracle'
            db_name += r'?service_name='

        database_url = "{db_type}://{username}:{password}@{host}:{port}/{db_name}".format(
            db_type=db_type,
            username=username,
            password=password,
            host=host,
            port=port,
            db_name=db_name)
        try:
            self.engine = create_engine(database_url)
            self.inspector = inspect(self.engine)
            self.conn = self.engine.connect()
        except Exception as e:
            return e

    def get_tables(self):
        tables = self.inspector.get_table_names()
        return tables

    def get_ext_pri_key(self, table):
        res = self.inspector.get_pk_constraint(table)
        pk_list = res.get('constrained_columns')
        pk = ','.join(pk_list) if pk_list else ''
        return pk

    def get_ext_column(self, table):
        columns_list = self.inspector.get_columns(table)
        columns = {column['name']: str(column['type']) for column in columns_list}
        columns = json.dumps(columns)
        return columns

    def get_record_num(self, tab):
        rows = select([func.count()]).select_from(table(tab))
        rows = self.conn.execute(rows).fetchall()
        record_num = rows[0][0]
        return record_num

    def get_table_from_pgsql(self, **kwargs):
        cmid = kwargs.get('cmid')
        table_name = kwargs.get('table_name')
        table_info = ExtTableInfo.query.filter(ExtTableInfo.cmid == cmid, ExtTableInfo.table_name == table_name)
        return table_info

    @session_scope
    def update_ext_table(self, table_info, **kwargs):
        table_info.update(**kwargs)

    @session_scope
    def create_ext_table(self, **kwargs):
        ExtTableInfo.create(**kwargs)
