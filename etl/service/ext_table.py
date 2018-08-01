from sqlalchemy import create_engine, inspect, func, select, table
from etl.models.ext_table_info import ExtTableInfo
from etl.dao.dao import session_scope
from etl.models.datasource import ExtDatasource
import json


class ExtTableService(object):

    def __init__(self):
        self.engine = None
        self.inspector = None
        self.conn = None
        self.schema = None

    def connect_test(self, **kwargs):

        self.db_schema = kwargs.get('schema')
        db_type = kwargs.get('db_type')
        db_name = kwargs.get('database')
        username = kwargs.get('username')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port')

        if not all([db_type, db_name, username, password, host, port]):
            return "Parameter is missing"

        if db_type == 'mssql':
            db_type += r'+pymssql'
        elif db_type == 'postgresql':
            db_type += r'+psycopg2'
        elif db_type == 'oracle':
            db_type += r'+cx_oracle'
            db_name = r'?service_name=' + db_name

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
            return repr(e)

    def __get_tables(self):
        tables = self.inspector.get_table_names(schema=self.db_schema)
        return tables

    def __get_ext_pri_key(self, table):
        res = self.inspector.get_pk_constraint(table, self.db_schema)
        pk_list = res.get('constrained_columns')
        pk = ','.join(pk_list) if pk_list else ''
        return pk

    def __get_ext_column(self, table):
        columns_list = self.inspector.get_columns(table, self.db_schema)
        columns = {column['name']: str(column['type']) for column in columns_list}
        columns = json.dumps(columns)
        return columns

    def __get_record_num(self, tab):
        rows = select([func.count()]).select_from(table(tab))
        rows = self.conn.execute(rows).fetchall()
        record_num = rows[0][0]
        return record_num

    def __get_table_from_pgsql(self, **kwargs):
        cmid = kwargs.get('cmid')
        table_name = kwargs.get('table_name')
        table_info = ExtTableInfo.query.filter(ExtTableInfo.cmid == cmid, ExtTableInfo.table_name == table_name).all()
        return table_info

    @session_scope
    def __update_ext_table(self, table_info, **kwargs):
        table_info.update(**kwargs)

    @session_scope
    def __create_ext_table(self, **kwargs):
        ExtTableInfo.create(**kwargs)

    def get_datasource(self, source_id):
        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        data_dict = {
            'cmid': datasource.cmid,
            'db_type': datasource.db_type,
            'db_name': datasource.db_name,
            'username': datasource.username,
            'password': datasource.password,
            'host': datasource.host,
            'port': datasource.port
        }
        return data_dict

    def download_table(self, **data):

        cmid = data.get('cmid')

        res = self.__connect_test(**data)
        if res:
            return
        tables = self.__get_tables()

        for table in tables:
            try:
                ext_pri_key = self.__get_ext_pri_key(table)
                ext_column = self.__get_ext_column(table)
                record_num = self.__get_record_num(table)
            except Exception as e:
                continue
            table_info = self.__get_table_from_pgsql(cmid=cmid, table_name=table)
            weight = 0 if record_num == 0 else 2

            if len(table_info) == 0:
                try:
                    self.__create_ext_table(
                        cmid=cmid,
                        table_name=table,
                        ext_pri_key=ext_pri_key,
                        ext_column=ext_column,
                        record_num=record_num,
                        weight=weight
                        )
                except Exception as e:
                    pass

                continue

            table_info = table_info[0]

            if table_info.ext_pri_key == ext_pri_key and \
                    table_info.ext_column == ext_column and \
                    table_info.record_num == record_num:
                continue
            try:
                self.__update_ext_table(
                    table_info,
                    ext_pri_key=ext_pri_key,
                    ext_column=ext_column,
                    weight=weight
                    )
            except Exception as e:
                pass



