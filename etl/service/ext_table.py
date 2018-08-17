from sqlalchemy import create_engine, inspect, func, select, table
from etl.dao.dao import session_scope
from etl.models.datasource import ExtDatasource
from etl.models.ext_table_info import ExtTableInfo
from etl.service.datasource import DatasourceService
from sqlalchemy.exc import SQLAlchemyError


class ExtTableService(object):

    def __init__(self):
        self.engine = None
        self.inspector = None
        self.conn = None
        self.schema = None
        self.db_schema = None

    def connect_test(self, **kwargs):

        db_type = kwargs.get('db_type')
        db_name = kwargs.get('database')
        username = kwargs.get('username')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port')

        if not all([db_type, db_name, username, password, host, port]):
            return "DB_url parameter is missing"

        if db_type == 'sqlserver':
            db_type = 'mssql+pymssql'

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

    def _get_tables(self, schema):
        tables = self.inspector.get_table_names(schema=schema)
        return tables

    def _get_ext_pri_key(self, table, schema):
        res = self.inspector.get_pk_constraint(table, schema=schema)
        pk_list = res.get('constrained_columns')
        pk = ','.join(pk_list) if pk_list else ''
        return pk

    def _get_ext_column(self, table, ext_pri_key, schema):
        flag = 0
        columns_list = self.inspector.get_columns(table, schema=schema)

        # 判断主键是否是单主键自增
        if ext_pri_key != '' and ',' not in ext_pri_key:
            for col in columns_list:
                if col['name'] == ext_pri_key and col['autoincrement'] is True:
                    flag = 1

        columns = {column['name']: repr(column['type']).replace(", collation='Chinese_PRC_CI_AS'", "")
                   for column in columns_list}
        columns.update({'autoincrement': flag})
        return columns

    def _get_record_num(self, tab):
        rows = """select count(*) from {table}""".format(table=tab)
        rows = self.conn.execute(rows).fetchall()
        record_num = rows[0][0]
        return record_num

    def _get_views(self, schema):
        res = self.inspector.get_view_names(schema=schema)
        views = [f'v_{view}' for view in res]
        return views

    @staticmethod
    def _get_table_from_pgsql(**kwargs):
        source_id = kwargs.get('source_id')
        table_name = kwargs.get('table_name')
        table_info = ExtTableInfo.query.filter(ExtTableInfo.source_id == source_id,
                                               ExtTableInfo.table_name == table_name).all()
        return table_info

    @session_scope
    def _update_ext_table(self, table_info, **kwargs):
        table_info.update(**kwargs)

    @session_scope
    def _create_ext_table(self, **kwargs):
        ExtTableInfo.create(**kwargs)

    @staticmethod
    def get_datasource_by_source_id(source_id):
        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        if datasource is None:
            return None

        data_dict = {
            'source_id': datasource.source_id,
            'db_type': datasource.db_type,
            'db_name': datasource.db_name,
            'username': datasource.username,
            'password': datasource.password,
            'host': datasource.host,
            'port': datasource.port,
            'erp_vendor': datasource.erp_vendor
        }
        return data_dict

    def download_table_once(self, data):
        source_id = data.get('source_id')
        schema = data.get('schema')
        erp_vendor = data.get('erp_vendor')

        schema_new = None
        if erp_vendor == "百年创纪云":
            schema_new = f"[{schema}]"

        tables = self._get_tables(schema)
        views = self._get_views(schema)
        tables.extend(views)
        for table in tables:
            try:
                if table.startswith('v_'):
                    table = table.replace('v_', '', 1)
                    table_name = f'{schema}.{table}' if schema else table
                    ext_pri_key = ''
                    record_num = 0
                else:
                    ext_pri_key = self._get_ext_pri_key(table, schema)

                    if schema_new:
                        schema = schema_new
                    table_name = f'{schema}.{table}' if schema else table
                    record_num = self._get_record_num(table_name)
                    print(table_name)
                ext_column = self._get_ext_column(table, ext_pri_key, schema)
                print(table, record_num)
            except SQLAlchemyError as e:
                print(e)
                continue
            table_info = self._get_table_from_pgsql(source_id=source_id, table_name=table_name)

            if len(table_info) == 0:
                weight = 0 if record_num == 0 else 2

                try:
                    self._create_ext_table(
                        source_id=source_id,
                        table_name=table_name,
                        ext_pri_key=ext_pri_key,
                        ext_column=ext_column,
                        record_num=record_num,
                        weight=weight
                    )
                except SQLAlchemyError as e:
                    print(e)
                continue

            table_info = table_info[0]

            if table_info.ext_pri_key == ext_pri_key and \
                    table_info.ext_column == ext_column and \
                    table_info.record_num == record_num:
                continue
            try:
                self._update_ext_table(
                    table_info,
                    ext_pri_key=ext_pri_key,
                    ext_column=ext_column,
                    record_num=record_num
                )
            except SQLAlchemyError as e:
                print(e)

    def download_tables(self, app, **data):
        with app.app_context():
            source_id = data.get('source_id')
            self._update_status(source_id, 'running')

            db_name = data.get("db_name")
            schema_list = db_name.get('schema')

            try:
                if not schema_list:
                    self.download_table_once(data)
                else:
                    for scheam in schema_list:
                        data['schema'] = scheam
                        self.download_table_once(data)

                self._update_status(source_id, 'success')
            except Exception as e:
                print(str(e))
                self._update_status(source_id, 'fail')

        self.conn.close()

    def get_status(self, source_id):
        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        status = datasource.fetch_status if datasource else None
        return status

    @session_scope
    def _update_status(self, source_id, status):
        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        datasource.fetch_status = status

    @session_scope
    def set_all_fail(self):
        datasource_service = DatasourceService()
        datasource_models = datasource_service.find_all()
        for datasource in datasource_models:
            if datasource.fetch_status == 'running':
                datasource.fetch_status = 'fail'
