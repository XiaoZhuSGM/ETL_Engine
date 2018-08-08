from sqlalchemy import create_engine, inspect, func, select, table
from etl.models.ext_table_info import ExtTableInfo
from etl.dao.dao import session_scope
from etl.models.datasource import ExtDatasource
from etl.service.datasource import DatasourceService


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

        columns = {column['name']: str(column['type']) for column in columns_list}
        columns.update({'autoincrement': flag})
        return columns

    def _get_record_num(self, tab):
        rows = select([func.count()]).select_from(table(tab))
        rows = self.conn.execute(rows).fetchall()
        record_num = rows[0][0]
        return record_num

    @staticmethod
    def _get_table_from_pgsql(**kwargs):
        source_id = kwargs.get('source_id')
        table_name = kwargs.get('table_name')
        table_info = ExtTableInfo.query.filter(ExtTableInfo.source_id == source_id, ExtTableInfo.table_name == table_name).all()
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
            'port': datasource.port
        }
        return data_dict

    def download_table_once(self, **data):
        source_id = data.get('source_id')
        is_dbs = data.get('is_dbs')
        db_name = data.get('database')
        schema = data.get('schema')

        res = self.connect_test(**data)
        if res:
            return

        tables = self._get_tables(schema)
        for table in tables:
            table_name = db_name + '.' + table if is_dbs else table
            try:
                ext_pri_key = self._get_ext_pri_key(table, schema)
                ext_column = self._get_ext_column(table, ext_pri_key, schema)
                record_num = self._get_record_num(table)
            except Exception as e:
                continue
            table_info = self._get_table_from_pgsql(source_id=source_id, table_name=table_name)

            weight = 0 if record_num == 0 else 2

            if len(table_info) == 0:
                try:
                    self._create_ext_table(
                        source_id=source_id,
                        table_name=table_name,
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
                self._update_ext_table(
                    table_info,
                    ext_pri_key=ext_pri_key,
                    ext_column=ext_column,
                    weight=weight
                    )
            except Exception:
                pass

        self.conn.close()

    def download_tables(self, app, **data):
        with app.app_context():
            source_id = data.get('source_id')
            self._update_status(source_id, 'running')

            try:
                db_name = data.get('db_name', [])
                if len(db_name) > 1:
                    data['is_dbs'] = True

                for db_dict in db_name:
                    database = db_dict.get('database')
                    data['database'] = database

                    schema_list = db_dict.get('schema')
                    if not schema_list:
                        self.download_table_once(**data)
                        continue
                    for scheam in schema_list:
                        data['schema'] = scheam
                        self.download_table_once(**data)

            # 如果任务异常结束，保证状态能及时更新
            except Exception as e:
                print(repr(e))
                self._update_status(source_id, 'fail')

            self._update_status(source_id, 'success')

    def get_status(self, source_id):
        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        status = datasource.table_structure if datasource else None
        return status

    @session_scope
    def _update_status(self, source_id, status):
        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()
        datasource.table_structure = status

    @session_scope
    def set_all_fail(self):
        datasource_service = DatasourceService()
        datasource_models = datasource_service.find_all()
        for datasource in datasource_models:
            if datasource.table_structure == 'running':
                datasource.table_structure = 'fail'
