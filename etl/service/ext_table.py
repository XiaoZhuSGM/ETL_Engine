from sqlalchemy import create_engine, inspect
from etl.dao.dao import session_scope
from etl.models.datasource import ExtDatasource
from etl.models.ext_table_info import ExtTableInfo
from etl.service.datasource import DatasourceService
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import arrow


class ExtTableService(object):

    def __init__(self):
        self.engine = None
        self.inspector = None
        self.conn = None
        self.schema = None
        self.db_schema = None

    def connect_test(self, kwargs):

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

        columns = {column['name'].lower(): repr(column['type']).replace(", collation='Chinese_PRC_CI_AS'", "")
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

    def get_datasource_by_source_id(self, source_id):
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

        special_schema = None
        if schema and (erp_vendor == "百年创纪云" or erp_vendor == "百年新世纪"):
            special_schema = f"[{schema}]"

        tables = self._get_tables(schema)
        views = self._get_views(schema)
        tables.extend(views)
        for table in tables:
            print(table)
            table_name = f'{special_schema}.{table}' if special_schema else f'{schema}.{table}' if schema else table

            try:
                if table.startswith('v_'):
                    table = table.replace('v_', '', 1)
                    ext_pri_key = ''
                    record_num = 0
                else:
                    ext_pri_key = self._get_ext_pri_key(table, schema)

                    executor = ThreadPoolExecutor()
                    future = executor.submit(self._get_record_num, table_name)
                    try:
                        record_num = future.result(15)
                    except TimeoutError:
                        record_num = 0
                    finally:
                        executor.shutdown(wait=False)
                ext_column = self._get_ext_column(table, ext_pri_key, schema)
                print(table_name, record_num, len(ext_column))
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

    @session_scope
    def download_tables(self, app, **data):
        with app.app_context():
            source_id = data.get('source_id')
            self._update_status(source_id, 'running')

            db_name = data.get("db_name")
            schema_list = db_name.get('schema')
            try:
                if not schema_list:
                    print("start download table")
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

    @session_scope
    def download_special_tables(self, source_id, schema, tables, data):
        erp_vendor = data.get("erp_vendor")
        special_schema = None
        if schema and (erp_vendor == "百年创纪云" or erp_vendor == "百年新世纪"):
            special_schema = f"[{schema}]"
        for table in tables:
            table_name = f'{special_schema}.{table}' if special_schema else f'{schema}.{table}' if schema else table
            try:
                ext_pri_key = self._get_ext_pri_key(table, schema)

                executor = ThreadPoolExecutor()
                future = executor.submit(self._get_record_num, table_name)
                try:
                    record_num = future.result(15)
                except TimeoutError:
                    record_num = 0
                finally:
                    executor.shutdown(wait=False)
                ext_column = self._get_ext_column(table, ext_pri_key, schema)
                print(table_name, record_num, len(ext_column))
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
            else:
                table = table_info[0]
                try:
                    self._update_ext_table(
                        table,
                        ext_pri_key=ext_pri_key,
                        ext_column=ext_column,
                        record_num=record_num
                    )
                except SQLAlchemyError as e:
                    print(e)

    def download_about_date_table(self, app):
        """
        获取和时间相关的表，并存储到ext_table_info里
        :return:
        """
        DATE_TABLE = [
            ["1015YYYYYYYYYYY", "dbusrsdms", ["sale_j{date}", "salecost{date}", "pay_j{date}"]],
            ["48YYYYYYYYYYYYY", "dbo", ["GoodsSale{date}", "Item{date}"]],
            ["52YYYYYYYYYYYYY", "hscmp", ["tsalpludetail{date}", "tsalsale{date}", "tsalsaleplu{date}"]],
            ["55YYYYYYYYYYYYY", "hscmp", ["tsalpludetail{date}", "tsalsale{date}", "tsalsaleplu{date}"]],
            ["64YYYYYYYYYYYYY", "hscmp", ["tsalpludetail{date}", "tsalsale{date}", "tsalsaleplu{date}"]],
        ]
        current_month = arrow.now().format("YYYYMM")
        last_one_month = arrow.now().shift(months=-1).format("YYYYMM")
        last_two_month = arrow.now().shift(months=-2).format("YYYYMM")
        with app.app_context():
            for table_list in DATE_TABLE:
                self.insert_one_source_id(table_list, current_month, last_one_month, last_two_month)

    @session_scope
    def insert_one_source_id(self, table_list, current_month, last_one_month, last_two_month):
        # 查询对方是否存在对应的表

        source_id, schema, tables = table_list
        data = self.get_datasource_by_source_id(source_id)
        if not data:
            print(f"服务器没有{source_id}")
            return
        data['database'] = data["db_name"]["database"]
        if self.connect_test(data):
            return
        ext_table = self._get_tables(schema)
        self.conn.close()
        for table in tables:
            current_table_name = table.format(date=current_month)
            print(source_id, schema, current_table_name)

            current_table = (
                ExtTableInfo.query
                .filter_by(source_id=source_id, weight=1, table_name=f"{schema}.{current_table_name}")
                .first()
            )
            if current_table:
                print(f"服务器已存在{current_table.table_name}，跳过")
                continue

            if current_table_name in ext_table:
                print(f"客户数据库已生成{current_table_name}，准备插入")
                # 获取相似表的配置，存储到ext_table_info里，并将2月前的表配置为不抓
                last_one_table = (
                    ExtTableInfo.query
                    .filter_by(source_id=source_id, weight=1,
                               table_name=f"{schema}.{table.format(date=last_one_month)}")
                    .first()
                )
                if last_one_table:
                    info = last_one_table.to_dict()
                    del info["id"], info["created_at"], info["updated_at"]

                    info["table_name"] = f"{schema}.{current_table_name}"
                    ExtTableInfo.create(**info)
                    print(f"{current_table_name}插入完成")

                ExtTableInfo.query\
                    .filter_by(source_id=source_id,
                               weight=1,
                               table_name=f"{schema}.{table.format(date=last_two_month)}")\
                    .update({"weight": 2})
