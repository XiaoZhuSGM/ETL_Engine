from sqlalchemy import create_engine, inspect
from etl.dao.dao import session_scope
from etl.models.datasource import ExtDatasource
from etl.models.ext_table_info import ExtTableInfo
from etl.service.datasource import DatasourceService
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import arrow
from threading import Thread
from flask import current_app, request


class ExtTaskExists(Exception):
    def __str__(self):
        return "任务已经在运行了，请不要重复点击"


class ExtDatasourceParaMiss(Exception):
    def __str__(self):
        return f"数据库配置不完整, 请检查"


class ExtDatasourceConnError(Exception):
    def __init__(self, errmsg):
        self.errmsg = errmsg

    def __str__(self):
        return f"数据库连接错误,错误信息:{self.errmsg}"


class ExtTableService(object):
    @staticmethod
    def connect_test(kwargs):
        db_type = kwargs.get('db_type')
        db_name = kwargs.get("db_name").get('database') if kwargs.get("db_name") else None
        username = kwargs.get('username')
        password = kwargs.get('password')
        host = kwargs.get('host')
        port = kwargs.get('port')

        if not all([db_type, db_name, username, password, host, port]):
            raise ExtDatasourceParaMiss()

        if db_type == 'sqlserver':
            db_type = 'mssql+pymssql'
        elif db_type == 'postgresql':
            db_type += r'+psycopg2'
        elif db_type == 'oracle':
            db_type += r'+cx_oracle'
            db_name = r'?service_name=' + db_name

        db_url = f"{db_type}://{username}:{password}@{host}:{port}/{db_name}"
        try:
            engine = create_engine(db_url)
            inspector = inspect(engine)
            with engine.connect():
                pass
        except OperationalError as e:
            raise ExtDatasourceConnError(e)

        return engine, inspector

    @staticmethod
    def _get_tables(inspector, schema):
        tables = inspector.get_table_names(schema=schema)
        res = inspector.get_view_names(schema=schema)
        views = [f'v_{view}' for view in res]
        return tables + views

    @staticmethod
    def _get_ext_pri_key(inspector, table, schema):
        res = inspector.get_pk_constraint(table, schema=schema)
        pk_list = res.get('constrained_columns')
        pk = ','.join(pk_list) if pk_list else ''
        return pk

    @staticmethod
    def _get_ext_column(inspector, table, ext_pri_key, schema):
        flag = 0
        columns_list = inspector.get_columns(table, schema=schema)
        # 判断主键是否是单主键自增
        if ext_pri_key != '' and ',' not in ext_pri_key:
            for col in columns_list:
                if col['name'] == ext_pri_key and col['autoincrement'] is True:
                    flag = 1

        columns = {column['name'].lower(): repr(column['type']).replace(", collation='Chinese_PRC_CI_AS'", "")
                   for column in columns_list}
        columns.update({'autoincrement': flag})
        return columns

    @staticmethod
    def _get_record_num(table, engine):
        with engine.connect() as conn:
            rows = conn.execute(f"select count(*) from {table}").fetchall()
            record_num = rows[0][0]
        return record_num

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

    @session_scope
    def create_table_info(self, source_id, table_name, ext_pri_key, ext_column, record_num):
        result = (
            ExtTableInfo.query
            .filter_by(source_id=source_id, table_name=table_name)
            .update(dict(ext_pri_key=ext_pri_key, ext_column=ext_column, record_num=record_num))
        )
        if result == 0:
            weight = 2 if record_num > 0 else 1
            ExtTableInfo.create(
                source_id=source_id, table_name=table_name, ext_pri_key=ext_pri_key,
                ext_column=ext_column, record_num=record_num, weight=weight
            )

    def _download_one_table(self, table, schema, special_schema, source_id, engine, inspector, app):
        with app.app_context():
            if table.startswith('v_'):
                table, ext_pri_key = table.replace("v_", "", 1), ""
            else:
                ext_pri_key = self._get_ext_pri_key(inspector, table, schema)

            table_name = f"{special_schema}.{table}" if special_schema else f"{schema}.{table}" if schema else table

            try:
                executor = ThreadPoolExecutor()
                future = executor.submit(self._get_record_num, table_name, engine)
                record_num = future.result(5)
                executor.shutdown(wait=False)
            except (TimeoutError, SQLAlchemyError) as e:
                print(str(e))
                record_num = 0

            ext_column = self._get_ext_column(inspector, table, ext_pri_key, schema)

            self.create_table_info(source_id, table_name, ext_pri_key, ext_column, record_num)

    def _download_one_schema(self, schema, engine, inspector, data, app):
        source_id = data.get('source_id')
        erp_vendor = data.get('erp_vendor')

        special_schema = f"[{schema}]" if schema and (erp_vendor == "百年创纪云" or erp_vendor == "百年新世纪") else None

        tables = self._get_tables(inspector, schema)
        with ThreadPoolExecutor(5) as executor:
            for table in tables:
                executor.submit(
                    self._download_one_table, table, schema, special_schema, source_id, engine, inspector, app
                )

    def _download_table(self, app, engine, inspector, **data):
        with app.app_context():
            source_id = data.get("source_id")
            self._update_status(data.get("source_id"), "running")

            schema_list = data.get("db_name").get("schema") or [None]
            try:
                for schema in schema_list:
                    self._download_one_schema(schema, engine, inspector, data, app)

                self._update_status(source_id, "success")
            except Exception as e:
                print(str(e))
                self._update_status(source_id, "fail")

    def generate_download_table(self, source_id):
        # 测试数据库是否能够正常连接，无法连接就返回错误信息
        data = self.get_datasource_by_source_id(source_id)

        # 如果任务已经在运行，就返回
        if self.get_status(source_id) == "running":
            raise ExtTaskExists()

        # 尝试连接数据，如果错误就在函数内直接报错

        engine, inspector = self.connect_test(data)

        task = (
            Thread(
                target=self._download_table,
                args=(current_app._get_current_object(), engine, inspector),
                kwargs=data
            )
        )

        task.start()

    @staticmethod
    def get_status(source_id):
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

    def download_special_tables(self):
        data = request.get_json()
        source_id = data.get("source_id")
        schema = data.get("schema")
        tables = data.get("tables").split(",")

        if not all([source_id, tables]):
            raise ExtDatasourceParaMiss()
        data = self.get_datasource_by_source_id(source_id)
        erp_vendor = data.get('erp_vendor')
        engine, inspector = self.connect_test(data)
        special_schema = f"[{schema}]" if schema and (erp_vendor == "百年创纪云" or erp_vendor == "百年新世纪") else None
        app = current_app._get_current_object()
        with ThreadPoolExecutor(5) as executor:
            for table in tables:
                executor.submit(
                    self._download_one_table, table, schema, special_schema, source_id, engine, inspector, app
                )

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
            print(f"etl db not exist {source_id}")
            return

        engine, insector = self.connect_test(data)
        ext_table = self._get_tables(insector, schema)

        for table in tables:
            current_table_name = table.format(date=current_month)
            print(source_id, schema, current_table_name)

            current_table = (
                ExtTableInfo.query
                .filter_by(source_id=source_id, weight=1, table_name=f"{schema}.{current_table_name}")
                .first()
            )
            if current_table:
                print(f"etl db exist {current_table.table_name}, break")
                continue

            if current_table_name in ext_table:
                print(f"source_id db exist {current_table_name}，insert to etl db")
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
                    print(f"{current_table_name} insert etl db success")

                ExtTableInfo.query\
                    .filter_by(source_id=source_id,
                               weight=1,
                               table_name=f"{schema}.{table.format(date=last_two_month)}")\
                    .update({"weight": 2})
