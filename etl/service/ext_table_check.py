from sqlalchemy import create_engine, inspect

from etl.models import session_scope
from etl.models.datasource import ExtDatasource
from etl.models.etl_table import ExtCheckNum, ExtTestQuery


class ExtCheckTable(object):

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

    def execute_sql(self, sql, date, source_id):
        rows = self.conn.execute(sql).fetchall()
        record_num = rows[0][0]
        self.conn.close()
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

    def get_target_num(self, source_id, date):
        ext_test_query = ExtTestQuery.query.filter_by(source_id=source_id, target_table="cost").first()
        sql = ext_test_query.query_sql.format(date=date)

        # 检测对方数据库是否有数
        num = self.execute_sql(sql, date, source_id)
        return num

    @session_scope
    def create_check_num(self, info):
        ext_check_num = ExtCheckNum(**info)
        ext_check_num.save()

    @session_scope
    def modify_check_num(self, source_id, num, date):
        print(source_id, date, num)
        ext_check_num = ExtCheckNum.query.filter_by(source_id=source_id, date=date).first()
        ext_check_num.update(num=num)
