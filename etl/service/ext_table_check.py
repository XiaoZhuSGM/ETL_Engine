import datetime
import decimal
import json

import arrow
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

    def alchemyencoder(self, obj):
        """JSON encoder function for SQLAlchemy special classes."""
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, decimal.Decimal):
            return float(obj)

    def execute_sql(self, sql):
        try:
            rows = self.conn.execute(sql)
        except Exception:
            return "error"
        rows = rows.fetchall()
        record_num = json.dumps([dict(r) for r in rows], default=self.alchemyencoder)
        record_num = json.loads(record_num)

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

    def get_target_num(self, source_id, target_table, date):
        ext_test_query = ExtTestQuery.query.filter_by(source_id=source_id, target_table=target_table).first()
        if ext_test_query:
            sql = ext_test_query.query_sql
            if "{yyyymm}" in sql:
                yyyy = arrow.now().format('YYYY')
                mm = arrow.now().format('MM')
                dd = arrow.now().format('DD')
                if dd == '01':
                    mm = arrow.now().shift(months=-1).format('MM')
                    if mm == '12':
                        yyyy = arrow.now().shift(years=-1).format('YYYY')
                yyyymm = yyyy + mm
                sql = sql.format(yyyymm=yyyymm, date=date)
            else:
                sql = sql.format(date=date)
            num = self.execute_sql(sql)
            return num
        else:
            return None

    @session_scope
    def create_check_num(self, source_id, target_table, num, date):
        ext_check_num = ExtCheckNum.query.filter_by(source_id=source_id, date=date, target_table=target_table).first()
        if ext_check_num:
            ext_check_num.update(num=num)
        else:
            ExtCheckNum(source_id=source_id, num=num, date=date, target_table=target_table).save()

    @session_scope
    def create_test_query(self, source_id, sql, target_table):
        ext_test_query = ExtTestQuery.query.filter_by(source_id=source_id, target_table='cost').first()
        if ext_test_query:
            ext_test_query.update(query_sql=sql)
        else:
            ExtTestQuery(source_id=source_id, query_sql=sql, target_table=target_table).save()
