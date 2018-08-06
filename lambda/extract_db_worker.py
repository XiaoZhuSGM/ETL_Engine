# -*- coding: utf-8 -*-
import json
import pytz
import time
import boto3
import threading
from queue import Queue

_tzinfo = pytz.timezone('Asia/Shanghai')

S3_BUCKET = 'ext-etl-data'

SQL_PREFIX = 'sql/source_id={source_id}/{date}/'

S3_CLIENT = boto3.resource('s3')
LAMBDA_CLIENT = boto3.resource('lambda')


def handler(event):
    # Check if the incoming message was sent by SNS
    if 'Records' in event:
        message = json.loads(event['Records'][0]['Sns']['Message'])
    else:
        message = event
    response = ext_db_worker = ExtDBWork(message)


class ExtDBWork(object):

    def __init__(self, message):
        self.source_id = message.get("source_id", None)
        self.query_date = message.get("query_date", None)
        self.task_type = message.get("task_type", None)  # full ,sync, increment
        self.filename = message.get("filename", None)
        self.db_url = message.get("db_url")

    @property
    def sqls(self):
        """
        example:
        {"sqls": [{"t_im_check_master": "SELECT * FROM t_im_check_master where oper_date >= '20180805' and oper_date < '20180806'"},
		        {"t_im_flow": "SELECT * FROM t_im_flow where oper_date >= '20180805' and oper_date < '20180806'"},
		        {"t_rm_saleflow": "SELECT * FROM t_rm_saleflow where oper_date >= '20180805' and oper_date < '20180806'"},
		        {"t_da_jxc_daysum": "SELECT * FROM t_da_jxc_daysum where oper_date >= '20180805' and oper_date < '20180806'"},
		        {"t_im_check_sum": "SELECT a.* FROM t_im_check_sum a left join t_im_check_master b on a.sheet_no=b.check_no where oper_date >= '20180805' and oper_date < '20180806'"}],
	     "source_id": "54YYYYYYYYYYYYY",
	     "query_date": "2018-08-05"
	     "type":"full"}
        :return:
        """
        key = SQL_PREFIX.format(source_id=self.source_id, date=self.query_date) + self.filename
        extract_sqls = S3_CLIENT.Object(S3_BUCKET, key).get()['Body'].read().decode('utf-8')
        return extract_sqls

    def extract_data(self):
        if self.source_id is None:
            return "source_id不能为空"

        if self.query_date != self.sqls["query_date"]:
            return "抓取日期和文件日期不一致"

        q = Queue()
        threads = []
        _type = self.sqls["type"]
        for sql in self.sqls["sql"]:
            thread_query = threading.Thread(target=self.thread_query_tables, args=(sql, q, _type))
            thread_query.start()
            time.sleep(0.4)
            threads.append(thread_query)

        for thread in threads:
            thread.join()

        response = dict(source_id=self.source_id, query_date=self.query_date, task_type=self.task_type)

        extracted_data_list = list(q.queue)

        return response

    def thread_query_tables(self, sql, q, _type):
        sql = tuple(sql)
        msg = dict(source_id=self.source_id, sql=sql, type=_type, db_url=self.db_url)
        from .executor_sql import my_function
        payload = my_function(msg)
        # invoke_response = LAMBDA_CLIENT.invoke(
        #     FunctionName="executor_sql", InvocationType='RequestResponse',
        #     Payload=json.dumps(msg), Qualifier='prod')
        # payload = invoke_response.get('Payload')
        # payload_str = payload.read()
        # payload = json.loads(payload_str)
        status = payload.get('status', None)
        if status and status == 'OK':
            result = payload.get('result')
            q.put_nowait(result)
        elif status and status == 'error':
            trace = payload.get('trace')
            print(trace)
