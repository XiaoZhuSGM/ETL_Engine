import json
import pytz
import time
import boto3
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from datetime import datetime
from enum import Enum


_TZINFO = pytz.timezone('Asia/Shanghai')

S3_BUCKET = 'ext-etl-data'

SQL_PREFIX = 'sql/source_id={source_id}/{date}/'
HISTORY_HUMP_JSON = 'datapipeline/source_id={source_id}/ext_date={date}/history_dump_json/' \
                    'dump={timestamp}.json'

FULL_JSON = 'data/source_id={source_id}/ext_date={date}/' \
            'dump={date}_whole_path.json'

S3_CLIENT = boto3.resource('s3')
LAMBDA_CLIENT = boto3.client('lambda')


class Method(Enum):
    full = 1
    sync = 2
    increment = 3


def handler(event):
    # Check if the incoming message was sent by SNS
    if 'Records' in event:
        message = json.loads(event['Records'][0]['Sns']['Message'])
    else:
        message = event
    ext_db_worker = ExtDBWork(message)
    response = json.dumps(ext_db_worker.extract_data())
    print(response)
    if response and ext_db_worker.task_type != Method.sync.name:
        json_key = HISTORY_HUMP_JSON.format(source_id=ext_db_worker.source_id, date=ext_db_worker.query_date,
                                            timestamp=now_timestamp())
        print(json_key)
        S3_CLIENT.Object(bucket_name=S3_BUCKET, key=json_key).put(
            Body=response)

        if ext_db_worker.task_type == Method.full.name:
            """full json 每天只会有一份，每次增量后的话都是更新，同步更新后放到data目录"""
            full_json_ley = FULL_JSON.format(source_id=ext_db_worker.source_id, date=ext_db_worker.query_date)
            S3_CLIENT.Object(bucket_name=S3_BUCKET, key=full_json_ley).put(
                Body=response)
    return response


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp


class ExtDBWork(object):

    def __init__(self, message):
        self.source_id = message.get("source_id", None)
        self.query_date = message.get("query_date", None)
        self.task_type = message.get("task_type", None)  # full ,sync, increment
        self.filename = message.get("filename", None)
        self.db_url = message.get("db_url")

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
        return:
        """
        key = SQL_PREFIX.format(source_id=self.source_id, date=self.query_date) + self.filename
        print(key)
        extract_sqls = S3_CLIENT.Object(S3_BUCKET, key).get()['Body'].read().decode('utf-8')
        return json.loads(extract_sqls)

    def extract_data(self):
        if self.source_id is None:
            return "source_id不能为空"

        sql_info = self.sqls()

        if self.query_date != sql_info["query_date"]:
            return "抓取日期和文件日期不一致"

        _type = sql_info["type"]
        futures = []
        with ThreadPoolExecutor() as executor:
            for sql in sql_info['sqls']:
                for table_name, sql_statement in sql.items():
                    for value in sql_statement:
                        future = executor.submit(self.thread_query_tables, (table_name, value), _type)
                        futures.append(future)
                        time.sleep(1)

        response = dict(source_id=self.source_id, query_date=self.query_date, task_type=self.task_type)
        results = [f.result() for f in futures]
        extracted_data_list = [r for r in results if r is not None]

        if len(sql_info["sqls"]) == len(extracted_data_list):
            print("OK")

        extracted_data = list()
        sign_has_record_in_extracted_data_table_name = list()
        for data_road in extracted_data_list:
            (table, road), = data_road.items()
            if table not in sign_has_record_in_extracted_data_table_name:
                temp = defaultdict(list)
                temp["table"] = table
                temp["records"].append(road)
                extracted_data.append(temp)
                sign_has_record_in_extracted_data_table_name.append(table)
            else:
                for target_dict in extracted_data:
                    if target_dict["table"] == table:
                        target_dict["records"].append(road)
                        break
                    continue

        response["extract_data"] = extracted_data

        return response

    def thread_query_tables(self, sql, _type):
        msg = dict(source_id=self.source_id, sql=sql, type=_type, db_url=self.db_url, query_date=self.query_date)
        from executor_sql import handler
        payload = handler(msg)
        # invoke_response = LAMBDA_CLIENT.invoke(
        #     FunctionName="executor_sql", InvocationType='RequestResponse',
        #     Payload=json.dumps(msg), Qualifier='prod')
        # payload = invoke_response.get('Payload')
        # payload_str = payload.read()
        # payload = json.loads(payload_str)
        status = payload.get('status', None)
        if status and status == 'OK':
            result = payload.get('result')
            return result
        elif status and status == 'error':
            trace = payload.get('trace')
            print(trace)
        return None


if __name__ == '__main__':
    start = time.time()
    event = dict(source_id="54YYYYYYYYYYYYY", query_date="2018-08-06", task_type="full", filename="1.json",
                 db_url="mssql+pymssql://cm:cmdata!2017@172.31.0.18:40054/hbposev9")
    handler(event)
    print('spend time: ', time.time() - start)
