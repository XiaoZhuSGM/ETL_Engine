# -*- coding: utf-8 -*-
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import requests
from enum import Enum

import boto3
import botocore
import pytz
from botocore.config import Config

_TZINFO = pytz.timezone("Asia/Shanghai")

S3_BUCKET = "ext-etl-data"

INV_SQL_PREFIX = "sql/source_id={source_id}/{date}/inventory/"
INV_HISTORY_HUMP_JSON = (
    "inventory/source_id={source_id}/ext_date={date}/history_dump_json/{hour}/dump={timestamp}.json"
)
FULL_JSON = "data/source_id={source_id}/ext_date={date}/" "dump={date}_whole_path.json"

S3_CLIENT = boto3.resource("s3")
session = botocore.session.get_session()
BOTO3_CONFIG = Config(connect_timeout=300, read_timeout=300)
LAMBDA_CLIENT = session.create_client("lambda", config=BOTO3_CONFIG)


class Method(Enum):
    full = 1
    sync = 2
    increment = 3


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp


def handler(event, context):
    if "Records" in event:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        print(message)
    else:
        message = event
    ext_inv_work = ExtInvWork(message)
    response = json.dumps(ext_inv_work.extract_data())
    if response and ext_inv_work.task_type != Method.sync.name:
        json_key = INV_HISTORY_HUMP_JSON.format(
            source_id=ext_inv_work.source_id,
            date=ext_inv_work.inv_date,
            timestamp=now_timestamp(),
            hour=ext_inv_work.inv_hour
        )
        S3_CLIENT.Object(bucket_name=S3_BUCKET, key=json_key).put(Body=response)

        if ext_inv_work.task_type == Method.full.name:
            """full json 每天只会有一份，每次增量后的话都是更新，同步更新后放到data目录"""
            full_json_key = FULL_JSON.format(
                source_id=ext_inv_work.source_id, date=ext_inv_work.inv_date
            )
            S3_CLIENT.Object(bucket_name=S3_BUCKET, key=full_json_key).put(
                Body=response
            )
    return response


class ExtInvWork(object):
    def __init__(self, message):
        self.source_id = message.get("source_id", None)
        self.task_type = message.get("task_type", None)  # full ,sync, increment
        self.db_url = message.get("db_url", None)
        self.inv_date = message.get('query_date', None)
        self.filename = message.get('filename', None)
        self.inv_hour = datetime.now().hour
        assert self.inv_date == datetime.now().strftime('%Y-%m-%d'), "库存日期不是当天"

    def inv_sqls_info(self):
        #####从s3 获取json文件,目前还没有修改，需要手写
        key = (
                INV_SQL_PREFIX.format(source_id=self.source_id, date=self.inv_date)
                + self.filename
        )
        inv_sqls = (
            S3_CLIENT.Object(S3_BUCKET, key).get()["Body"].read().decode("utf-8")
        )
        return json.loads(inv_sqls)

    def extract_data(self):
        if self.source_id is None:
            return "source_id不能为空"
        inv_sqls_info = self.inv_sqls_info()
        _type = inv_sqls_info["type"]
        futures = []
        with ThreadPoolExecutor() as executor:
            for table_name, sql_statement in inv_sqls_info["inv_sqls"].items():
                for value in sql_statement:
                    print(value)
                    future = executor.submit(
                        self.thread_query_tables, (table_name, value), _type
                    )
                    futures.append(future)
                    time.sleep(1)

        result = [f.result() for f in futures]
        response = dict(
            source_id=self.source_id,
            query_date=self.inv_date,
            hour=self.inv_hour,
            task_type=self.task_type
        )
        extracted_data_list = [r[0] for r in result if r is not None]
        sucess_sql_list = [r[1] for r in result if r is not None]
        all_sqls_list = list(inv_sqls_info["inv_sqls"].values())[0]
        failure_sqls = list(set(all_sqls_list).difference(set(sucess_sql_list)))
        extracted_data = defaultdict(list)
        for data_road in extracted_data_list:
            (table, road), = data_road.items()
            extracted_data[table].append(road)
        response["extract_data"] = extracted_data
        response["failure_sqls"] = failure_sqls
        print(response)
        return response

    def thread_query_tables(self, sql, _type):
        msg = dict(
            source_id=self.source_id,
            sql=sql,
            type=_type,
            db_url=self.db_url,
            query_date=self.inv_date,
            hour=self.inv_hour
        )
        from executor_inv_sql import handler
        payload = handler(msg, None)
        status = payload.get("status", None)
        if status and status == "OK":
            result = payload.get("result")
            sql = payload.get("sql")
            return result, sql
        elif status and status == "error":
            trace = payload.get('trace')
            error_sql = payload.get('error_sql')
            print(error_sql, trace)
        return None


if __name__ == '__main__':
    source_id = '34YYYYYYYYYYYYY'
    db_url = requests.get(f"http://172.31.16.24:50010/etl/admin/api/datasource/dburl/{source_id}",
                          headers={"token": "AIRFLOW_REQUEST_TOKEN"},
                          ).json()['data']
    date = datetime.now().strftime('%Y-%m-%d')
    params = {'source_id': source_id, 'date': date}
    respones = requests.get('http://10.1.20.226:5000/etl/admin/api/inv/sql', params=params)
    res = json.loads(respones.text)
    filename = res['data']
    event = {
        "task_type": "full",
        "db_url": db_url,
        "source_id": source_id,
        "query_date": date,
        "filename": filename
    }
    print(event)
    handler(event, None)

    # params = {'source_id': source_id, 'date': '2019-01-01'}
    # response = requests.get('http://localhost:5000/etl/admin/api/inv/sql', params=params)
    # res = json.loads(response.text)
    # print(res)

    # invoke_response = LAMBDA_CLIENT.invoke(
    #     FunctionName="extract_inv_worker", InvocationType='RequestResponse',
    #     Payload=json.dumps(event))
    # payload = invoke_response.get('Payload')
    # payload_str = payload.read()
    # payload = json.loads(payload_str)
    # print(payload)
