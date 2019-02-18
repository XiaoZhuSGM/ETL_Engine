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

SQL_PREFIX = "sql/source_id={source_id}/{date}/"
HISTORY_HUMP_JSON = (
    "datapipeline/source_id={source_id}/ext_date={date}/table={table_name}/hour={hour}/history_dump_json/"
    "dump={timestamp}.json"
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

def handler(event,context):
    if "Records" in event:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        print(message)
    else:
        message = event
    ext_inv_work = ExtInvWork(message)
    response = json.dumps(ext_inv_work.extract_data())
    print(response)
    if response and ext_inv_work.task_type != Method.sync.name:
        json_key = HISTORY_HUMP_JSON.format(
            source_id =ext_inv_work.source_id,
            date=ext_inv_work.inv_date,
            timestamp=now_timestamp(),
            hour=ext_inv_work.inv_hour,
            table_name=list(ext_inv_work.inv_sqls.keys())[0]
        )
        print(json_key)
        S3_CLIENT.Object(bucket_name=S3_BUCKET, key=json_key).put(Body=response)

        if ext_inv_work.task_type == Method.full.name:
            """full json 每天只会有一份，每次增量后的话都是更新，同步更新后放到data目录"""
            full_json_key = FULL_JSON.format(
                source_id=ext_inv_work.source_id, date=ext_inv_work.query_date
            )
            S3_CLIENT.Object(bucket_name=S3_BUCKET, key=full_json_key).put(
                Body=response
            )
    return response


class ExtInvWork(object):
    def __init__(self,message):
        self.source_id = message.get("source_id", None)
        self.task_type = message.get("task_type", None)  # full ,sync, increment
        self.db_url = message.get("db_url")
        self.inv_date = datetime.now().strftime('%Y-%m-%d')
        self.inv_hour = datetime.now().hour
        self.inv_sqls = message.get('inv_sqls', None)

    def inv_sqls_info(self):
        #####从s3 获取json文件,目前还没有修改，需要手写
        inv_sqls = {
            "type":"inventory",
            "source_id":self.source_id,
            "query_date":self.inv_date,
            "sqls": self.inv_sqls
        }
        return inv_sqls

    def extract_data(self):
        if self.source_id is None:
            return "source_id不能为空"
        inv_sqls_info = self.inv_sqls_info()
        _type = inv_sqls_info["type"]

        futures = []
        _type = inv_sqls_info['type']
        with ThreadPoolExecutor() as executor:
            for table_name,sql_statement in inv_sqls_info["sqls"].items():
                for value in sql_statement:
                    future = executor.submit(
                        self.thread_query_tables,(table_name,value),_type
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
        extracted_data_list = [r for r in result if r is not None]
        extracted_data = defaultdict(list)
        for data_road in extracted_data_list:
            (table, road), = data_road.items()
            extracted_data[table].append(road)
        print(extracted_data)
        response["extract_data"] = extracted_data
        return response


    def thread_query_tables(self,sql,_type):
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
            return result
        elif status and status == "error":
            trace = payload.get('trace')
            print(trace)
        return None






if __name__ == '__main__':
    source_id = '43YYYYYYYYYYYYY'
    db_url = requests.get(f"http://172.31.16.24:50010/etl/admin/api/datasource/dburl/{source_id}",
                 headers={"token":"AIRFLOW_REQUEST_TOKEN"},
                 ).json()['data']
    event = {
        "type": "full",
        "db_url":db_url,
        "source_id": source_id,
        "query_date":'',
        "sqls":{
            "table":['select * from ...',
                     'select * from ...']
        },
        "inv_sqls": {
            "actinvs": ["""SELECT * FROM (SELECT RPT.*, ROWNUM RN FROM (SELECT * FROM actinvs order by gdgid,store,
                        qty desc ) RPT WHERE  ROWNUM <= 300000 )  temp_rpt WHERE RN > 0""",
                        """SELECT * FROM (SELECT RPT.*, ROWNUM RN FROM (SELECT * FROM actinvs order by gdgid,store,
                        qty desc ) RPT WHERE  ROWNUM <= 600000 )  temp_rpt WHERE RN > 300000""",
                        """SELECT * FROM (SELECT RPT.*, ROWNUM RN FROM (SELECT * FROM actinvs order by gdgid,store,
                        qty desc ) RPT WHERE  ROWNUM <= 900000 )  temp_rpt WHERE RN > 600000""",
                        """SELECT * FROM (SELECT RPT.*, ROWNUM RN FROM (SELECT * FROM actinvs order by gdgid,store,
                        qty desc ) RPT )  temp_rpt WHERE RN > 900000"""
                        ]
        }
    }
    # print(event)
    handler(event,None)
