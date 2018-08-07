# -*- coding: utf-8 -*-
import tempfile
import boto3
import json
import pytz
import time
import traceback
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from datetime import datetime, timedelta
from enum import Enum

S3_BUCKET = "ext-etl-data"
S3_RECORDS = 'datapipeline/source_id={source_id}/ext_date={date}/table={ext_table}/' \
             'dump={timestamp}&rowcount={rowcount}.csv.gz'

S3_WHOLE_RECORDS = 'data/source_id={source_id}/ext_date={date}/table={ext_table}/' \
                   'dump={timestamp}&rowcount={rowcount}.csv.gz'

_TZINFO = pytz.timezone('Asia/Shanghai')
S3 = boto3.resource("s3")


class Method(Enum):
    full = 1
    sync = 2
    increment = 3


def handler(event):
    # Check if the incoming message was sent by SNS
    if 'Records' in event:
        message = json.loads(event['Records'][0]['Sns']['Message'])
        print(message)
    else:
        message = event

    source_id = message["source_id"]
    table, sql = message["sql"]
    db_url = message["db_url"]
    _type = message["type"]
    query_date = message["query_date"]
    engine = create_engine(db_url, echo=False, poolclass=NullPool)
    try:
        sql_data_frame = pd.read_sql(sql, engine)

        if _type.lower() == Method.sync.name:
            return

        key, full_key = upload_to_s3(source_id, table, _type, query_date, sql_data_frame)

        response = dict(status="OK", result=dict())
        response["result"][table] = key
        return response
    except Exception as e:
        return {'status': 'error', 'trace': str(traceback.format_exc())}
    finally:
        engine.dispose()


def upload_to_s3(source_id, table, _type, query_date, frame):
    filename = tempfile.NamedTemporaryFile(mode="w", encoding='utf-8')
    count = len(frame)
    frame.to_csv(filename.name, index=False, compression="gzip")
    filename.seek(0)

    key = S3_RECORDS.format(source_id=source_id, ext_table=table, date=query_date, timestamp=now_timestamp(),
                            rowcount=count)
    # S3.Object(bucket_name=S3_BUCKET, key=key).put(Body=filename)
    S3.Bucket(S3_BUCKET).upload_file(filename.name, key)
    if _type == Method.full.name:
        copy_source = {
            'Bucket': S3_BUCKET,
            'Key': key
        }
        full_key = S3_WHOLE_RECORDS.format(source_id=source_id, ext_table=table, date=query_date,
                                           timestamp=now_timestamp(),
                                           rowcount=count)
        S3.Bucket(S3_BUCKET).copy(copy_source, full_key)
        return key, full_key
    return key, None


def get_ymd():
    yesterday = datetime.now(tz=_TZINFO) - timedelta(1)
    return yesterday.strftime('%Y-%m-%d')


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp


# {'t_im_flow': "SELECT * FROM t_im_flow where oper_date >= '20180805' and oper_date < '20180806'"}
if __name__ == '__main__':
    event = {"source_id": "54YYYYYYYYYYYYY", "sql": (
        "t_im_flow", "SELECT * FROM t_im_flow where oper_date >= '20180805' and oper_date < '20180806'"),
             "type": "full", "db_url": "mssql+pymssql://cm:cmdata!2017@172.31.0.18:40054/hbposev9",
             "query_date": "2018-08-05"}

    handler(event)
