# -*- coding: utf-8 -*-
import os
import json
import tempfile
import time
import traceback
from datetime import datetime
from enum import Enum

import boto3
import pandas as pd
import pytz
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from csv import QUOTE_NONNUMERIC

S3_BUCKET = "ext-etl-data"

S3_RECORDS = (
    "datapipeline/source_id={source_id}/ext_date={date}/table={ext_table}/"
    "hour={hour}/dump={timestamp}&rowcount={rowcount}.csv.gz"
)

_TZINFO = pytz.timezone("Asia/Shanghai")
S3 = boto3.resource("s3")


class Method(Enum):
    full = 1
    sync = 2
    increment = 3


def handler(event, context):
    # Check if the incoming message was sent by SNS
    if "Records" in event:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        print(message)
    else:
        message = event

    source_id = message["source_id"]
    table, sql = message["sql"]
    db_url = message["db_url"]
    _type = message["type"]
    query_date = message["query_date"]
    hour = message["hour"]
    os.environ["NLS_LANG"] = "SIMPLIFIED CHINESE_CHINA.UTF8"
    try:
        engine = create_engine(db_url, echo=False, poolclass=NullPool)
        sql_data_frame = pd.read_sql(text(sql), engine)
        sql_data_frame.columns = [c.lower() for c in sql_data_frame.columns]

        key = upload_to_s3(source_id, table, _type, query_date, hour, sql_data_frame)
        response = dict(status="OK", sql=sql, result=dict())
        response["result"][table] = key
        return response
    except Exception as e:
        return {"status": "error", "error_sql": sql, "trace": str(traceback.format_exc())}
    finally:
        engine.dispose()


def upload_to_s3(source_id, table, _type, query_date, hour, frame):
    filename = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
    count = len(frame)
    frame.to_csv(filename.name, index=False, compression="gzip", quoting=QUOTE_NONNUMERIC)
    filename.seek(0)

    if _type == Method.sync.name:
        key = SYNC_RECORDS
    else:
        key = S3_RECORDS

    key = key.format(
        source_id=source_id,
        ext_table=table,
        date=query_date,
        hour=hour,
        timestamp=now_timestamp(),
        rowcount=count,
    )
    print(key)
    S3.Bucket(S3_BUCKET).upload_file(filename.name, key)
    return key


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp


# {'t_im_flow': "SELECT * FROM t_im_flow where oper_date >= '20180805' and oper_date < '20180806'"}
if __name__ == "__main__":
    event = {
        "source_id": "73YYYYYYYYYYYYY",
        "sql": [
            "t_sk_master",
            "select t_sk_master_01.* from t_sk_master_01 "
        ],
        "type": "full",
        "db_url": "mssql+pymssql://chaomeng:CHAOMENG@172.31.0.18:40073/fjwjdb",
        "query_date": "2019-02-27",
        "hour": 13
    }

    handler(event, None)
