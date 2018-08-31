# -*- coding: utf-8 -*-
# @Time    : 2018/8/31 16:33
# @Author  : 范佳楠

import tempfile
import time
from datetime import datetime

import boto3
import pandas as pd
import pytz
from typing import Dict

S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"
_TZINFO = pytz.timezone("Asia/Shanghai")


class Base(object):

    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        self.source_id = source_id
        self.date = date
        self.cmid = self.source_id.split('Y', 1)[0]
        self.data = data

    def clean(self, target_table):
        method = getattr(self, target_table, None)
        if method and callable(method):
            df = getattr(self, target_table)()
            return self.up_load_to_s3(df, target_table)
        else:
            raise RuntimeError(f"没有这个表:{target_table}")

    def up_load_to_s3(self, dataframe, target_table):
        file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8')
        count = len(dataframe)
        dataframe.to_csv(file.name, index=False, compression='gzip', float_format='%.4f')
        file.seek(0)
        key = CLEANED_PATH.format(
            source_id=self.source_id,
            target_table=target_table,
            date=self.date,
            timestamp=datetime.fromtimestamp(time.time(), tz=_TZINFO),
            rowcount=count,
        )

        S3.Bucket(S3_BUCKET).upload_file(file.name, key)

        return key
