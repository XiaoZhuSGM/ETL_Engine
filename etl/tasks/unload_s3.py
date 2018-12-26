import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import wraps
from typing import List, Optional

import boto3
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.config import config

REDSHIFT_URL = config['prod'].SQLALCHEMY_DATABASE_URI

GOODSFLOW_SQL_TEMPLATE = """
unload ('select * from goodsflow_{source_id} where saletime >= \\'{start_date}\\' and saletime < \\'{end_date}\\'')
to 's3://standard-data/{year}/{month}/{day}/{source_id}/goodsflow/goodsflow_'
iam_role 'arn:aws-cn:iam::244434823928:role/redshift_to_s3'
PARALLEL off
DELIMITER ','
ESCAPE
GZIP;
"""

COST_SQL_TEMPLATE = """
unload ('select * from cost_{source_id} where date=\\'{date}\\'')
to 's3://standard-data/{year}/{month}/{day}/{source_id}/cost/cost_'
iam_role 'arn:aws-cn:iam::244434823928:role/redshift_to_s3'
PARALLEL off
DELIMITER ','
ESCAPE
GZIP;
"""

INVENTORY_SQL_TEMPLATE = """
unload ('select * from inventory_{source_id} where date=\\'{date}\\'')
to 's3://standard-data/{year}/{month}/{day}/{source_id}/inventory/inventory_'
iam_role 'arn:aws-cn:iam::244434823928:role/redshift_to_s3'
PARALLEL off
DELIMITER ','
ESCAPE
GZIP;
"""

CHAIN_GOODS_SQL_TEMPLATE = """
unload ('select * from chain_goods where cmid={cmid}')
to 's3://standard-data/{year}/{month}/{day}/{source_id}/chain_goods/chain_goods'
iam_role 'arn:aws-cn:iam::244434823928:role/redshift_to_s3'
PARALLEL off
DELIMITER ','
ESCAPE
ADDQUOTES
GZIP;
"""

CHAIN_STORE_SQL_TEMPLATE = """
unload ('select * from chain_store where cmid={cmid}')
to 's3://standard-data/{year}/{month}/{day}/{source_id}/chain_store/chain_store'
iam_role 'arn:aws-cn:iam::244434823928:role/redshift_to_s3'
PARALLEL off
DELIMITER ','
ESCAPE
ADDQUOTES
GZIP;
"""

CHAIN_CATEGORY_SQL_TEMPLATE = """
unload ('select * from chain_category where cmid={cmid}')
to 's3://standard-data/{year}/{month}/{day}/{source_id}/chain_category/chain_category'
iam_role 'arn:aws-cn:iam::244434823928:role/redshift_to_s3'
PARALLEL off
DELIMITER ','
ESCAPE
ADDQUOTES
GZIP;
"""


goodsflow_keys = "{year}/{month}/{day}/{source_id}/goodsflow/"
cost_keys = "{year}/{month}/{day}/{source_id}/cost/"
inventory_keys = "{year}/{month}/{day}/{source_id}/inventory/"

chain_store_keys = "{year}/{month}/{day}/{source_id}/chain_store/"
chain_goods_keys = "{year}/{month}/{day}/{source_id}/chain_goods/"
chain_category_keys = "{year}/{month}/{day}/{source_id}/chain_category/"


class UploadS3(object):
    def __init__(self, source_id: str) -> None:
        self.engine = create_engine(
            REDSHIFT_URL, echo=False, pool_size=20, max_overflow=40
        )
        self.source_id = source_id
        self.cmid = source_id.split("Y", 1)[0]
        self.bucket = "standard-data"
        self.s3_client = boto3.client("s3")
        self.s3 = boto3.resource("s3")
        self.Session = sessionmaker(bind=self.engine)

    def delete_keys(self, date: str, table: str) -> None:
        year, month, day = [x for x in date.split("-")]
        prefix = f"{year}/{month}/{day}/{self.source_id}/{table}/"
        response = self.s3_client.list_objects(
            Bucket=self.bucket, Delimiter="/", Prefix=prefix
        )
        key_dict = dict()
        if "Contents" in response:
            key_dict["Objects"] = [dict(Key=obj["Key"]) for obj in response["Contents"]]
            self.s3_client.delete_objects(Bucket=self.bucket, Delete=key_dict)

    def load_goodsflow_frame(self, date: str) -> str:
        start_date = datetime.strptime(date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=1)
        year, month, day = date.split("-")
        GOODSFLOW_SQL = GOODSFLOW_SQL_TEMPLATE.format(
            start_date=start_date.strftime("%Y-%m-%d 00:00:00"),
            end_date=end_date.strftime("%Y-%m-%d 00:00:00"),
            source_id=self.source_id,
            year=year,
            month=month,
            day=day,
        )
        return GOODSFLOW_SQL

    def load_cost_frame(self, date: str) -> str:
        year, month, day = date.split("-")
        COST_SQL = COST_SQL_TEMPLATE.format(
            source_id=self.source_id, date=date, year=year, month=month, day=day
        )
        return COST_SQL

    def load_chain_goods_frame(self, date: str) -> pd.DataFrame:
        year, month, day = date.split("-")
        CHAIN_GOODS_SQL = CHAIN_GOODS_SQL_TEMPLATE.format(
            cmid=self.cmid, year=year, month=month, day=day, source_id=self.source_id
        )
        return CHAIN_GOODS_SQL

    def loop(self, dates: List["str"], table: str) -> bool:
        method_name = f"load_{table}_frame"
        for date in dates:
            if table == "inventory":
                temp_date = (
                    datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
            else:
                temp_date = date

            print(f"start\t{table}\t{date}")
            self.delete_keys(date, table)
            sql = getattr(self, method_name)(temp_date)
            redshift_session = self.Session()
            redshift_session.execute(sql)
            print(f"end\t{table}\t{date}")

        return True

    def get_all_date(self, start: str, end: Optional[str] = None) -> list:
        end = end if end else start
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")
        delta = end_date - start_date

        return [
            (start_date + timedelta(days=x)).strftime("%Y-%m-%d")
            for x in range(delta.days + 1)
        ]

