import json
import time
from sqlalchemy import create_engine
from datetime import timedelta
import boto3
import pandas as pd

S3_BUCKET = "ext-etl-data"
TARGET_TABLE_KEY = "target_table/target_table.json"
S3_CLIENT = boto3.resource("s3")


def handler(event, context):
    if "Records" in event:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
    else:
        message = event

    redshift_url = message["redshift_url"]
    data_key = message["data_key"]
    target_table = message["target_table"]
    warehouse_type = message["warehouse_type"]
    source_id = message["source_id"]
    cmid = message["cmid"]
    target_tables = json.loads(
        S3_CLIENT.Object(S3_BUCKET, TARGET_TABLE_KEY)
        .get()["Body"]
        .read()
        .decode("utf-8")
    )

    table_key = (
        target_table
        if not target_table.endswith(source_id)
        else target_table.split(f"_{source_id}")[0]
    )
    sync_column = target_tables[table_key]["sync_column"]
    date_column = target_tables[table_key]["date_column"]

    warehouser = Warehouser(
        redshift_url, target_table, data_key, sync_column, date_column, cmid, source_id
    )
    warehouser.run(warehouse_type)


class Warehouser:
    def __init__(
        self,
        db_url: str,
        target_table: str,
        data_key: str,
        sync_column: list,
        date_column: str,
        cmid: str,
        source_id: str,
    ) -> None:
        self.engine = create_engine(db_url)
        self.conn = self.engine.connect()
        self.target_table = target_table
        self.data_source = f"s3://{data_key}"
        self.sync_column = sync_column
        self.date_column = date_column
        self.cmid = cmid
        self.source_id = source_id

    def _copy_to_temporary_table(self):
        r = self.conn.execute(
            f"CREATE TABLE #{self.target_table} (LIKE {self.target_table})"
        )
        print(f"<#{self.target_table}> 创建临时表: {r.rowcount}")
        r = self.conn.execute(
            (
                f"COPY #{self.target_table} FROM '{self.data_source}' "
                f"IAM_ROLE 'arn:aws-cn:iam::244434823928:role/RedshiftCopyFromS3' "
                "CSV GZIP IGNOREHEADER 1"
            )
        )
        print(f"<#{self.target_table}> 拷贝到临时表: {r.rowcount}")

    def _upsert(self):
        if self.sync_column:
            where = " AND ".join(
                f"{self.target_table}.{sc} = #{self.target_table}.{sc}"
                for sc in self.sync_column
            )
            r = self.conn.execute(
                f"DELETE FROM {self.target_table} USING #{self.target_table} WHERE {where}"
            )
            print(f"<{self.target_table}> 删除已存在的数据: {r.rowcount}")

        r = self.conn.execute(
            f"INSERT INTO {self.target_table} SELECT * FROM #{self.target_table}"
        )
        print(f"<{self.target_table}> 插入数据: {r.rowcount}")

    def _delete_old_data(self):
        if not self.date_column:
            return
        dataframes = pd.read_csv(
            self.data_source,
            compression="gzip",
            converters={self.date_column: pd.to_datetime},
        )
        start_date = dataframes[self.date_column].min()
        end_date = dataframes[self.date_column].max() + timedelta(days=1)
        where = f"{self.date_column}::date >= '{start_date.strftime('%Y-%m-%d')}' AND {self.date_column}::date < '{end_date.strftime('%Y-%m-%d')}' AND cmid = {self.cmid}"
        r = self.conn.execute(f"DELETE FROM {self.target_table} WHERE {where}")
        print(f"<{self.target_table}> 删除旧数据 {self.data_date}：{r.rowcount}")

    def _copy(self):
        r = self.conn.execute(
            (
                f"COPY {self.target_table} FROM '{self.data_source}' "
                f"IAM_ROLE 'arn:aws-cn:iam::244434823928:role/RedshiftCopyFromS3' "
                f"CSV GZIP IGNOREHEADER 1"
            )
        )
        print(f"<{self.target_table}> 插入新数据：{r.rowcount}")

    def run(self, warehouse_type=""):
        with self.conn:
            with self.conn.begin():
                if warehouse_type == "copy":
                    print("copy!")
                    self._delete_old_data()
                    self._copy()
                elif warehouse_type == "upsert":
                    print("upsert!")
                    self._copy_to_temporary_table()
                    self._upsert()


if __name__ == "__main__":
    event = {
***REMOVED***
        "data_key": "ext-etl-data/clean_data/source_id=72YYYYYYYYYYYYY/clean_date=2018-09-05/target_table=goodsflow/dump=2018-09-06 11:51:55.981581+08:00&rowcount=4301.csv.gz",
        "target_table": "goodsflow_72YYYYYYYYYYYYY",
        "data_date": "2018-09-05",
        "warehouse_type": "copy",
        "cmid": "72",
        "source_id": "72YYYYYYYYYYYYY",
    }
    event1 = {
***REMOVED***
        "target_table": "chain_store",
        "warehouse_type": "upsert",
        "cmid": "72",
        "data_date": "2018-09-03",
        "data_key": "ext-etl-data/clean_data/source_id=72YYYYYYYYYYYYY/clean_date=2018-09-03/target_table=store/dump=2018-09-04 15:19:23.822474+08:00&rowcount=11.csv.gz",
    }

    "clean_data/source_id=43YYYYYYYYYYYYY/clean_date=2018-09-05/target_table=goodsflow/dump=2018-09-06 15:10:25.132712+08:00&rowcount=145796.csv.gz"

    event2 = {
        "redshift_url": "oracle+cx_oracle://hd40:ttblhd40@60.6.202.4:51521/?service_name=hdapp",
        "data_key": "ext-etl-data/clean_data/source_id=43YYYYYYYYYYYYY/clean_date=2018-09-05/target_table=goodsflow/dump=2018-09-06 15:10:25.132712+08:00&rowcount=145796.csv.gz",
        "target_table": "goodsflow_72YYYYYYYYYYYYY",
        "data_date": "2018-09-05",
        "warehouse_type": "copy",
        "cmid": "72",
        "source_id": "72YYYYYYYYYYYYY",
    }

    begin = time.time()
    handler(event, None)
    print(time.time() - begin)
