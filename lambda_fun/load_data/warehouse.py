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
        print(f"{self.cmid}:<#{self.target_table}> 创建临时表: {r.rowcount}")
        r = self.conn.execute(
            (
                f"COPY #{self.target_table} FROM '{self.data_source}' "
                f"IAM_ROLE 'arn:aws-cn:iam::244434823928:role/RedshiftCopyFromS3' "
                "CSV GZIP IGNOREHEADER 1"
            )
        )
        print(f"{self.cmid}:<#{self.target_table}> 拷贝到临时表: {r.rowcount}")

    def _upsert(self):
        if self.sync_column:
            where = " AND ".join(
                f"{self.target_table}.{sc} = #{self.target_table}.{sc}"
                for sc in self.sync_column
            )
            r = self.conn.execute(
                f"DELETE FROM {self.target_table} USING #{self.target_table} WHERE {where}"
            )
            print(f"{self.cmid}:<{self.target_table}> 删除已存在的数据: {r.rowcount}")

        r = self.conn.execute(
            f"INSERT INTO {self.target_table} SELECT * FROM #{self.target_table}"
        )
        print(f"{self.cmid}:<{self.target_table}> 插入数据: {r.rowcount}")

    def _delete_old_data(self):
        if not self.date_column:  # 没有日期字段，不需要删除
            return
        dataframes = pd.read_csv(
            self.data_source,
            compression="gzip",
            converters={self.date_column: pd.to_datetime},
        )
        if not len(dataframes):  # 数据为空，不需要删除
            return
        dates = dataframes[self.date_column].dropna().drop_duplicates()
        dates_str = [d.strftime("%Y-%m-%d") for d in dates]
        dates_str = [f"'{d}'" for d in dates_str]
        where = f"{self.date_column}::date IN ({','.join(d for d in dates_str)}) AND cmid = {self.cmid}"
        r = self.conn.execute(f"DELETE FROM {self.target_table} WHERE {where}")
        print(f"{self.cmid}:<{self.target_table}> 删除旧数据：{r.rowcount}")

    def _copy(self):
        r = self.conn.execute(
            (
                f"COPY {self.target_table} FROM '{self.data_source}' "
                f"IAM_ROLE 'arn:aws-cn:iam::244434823928:role/RedshiftCopyFromS3' "
                f"CSV GZIP IGNOREHEADER 1"
            )
        )
        print(f"{self.cmid}:<{self.target_table}> 插入新数据：{r.rowcount}")

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
        "data_key": "ext-etl-data/clean_data/source_id=88YYYYYYYYYYYYY/clean_date=2018-11-04/target_table=goods/dump=2018-11-06 15:47:16.286396+08:00&rowcount=8904.csv.gz",
        "target_table": "chain_goods",
        "data_date": "2018-11-04",
        "warehouse_type": "upsert",
        "cmid": "88",
        "source_id": "88YYYYYYYYYYYYY",
    }
    
    event1 = {
***REMOVED***
        "target_table": "chain_sales_target",
        "warehouse_type": "copy",
        "cmid": "57",
        "data_date": "2018-09-10",
        "data_key": "ext-etl-data/clean_data/source_id=57YYYYYYYYYYYYY/clean_date=2018-09-10/target_table=sales_target/dump=2018-09-11 23:01:10.434793+08:00&rowcount=35.csv.gz",
        "source_id": "57YYYYYYYYYYYYY",
    }

    begin = time.time()
    handler(event, None)
    print(time.time() - begin)
