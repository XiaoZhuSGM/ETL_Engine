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
    sync_column = "cmid"
    date_column = "date"

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
        query = f"DELETE FROM {self.target_table} WHERE {where}"
        print(query)
        print(f"{self.cmid}:<{self.target_table}> 删除旧数据：{r.rowcount}")

    def _copy(self):
        r = self.conn.execute(
            (
                f"COPY {self.target_table} FROM '{self.data_source}' "
                f"IAM_ROLE 'arn:aws-cn:iam::244434823928:role/RedshiftCopyFromS3' "
                f"CSV GZIP IGNOREHEADER 1"
            )
        )
        print(f"{self.cmid}:<{self.target_table}> 入库{self.date_column}新数据：{r.rowcount}")

    def run(self, warehouse_type=""):
        with self.conn:
            with self.conn.begin():
                if warehouse_type == "copy":
                    print("copy!")
                    self._delete_old_data()
                    self._copy()


if __name__ == "__main__":
    event = {
        # 'redshift_url': 'postgresql+psycopg2://cmdata:EDSvuSb!L9@172.31.0.231:5439/cmdata_new',
        "redshift_url": "postgresql+psycopg2://cmdata_offline_dev:8o4PemVCK9Cfcdw5zUXC@172.16.1.116:5439/cmdata_offline",
        'target_table': 'inventory_34yyyyyyyyyyyyy',
        'data_key': 'ext-etl-data/clean_data/source_id=34YYYYYYYYYYYYY/clean_date=2019-02-22/target_table=inventory/hour=10/dump=2019-02-22 10:20:17.679624+08:00&rowcount=748538.csv.gz',
        'data_date': '2019-02-22', 'warehouse_type': 'copy', 'source_id': '34YYYYYYYYYYYYY', 'cmid': '34'
    }

    begin = time.time()
    handler(event, None)
    print(time.time() - begin)
