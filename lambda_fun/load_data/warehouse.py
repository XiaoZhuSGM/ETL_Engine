import json
import time
from sqlalchemy import create_engine
from datetime import timedelta, datetime
import boto3

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
    data_date = message["data_date"]
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
        redshift_url,
        target_table,
        data_key,
        sync_column,
        data_date,
        date_column,
        cmid,
        source_id,
    )
    warehouser.run(warehouse_type)


class Warehouser:
    def __init__(
        self,
        db_url: str,
        target_table: str,
        data_key: str,
        sync_column: list,
        data_date: str,
        date_column: str,
        cmid: str,
        source_id: str,
    ) -> None:
        self.engine = create_engine(db_url)
        self.conn = self.engine.connect()
        self.target_table = target_table
        self.data_source = f"s3://{data_key}"
        self.sync_column = sync_column
        self.data_date = data_date
        self.date_column = date_column
        self.cmid = cmid
        self.source_id = source_id

    def _copy_to_temporary_table(self):
        r = self.conn.execute(
            f"CREATE TABLE #{self.target_table} (LIKE {self.target_table})"
        )
        print(f"创建临时表：{r.rowcount}")
        r = self.conn.execute(
            (
                f"COPY #{self.target_table} FROM '{self.data_source}' "
                f"IAM_ROLE 'arn:aws-cn:iam::244434823928:role/RedshiftCopyFromS3' "
                "CSV GZIP IGNOREHEADER 1"
            )
        )
        print(f"拷贝到临时表：{r.rowcount}")

    def _upsert(self):
        if not self.sync_column:
            self.sync_column = ["cmid"]
        else:
            self.sync_column.append("cmid")

        where = " AND ".join(
            f"{self.target_table}.{sc} = #{self.target_table}.{sc}"
            for sc in self.sync_column
        )
        r = self.conn.execute(
            f"DELETE FROM {self.target_table} USING #{self.target_table} WHERE {where}"
        )
        print(f"删除已存在的数据：{r.rowcount}")

        r = self.conn.execute(
            f"INSERT INTO {self.target_table} SELECT * FROM #{self.target_table}"
        )
        print(f"插入数据：{r.rowcount}")

    def _delete_old_data(self):
        if not self.date_column:
            return

        next_day = (
            datetime.strptime(self.data_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        where = f"{self.date_column}::date >= '{self.data_date}' AND {self.date_column}::date < '{next_day}' AND cmid = {self.cmid}"
        r = self.conn.execute(f"DELETE FROM {self.target_table} WHERE {where}")
        print(f"删除旧数据 {self.data_date}：{r.rowcount}")

    def _copy(self):
        r = self.conn.execute(
            (
                f"COPY {self.target_table} FROM '{self.data_source}' "
                f"IAM_ROLE 'arn:aws-cn:iam::244434823928:role/RedshiftCopyFromS3' "
                f"CSV GZIP IGNOREHEADER 1"
            )
        )
        print(f"插入新数据：{r.rowcount}")

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
