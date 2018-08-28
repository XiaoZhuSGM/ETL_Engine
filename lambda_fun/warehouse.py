import json
import time
from sqlalchemy import create_engine
from datetime import timedelta, datetime


BASIC_TABLE = {"store", "goods", "category"}


def handler(event, context):
    if "Records" in event:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
    else:
        message = event

    redshift_url = message["redshift_url"]
    data_key = message["data_key"]
    data_date = message["data_date"]
    target_table = message["target_table"]
    sync_column = message["sync_column"]
    date_column = message["date_column"]
    warehouse_type = message["warehouse_type"]

    warehouser = Warehouser(
        redshift_url, target_table, data_key, sync_column, data_date, date_column
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
    ) -> None:
        self.engine = create_engine(db_url)
        self.conn = self.engine.connect()
        self.target_table = target_table
        self.data_source = f"s3://{data_key}"
        self.sync_column = sync_column
        self.data_date = data_date
        self.date_column = date_column

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
        where = " AND ".join(
            f"{self.target_table}.{sc} = #{self.target_table}.{sc}"
            for sc in self.sync_column
        )
        if where:
            r = self.conn.execute(
                f"DELETE FROM {self.target_table} USING #{self.target_table} WHERE {where}"
            )
            print(f"删除已存在的数据：{r.rowcount}")
        r = self.conn.execute(
            f"INSERT INTO {self.target_table} SELECT * FROM #{self.target_table}"
        )
        print(f"插入数据：{r.rowcount}")

    def _delete_old_data(self):
        next_day = (
            datetime.strptime(self.data_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        where = f"{self.date_column}::date >= '{self.data_date}' AND {self.date_column}::date < '{next_day}'"
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
        "data_key": "ext-etl-data/clean_data/source_id=43YYYYYYYYYYYYY/clean_date=2018-08-23/target_table=goodsflow/dump=2018-08-27 17:44:14.026780+08:00&rowcount=135430.csv.gz",
        "target_table": "goodsflow",
        "data_date": "2018-08-23",
        "sync_column": ["foreign_item_id"],
        "date_column": "saletime",
        "warehouse_type": "copy",
    }
    begin = time.time()
    handler(event, None)
    print(time.time() - begin)
