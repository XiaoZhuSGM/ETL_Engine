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
    data_date = message["data_date"]
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
    print(table_key)
    sync_column = target_tables[table_key]["sync_column"]
    date_column = target_tables[table_key]["date_column"]

    warehouser = Warehouser(
        redshift_url, target_table, data_key, sync_column, date_column, cmid, source_id, data_date
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
            data_date: str
    ) -> None:
        self.engine = create_engine(db_url)
        self.conn = self.engine.connect()
        self.target_table = target_table
        self.data_source = f"s3://{data_key}"
        self.sync_column = sync_column
        self.date_column = date_column
        self.cmid = cmid
        self.source_id = source_id
        self.data_date = data_date

    def _delete_old_data(self):
        if not self.date_column:  # 没有日期字段，不需要删除
            return
        print(self.target_table)
        data_date = f"'{self.data_date}'"
        query_inv = f"select count(1) from {self.target_table} where date ={data_date}"
        print(query_inv)
        result = self.conn.execute(query_inv).fetchall()[0][0]
        print(result)
        if not result:
            return  # 判断数据库里是否有当天数据
        else:
            where = f"{self.date_column}::date = {data_date} AND cmid = {self.cmid}"
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
        'target_table': 'inventory_34YYYYYYYYYYYYY',
        'data_key': 'ext-etl-data/clean_data/source_id=34YYYYYYYYYYYYY/clean_date=2019-02-26/target_table=inventory/hour=10/dump=2019-02-26 11:11:10.033368+08:00&rowcount=741982.csv.gz',
        'data_date': '2019-02-26', 'warehouse_type': 'copy', 'source_id': '34YYYYYYYYYYYYY', 'cmid': '34'
    }
    #
    begin = time.time()
    handler(event, None)
    print(time.time() - begin)


    # import boto3
    # from botocore.client import Config
    #
    # lam = boto3.client("lambda", config=Config(connect_timeout=910, read_timeout=910, retries=dict(max_attempts=0)))
    #
    #
    # def invoke_lambda(functionname, event):
    #     invoke_response = lam.invoke(
    #         FunctionName=functionname, InvocationType='RequestResponse', Payload=json.dumps(event), LogType='Tail'
    #     )
    #     return invoke_response
    #
    #
    # response = invoke_lambda('load_data_inv', event)
    # payload_body = response['Payload']
    # payload_str = payload_body.read()
    # response = json.loads(payload_str)
    # print(response)
