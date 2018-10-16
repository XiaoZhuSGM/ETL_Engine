from etl.tasks.config import huey
from etl.service.ext_sql import DatasourceSqlService
from config.config import config
from etl.etl import create_app, db
from etl.service.datasource import DatasourceService
from etl.service.ext_clean_info import ExtCleanInfoService
import os
import boto3
from etl.tasks.common import lambda_invoker
from etl.service.ext_log import ExtLogSqlService
import time
from datetime import datetime
import re
from etl.tasks.tasks import task_warehouse, task_extract_data
import json
from traceback import print_exc
from flask import current_app


sql_service = DatasourceSqlService()
datasource_service = DatasourceService()
cleaninfo_service = ExtCleanInfoService()
log_service = ExtLogSqlService()

setting = config.get(os.getenv("ETL_ENVIREMENT", "dev"))
S3 = boto3.resource("s3")
S3_BUCKET = "ext-etl-data"


class RollbackError(Exception):
    def __init__(self, content: dict) -> None:
        log_service.add_log(**content)
        self.content = content

    def __str__(self):
        return str(self.content)


class RollbackTaskSet:
    upsert_tables = {"chain_store", "chain_goods", "chain_category"}

    def __init__(
        self, source_id: str, date: str, erp_name: str, target_list: list
    ) -> None:
        self.source_id = source_id
        self.date = date
        self.erp_name = erp_name
        self.target_list = target_list
        self.cmid = self.source_id.split("Y", 1)[0]
        self.app = create_app(setting)

    def record_log(self, content):
        content["start_time"] = datetime.fromtimestamp(content["start_time"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        content["end_time"] = datetime.fromtimestamp(content["end_time"]).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        content.update(
            {"extract_date": self.date, "source_id": self.source_id, "cmid": self.cmid}
        )
        log_service.add_log(**content)

    def extract_api(self, event):
        task = task_extract_data(
            self.source_id,
            self.date,
            event["task_type"],
            event["filename"],
            event["db_url"],
        )
        try:
            task(blocking=True)
        except Exception as e:
            print_exc()
            return False
        else:
            return True

    def warehouse_api(self, event):
        target_tables = json.load(
            S3.Object(S3_BUCKET, "target_table/target_table.json").get()["Body"]
        )
        table_key = (
            event["target_table"]
            if not event["target_table"].endswith(self.source_id)
            else event["target_table"].split(f"_{self.source_id}")[0]
        )
        sync_column = target_tables[table_key]["sync_column"]
        date_column = target_tables[table_key]["date_column"]
        task = task_warehouse(
            event["redshift_url"],
            event["target_table"],
            event["data_key"],
            sync_column,
            date_column,
            self.cmid,
            self.source_id,
            event["warehouse_type"],
        )
        try:
            task(blocking=True)
        except Exception as e:
            print_exc()
            return False
        else:
            return True

    @property
    def sql_file(self):
        return sql_service.generate_full_sql(self.source_id, self.date)

    def extract_data(self):
        start_time = time.time()
        event = datasource_service.generator_extract_event(self.source_id)
        event["query_date"] = self.date
        event["filename"] = self.sql_file
        res = lambda_invoker("extract_db_worker", event, qualifier="$LATEST")
        end_time = time.time()
        payload = res["Payload"]
        task_status = {
            "start_time": start_time,
            "end_time": end_time,
            "cost_time": int(end_time - start_time),
            "task_type": 13,
        }
        if "FunctionError" in res:
            print("抓数 lambda 失败，调用抓数任务")
            res = self.extract_api(event)
            if not res:
                task_status.update({"result": 0, "remark": "[回滚]抓数失败"})
                self.record_log(task_status)
                raise RuntimeError(f"抓数失败：{payload}")

        task_status.update({"result": 1, "remark": "[回滚]抓数成功"})
        self.record_log(task_status)
        print(payload)
        return payload

    def clean_data(self, target):
        start_time = time.time()
        cleaninfo = cleaninfo_service.get_ext_clean_info_target(self.source_id, target)
        event = {
            "source_id": self.source_id,
            "erp_name": self.erp_name,
            "date": self.date,
            "target_table": target.split("chain_")[-1],
            "origin_table_columns": {
                key.lower(): value for key, value in cleaninfo["origin_table"].items()
            },
            "converts": {
                key.lower(): value for key, value in cleaninfo["covert_str"].items()
            },
        }
        res = lambda_invoker("lambda_clean_data", event, qualifier="$LATEST")
        end_time = time.time()
        payload = res["Payload"]
        task_status = {
            "start_time": start_time,
            "end_time": end_time,
            "cost_time": int(end_time - start_time),
            "table_name": target,
            "task_type": 23,
        }
        if "FunctionError" in res:
            task_status.update({"result": 0, "remark": "[回滚]清洗失败"})
        else:
            match = re.match(".*rowcount=(\d*).*", payload).group(1)
            record_num = int(match)
            task_status.update(
                {"result": 1, "remark": "[回滚]清洗成功", "record_num": record_num}
            )
        self.record_log(task_status)
        if task_status["result"] == 0:
            raise RuntimeError(f"{target}清洗失败：{payload}")
        return payload

    def warehouse_data(self, target, cleaned_file):
        start_time = time.time()
        event = {
            "redshift_url": current_app.config["REDSHIFT_URL"],
            "cmid": self.cmid,
            "data_date": self.date,
            "data_key": f"{S3_BUCKET}/{cleaned_file}",
            "source_id": self.source_id,
            "target_table": f"{target}_{self.source_id}"
            if target not in self.upsert_tables
            else target,
            "warehouse_type": "copy" if target not in self.upsert_tables else "upsert",
        }
        res = lambda_invoker("etl-warehouse", event, qualifier="$LATEST")
        end_time = time.time()
        payload = res["Payload"]
        task_status = {
            "start_time": start_time,
            "end_time": end_time,
            "cost_time": int(end_time - start_time),
            "table_name": target,
            "task_type": 33,
        }
        if "FunctionError" in res:
            print("入库 lambda 失败，调用入库任务")
            res = self.warehouse_api(event)
            if not res:
                task_status.update({"result": 0, "remark": "[回滚]入库失败"})
                self.record_log(task_status)
                raise RuntimeError(f"{target}入库失败：{payload}")
        match = re.match(".*rowcount=(\d*).*", cleaned_file).group(1)
        record_num = int(match)
        task_status.update(
            {"result": 1, "remark": "[回滚]入库成功", "record_num": record_num}
        )
        self.record_log(task_status)
        return payload

    def pipeline(self):
        with self.app.app_context():
            print("开始抓数")
            extracted_files = self.extract_data()
            print(f"抓取完成：{extracted_files}")
            for target in self.target_list:
                print(f"开始清洗：{target}")
                cleaned_file = self.clean_data(target)
                print(f"清洗完成：{cleaned_file}")
                print(f"开始入库：{target}")
                self.warehouse_data(target, cleaned_file)
                print(f"入库完成：{target}")
            db.engine.dispose()


@huey.task()
def task_rollback(source_id, date, erp_name, target_list):
    task_set = RollbackTaskSet(source_id, date, erp_name, target_list)
    task_set.pipeline()
    return True
