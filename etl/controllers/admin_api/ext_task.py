from etl.tasks.tasks import task_warehouse, task_extract_data
from . import etl_admin_api
from .. import jsonify_with_data, APIError
from flask import request
from etl.tasks.config import huey
import json
import boto3

S3_BUCKET = "ext-etl-data"
TARGET_TABLE_KEY = "target_table/target_table.json"
S3_CLIENT = boto3.resource("s3")


@etl_admin_api.route("/ext/tasks/extract_data", methods=["POST"])
def trigger_task_extract_data():
    message = request.json

    source_id = message["source_id"]
    query_date = message["query_date"]
    task_type = message["task_type"]
    filename = message["filename"]
    db_url = message["db_url"]

    result = task_extract_data(
        source_id=source_id,
        query_date=query_date,
        task_type=task_type,
        filename=filename,
        db_url=db_url,
    )
    return jsonify_with_data(APIError.OK, data={"task_id": result.task.task_id})


@etl_admin_api.route("/ext/tasks/extract_data/status", methods=["GET"])
def get_task_extract_data_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = huey.result(task_id, preserve=True)
    if result is None:
        status = "running"
    elif isinstance(result, str):
        status = "success"
    else:
        status = "failed"
        reason = str(result)
        result = ""
    return jsonify_with_data(
        APIError.OK,
        data={"status": status, "reason": reason, "task_id": task_id, "result": result},
    )


@etl_admin_api.route("/ext/tasks/warehouse", methods=["POST"])
def trigger_task_warehouse():
    message = request.json
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
    result = task_warehouse(
        redshift_url,
        target_table,
        data_key,
        sync_column,
        date_column,
        cmid,
        source_id,
        warehouse_type,
    )
    return jsonify_with_data(APIError.OK, data={"task_id": result.task.task_id})


@etl_admin_api.route("/ext/tasks/warehouse/status", methods=["GET"])
def get_task_warehouse_status():
    task_id = request.args.get("task_id")
    reason = ""
    result = huey.result(task_id, preserve=True)
    if result is None:
        status = "running"
    elif result is True:
        status = "success"
    else:
        status = "failed"
        reason = str(result)
    return jsonify_with_data(
        APIError.OK, data={"status": status, "reason": reason, "task_id": task_id}
    )
