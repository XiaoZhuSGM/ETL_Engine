from lambda_fun.load_data.warehouse import Warehouser
import lambda_fun.extract_data.extract_db_worker as worker
import lambda_fun.extract_inventory.extract_inv_worker as inv_worker
from etl.etl import celery
from etl.service.iqr import IQRService
from datetime import datetime
from etl.extensions import cache
import pytz

_TZ = pytz.timezone("Asia/Shanghai")


@celery.task(name="etl.iqr")
def task_iqr(source_id):
    iqr_service = IQRService(source_id)
    cache_key = f"iqr_{source_id}"

    hour = datetime.now(_TZ).hour
    result = cache.get(cache_key)
    print(result)
    if hour == 4 or (result is None):
        print("进行计算了")
        result = iqr_service.pipeline()
        cache.set(cache_key, result)
    return result


@celery.task(name="etl.task_warehose")
def task_warehouse(
    db_url,
    target_table,
    data_key,
    sync_column,
    date_column,
    cmid,
    source_id,
    warehouse_type,
    **kwargs,
):
    runner = Warehouser(
        db_url, target_table, data_key, sync_column, date_column, cmid, source_id
    )
    runner.run(warehouse_type)
    return True


@celery.task(name="etl.task_extract_data")
def task_extract_data(source_id, query_date, task_type, filename, db_url, **kwargs):
    event = dict(
        source_id=source_id,
        query_date=query_date,
        task_type=task_type,
        filename=filename,
        db_url=db_url,
    )
    return worker.handler(event, None)


@celery.task(name="inventory.task_extract_inventory")
def task_extract_inventory(source_id, query_date, task_type, filename, db_url, **kwargs):
    """kucun"""
    event = dict(
        source_id=source_id,
        query_date=query_date,
        task_type=task_type,
        filename=filename,
        db_url=db_url,
    )
    return inv_worker.handler(event, None)
