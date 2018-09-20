from etl.tasks.config import huey
from lambda_fun.load_data.warehouse import Warehouser
import lambda_fun.extract_data.extract_db_worker as worker

@huey.task(include_task=True)
def task_warehouse(
        db_url,
        target_table,
        data_key,
        sync_column,
        date_column,
        cmid,
        source_id,
        warehouse_type,
        **kwargs
):
    runner = Warehouser(
        db_url,
        target_table,
        data_key,
        sync_column,
        date_column,
        cmid,
        source_id,
    )
    runner.run(warehouse_type)
    return True


@huey.task(include_task=True)
def task_extract_data(
        source_id,
        query_date,
        task_type,
        filename,
        db_url,
        **kwargs
):
    event = dict(source_id=source_id, query_date=query_date, task_type=task_type,
                 filename=filename,
                 db_url=db_url)

    return worker.handler(event, None)
