from etl.tasks.config import huey
from lambda_fun.load_data.warehouse import Warehouser


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
