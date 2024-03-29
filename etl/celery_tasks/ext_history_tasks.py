from etl.etl import celery
from etl.service.ext_sql import DatasourceSqlService
from etl.service.datasource import DatasourceService
from datetime import timedelta
import lambda_fun.extract_data.extract_db_worker as worker
from lambda_fun.load_data.warehouse import handler as load_warehouse
from common.common import get_content, SQL_PREFIX, S3_BUCKET, LAMBDA
from etl.models.etl_table import ExtCleanInfo, ExtHistoryTask, ExtHistoryLog
from etl.models.datasource import ExtDatasource
from etl.models.ext_datasource_con import ExtDatasourceCon
import json
from datetime import datetime
from etl.models import session_scope
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import current_app
from config.config import config
import os
import time
from botocore.vendored.requests.exceptions import ReadTimeout
from functools import partial

UPSERT_TABLE = ['chain_store', 'chain_goods', 'chain_category', 'chain_verdor']


@celery.task(bind=True, name='ext_history.start')
def start_tasks(self, data):
    source_id = data.get('source_id')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    target_tables = data.get('target_tables')
    task_type = data.get('task_type')

    task_id = start_tasks.request.id
    # 将任务信息记录到数据库中
    create_task_status(data, task_id, task_type, target_tables)

    # 获取抓数休息时间，防止对数据库造成压力, 默认100s
    datasource_con = ExtDatasourceCon.query.filter_by(source_id=source_id).first()
    period = datasource_con.period if datasource_con else 100

    total = two_date_total(start_date, end_date)

    record_par = partial(record_log, source_id, task_id)
    ext_par = partial(ext, source_id, target_tables)
    load_par = partial(load, source_id, target_tables)

    # 正式开始执行任务
    while start_date <= end_date:
        print(end_date)
        flag, remark, result, success_list, fail_list = True, None, None, None, None
        if task_type == '1':
            # 抓数，清洗，入库
            print('任务类型：抓数和入库')
            # 开始抓数
            flag, remark = ext_par(end_date)
            if flag is True:
                # 开始清洗，入库
                success_list, fail_list = load_par(end_date)

        elif task_type == '2':
            # 只抓数
            print('任务类型：抓数')
            flag, remark = ext_par(end_date)
            result = 1 if flag else 2

        elif task_type == '3':
            # 清洗，入库
            print('任务类型：入库')
            print(end_date)
            success_list, fail_list = load_par(end_date)
            result = 2 if fail_list else 1

        record_par(end_date, remark=remark, result=result, success_list=success_list, fail_list=fail_list)

        end_date = date_reduce_one_data(end_date)
        self.update_state(state='RUNNING', meta={'total': total, 'pending': two_date_total(start_date, end_date)})

        if end_date >= start_date and task_type != '3':
            # 休息一段时间再抓数
            print(f'{source_id}休息{period}秒')
            time.sleep(period)

    # 更新任务状态
    update_task_status(task_id, 1)


def ext(source_id, target_tables, end_date):
    """
    完成抓数任务
    :param source_id:
    :param end_date:
    :return:
    """
    # 每一步出错就跳过，记录信息到ext_history_log中

    # 调用接口，生成sql
    filename = DatasourceSqlService().generate_target_sql(source_id, end_date, target_tables)
    # 根据sql，调用抓数lambda
    event = DatasourceService().generator_extract_event(source_id)
    event['query_date'] = end_date
    event['filename'] = filename
    print(f'{source_id}抓数event:{event}')

    try:
        invoke_response = invoke_lambda('extract_db_worker', event)
        payload_body = invoke_response['Payload']
        payload_str = payload_body.read()
        response = json.loads(payload_str)
    except ReadTimeout as e:
        response = {}
        print(f'{source_id}调用lambda超时，错误原因:{str(e)}')

    errmsg = None
    flag = check_ext_result(source_id, end_date, filename, response.get('extract_data'))
    if not flag:
        print(f'{source_id}lambda抓数失败，开始调用接口')
        try:
            response = json.loads(worker.handler(event, None))
            extract_data = response.get('extract_data')
            flag = check_ext_result(source_id, end_date, filename, extract_data)
        except Exception as e:
            print(f'{source_id}调用接口抓数失败，errmsg:{str(e)}')
            flag, errmsg = False, f'抓数失败，失败原因:{str(e)}'

    return flag, errmsg


def load(source_id, target_tables, end_date):
    """
    清洗和入库任务，返回成功和失败的表名称的列表
    """
    success_table = []
    fail_table = []
    with ThreadPoolExecutor() as executor:
        futures_list = [
            executor.submit(load_one_table, source_id, end_date, table, current_app._get_current_object())
            for table in target_tables
        ]

        for futures in as_completed(futures_list):
            flag, item = futures.result()
            if flag:
                success_table.append(item)
            else:
                fail_table.append(item)

    return success_table, fail_table


def load_one_table(source_id, end_date, table, app):
    """
    只清洗和入库一个目标表，用于创建多线程
    success_list: ["table1", "table2"....]
    fail_list:[{"table1":"reason1"}, {"table2":"reason2"}......]
    :return:
    """
    cmid = source_id.split('Y')[0]

    # 调用清洗lambda
    with app.app_context():
        table_info = ExtCleanInfo.query.filter_by(source_id=source_id, target_table=table, deleted=False).first()
        origin_table = {key.lower(): value for key, value in table_info.origin_table.items()} \
            if table_info.origin_table else {}
        convert_str = {key.lower(): value for key, value in table_info.covert_str.items()} \
            if table_info.covert_str else {}

        datasource = ExtDatasource.query.filter_by(source_id=source_id).first()

    target_table = table.split('chain_')[-1]
    event = dict(source_id=source_id, erp_name=datasource.erp_vendor, date=end_date, target_table=target_table,
                 origin_table_columns=origin_table, converts=convert_str)
    print(f'清洗event:{event}')
    invoke_response = invoke_lambda('lambda_clean_data', event)
    payload_body = invoke_response['Payload']
    payload_str = payload_body.read()
    clean_data_file_path = json.loads(payload_str)

    if 'FunctionError' in invoke_response:

        remark = base64.b64decode(invoke_response['LogResult'])
        print(f'清洗失败，errmsg:{remark}')
        return False, f'{table}清洗失败'

    # 调用入库lambda
    event = dict(
        redshift_url=config[os.getenv('ETL_ENVIREMENT', 'dev')].REDSHIFT_URL,
        target_table=table if table in UPSERT_TABLE else f'{table}_{source_id}',
        data_key=f'ext-etl-data/{clean_data_file_path}',
        data_date=end_date,
        warehouse_type='upsert' if table in UPSERT_TABLE else 'copy',
        source_id=source_id,
        cmid=cmid
    )
    print(f'{source_id}入库event:{event}')
    invoke_response = invoke_lambda('etl-warehouse', event)

    if 'FunctionError' in invoke_response:
        remark = base64.b64decode(invoke_response['LogResult'])
        print(f'{source_id}lambda入库失败，{table},errmsg:{remark}')
        print(f'{source_id}开始调用接口入库')
        try:
            load_warehouse(event, None)
        except Exception as e:
            print(f'{source_id}调用接口入库失败，失败原因:{str(e)}')
            return False, f'{table}入库失败'

    print(f'{source_id}:{table}入库成功')

    return True, table


@session_scope
def record_log(source_id, task_id, ext_date, result=None, success_list=None, fail_list=None, remark=None):
    """
    {"source_id":"32YYYYYYYYYYYYY","cmid":3201,"task_type":1,"table_name":"goods","record_num":120,
    "start_time":2018-02-02,"end_time":2018-02-02,"cost_time":111,"result":1}

    success_list: ["table1", "table2"....]
    fail_list:[{"table1":"reason1"}, {"table2":"reason2"}......]
    :return:
    """
    if not result:
        result = 2 if fail_list else 1
    success_table = ','.join(success_list) if success_list else None
    fail_table = ','.join(fail_list) if fail_list else None

    info = dict(source_id=source_id, task_id=task_id, ext_date=ext_date, result=result,
                success_table=success_table, fail_table=fail_table, remark=remark)

    ExtHistoryLog(**info).save()


@session_scope
def create_task_status(data, task_id, task_type, target_table):

    source_id = data.get('source_id')
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    target_table = ','.join(target_table)
    info = dict(source_id=source_id, task_id=task_id, task_type=task_type, target_table=target_table,
                ext_start=start_date, ext_end=end_date, task_start=datetime.now(), status=3)

    ExtHistoryTask(**info).save()


@session_scope
def update_task_status(task_id, status, remark=None):
    ext_history_task = ExtHistoryTask.query.filter_by(task_id=task_id).first()
    ext_history_task.task_end = datetime.now()
    ext_history_task.status = status
    if remark:
        ext_history_task.remark = remark
    ext_history_task.save()


def check_ext_result(source_id, extract_date, filename, extract_data):
    """
    校验抓数的结果
    :return:
    """

    key = (SQL_PREFIX.format(source_id=source_id, date=extract_date) + filename)
    s3_body = get_content(S3_BUCKET, key)
    sqls = s3_body.get('sqls')
    for key, values in sqls.items():
        if (not extract_data) or (extract_data.get(key) is None) or (len(extract_data[key]) != len(values)):
            return False
    return True


def invoke_lambda(functionname, event):
    invoke_response = LAMBDA.invoke(
        FunctionName=functionname, InvocationType='RequestResponse', Payload=json.dumps(event), LogType='Tail'
    )
    return invoke_response


def two_date_total(start_date, end_date):
    """
    返回两个日期相差的天数
    :param start_date:
    :param end_date:
    :return:
    """
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    return (end_date - start_date).days + 1


def date_reduce_one_data(date):
    date = datetime.strptime(date, '%Y-%m-%d')
    date -= timedelta(days=1)
    date = date.strftime('%Y-%m-%d')
    return date

