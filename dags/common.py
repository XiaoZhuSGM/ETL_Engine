# -*- coding: utf-8 -*-
# @Time    : 2018/9/3 22:00
# @Author  : 范佳楠

from airflow.hooks.http_hook import HttpHook
import json
import time
import boto3
from datetime import datetime
from collections import defaultdict
import re

SQL_PREFIX = 'sql/source_id={source_id}/{date}/'
S3_BUCKET = 'ext-etl-data'
SQL_PREFIX_SPLIT = 'sql/source_id={source_id}/{date}/split/'

S3_CLIENT = boto3.resource('s3')
lambda_client = boto3.client('lambda')


def generator_extract_date_function(**kwargs):
    source_id = kwargs['source_id']
    event_dict = hook_get(endpoint=f'etl/admin/api/extract/event/{source_id}')
    if event_dict['meta']['code'] != 200:
        raise RuntimeError(event_dict['meta']['message'])

    print('success')
    return event_dict['data']['query_date']


def generator_sql_function(**kwargs):
    """
    首先我们要获取上一个task传递过来的日期,从xcom中进行获取
    然后返回sql文件的filename
    :param kwargs:
    :return: sql.filename
    """
    ti = kwargs['ti']
    source_id = kwargs['source_id']
    query_date = ti.xcom_pull(task_ids='generator_extract_date')
    params = dict(source_id=source_id, date=query_date)
    result = hook_get(endpoint='etl/admin/api/sql/full', data=params)

    if result['meta']['code'] != 200:
        raise RuntimeError(result['meta']['code'])

    return result['data']


def upload_s3(source_id, date, sqls):
    tables_sqls = {
        "type": "full",
        "query_date": date,
        "source_id": source_id,
        "sqls": sqls,
    }

    file_name = str(now_timestamp()) + ".json"
    key = (
            SQL_PREFIX_SPLIT.format(source_id=source_id, date=date)
            + file_name
    )
    S3_CLIENT.Object(bucket_name=S3_BUCKET, key=key).put(
        Body=json.dumps(tables_sqls))
    return f'split/{file_name}'


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time())
    return _timestamp


def split_sqls(source_id, date, file_name):
    key = SQL_PREFIX.format(source_id=source_id, date=date) + file_name
    extract_sqls_str = S3_CLIENT.Object(S3_BUCKET, key).get()['Body'].read().decode('utf-8')
    extract_sqls = json.loads(extract_sqls_str)
    sqls_dict = extract_sqls['sqls']
    total_sqls = [y for x in sqls_dict.values() for y in x]
    total_sql_len = len(total_sqls)
    if total_sql_len >= 100:
        sort_key = sorted(sqls_dict.keys(), key=lambda first: len(sqls_dict[first]))
        mid = total_sql_len // 2
        current_num = 0
        sqls_dict_1 = dict()
        sqls_dict_2 = dict()
        flag = True
        for key in sort_key:
            tables = sqls_dict[key]
            current_num += len(tables)
            if flag:
                if current_num >= mid:
                    flag = False
                sqls_dict_1[key] = tables
            else:
                sqls_dict_2[key] = tables
        file_name_1 = upload_s3(source_id, date, sqls_dict_1)
        file_name_2 = upload_s3(source_id, date, sqls_dict_2)
        return [file_name_1, file_name_2]
    else:
        return [file_name, ]


def invoke_lambda(FunctionName, **params):
    response = lambda_client.invoke(FunctionName=FunctionName,
                                    InvocationType='RequestResponse',
                                    Payload=json.dumps(params),
                                    )
    return response


def extract_data_by_filename(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(task_ids='generator_sql')
    source_id = kwargs['source_id']
    # 然后调用接口返回抓数的其他信息
    event_dict = hook_get(endpoint=f'etl/admin/api/extract/event/{source_id}')
    params = event_dict['data']
    date = params['query_date']
    # 这一步对我们获取的filename进行分析,看是否需要进行拆分
    sql_filename_list = split_sqls(source_id, date, filename)

    # 遍历filename_list
    """
    组织一下返回格式，为了方便之后进行校验
    {
        start_time:yyyy-MM-dd,
        end_time:yyyy-MM-dd,
        cost_time:1111,
        query_date:yyyy-MM-dd,
        mapping: [
            filename:filename,
            extract_data:{
            }
        ]
    }
    """
    extract_dict = dict()
    extract_dict['mapping'] = list()
    start_time = time.time()
    for sql_filename in sql_filename_list:
        params['filename'] = sql_filename
        response = invoke_lambda('extract_db_worker', **params)
        if 'FunctionError' not in response:
            payload_body = response['Payload']
            payload_str = payload_body.read()
            payload = json.loads(payload_str)
            current_mapping = dict()
            current_mapping['filename'] = sql_filename
            current_mapping['extract_data'] = payload['extract_data']
            extract_dict['mapping'].append(current_mapping)
        else:
            raise RuntimeError('lambda运行过程中出现问题')

        if len(sql_filename_list) != 1:
            time.sleep(600)

    end_time = time.time()
    cost_time = int(end_time - start_time)

    extract_dict['start_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
    extract_dict['end_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))
    extract_dict['cost_time'] = cost_time
    extract_dict['query_date'] = date

    return extract_dict


def compare_sqls_and_extract_datas(source_id,
                                   date,
                                   filename,
                                   start_time,
                                   end_time,
                                   cost_time,
                                   extract_data_dict):
    # 首先将filename中的数据读取进来
    cmid = source_id.split("Y")[0]
    sql_file_key = SQL_PREFIX.format(source_id=source_id, date=date) + filename
    extract_sqls_str = S3_CLIENT.Object(S3_BUCKET, sql_file_key).get()['Body'].read().decode('utf-8')
    sqls_dict = json.loads(extract_sqls_str)
    sql_table_info = sqls_dict['sqls']

    log_info_list = list()

    for table_name, table_sqls in sql_table_info.items():
        table_sql_len = len(table_sqls)
        table_extract_paths = extract_data_dict[table_name]
        table_extract_len = len(table_extract_paths)

        if table_sql_len == table_extract_len:
            # 说明当前表是抓数成功的
            # 首先去计算我们总共抓取了几条数据
            record_num = collect_extract_data_nums(table_extract_paths)
            extract_result_log = dict(
                source_id=source_id,
                cmid=cmid,
                table_name=table_name,
                extract_date=date,
                result=1,
                task_type=1,
                start_time=start_time,
                end_time=end_time,
                cost_time=cost_time,
                record_num=record_num,
                remark='抓数成功'
            )
        else:
            # 当前表没有抓数成功，我们将信息记录下来
            extract_result_log = dict(
                source_id=source_id,
                cmid=cmid,
                table_name=table_name,
                extract_date=date,
                result=2,
                task_type=1,
                start_time=start_time,
                end_time=end_time,
                cost_time=cost_time,
                remark='抓数失败,准备进行重抓'
            )

        log_info_list.append(extract_result_log)

    return log_info_list


def check_extract_data_result_function(**kwargs):
    ti = kwargs['ti']
    extract_dict = ti.xcom_pull(task_ids='extract_data')
    query_date = extract_dict['query_date']
    mapping_list = extract_dict['mapping']
    start_time = extract_dict['start_time']
    end_time = extract_dict['end_time']
    cost_time = extract_dict['cost_time']
    source_id = kwargs['source_id']

    total_log_list = list()

    for mapping in mapping_list:
        sql_filename = mapping['filename']
        extract_data_dict = mapping['extract_data']
        extract_data_dict = defaultdict(list, **extract_data_dict)
        current_log_list = compare_sqls_and_extract_datas(source_id,
                                                          query_date,
                                                          sql_filename,
                                                          start_time,
                                                          end_time,
                                                          cost_time,
                                                          extract_data_dict
                                                          )
        total_log_list.extend(current_log_list)

    return total_log_list


def collect_extract_data_nums(table_extract_paths):
    sum = 0
    for single_path in table_extract_paths:
        match = re.match('.*rowcount=(\d*).*', single_path)
        count_str = match.group(1)
        count = int(count_str)
        sum += count
    return sum


def recording_extract_data_log_function(**kwargs):
    """
        已经将各个表的抓取信息记录下来了,这步我们需要将这些日志信息入库,
        并且将失败的表记录下来,然后进行下一步的抓取
    """
    ti = kwargs['ti']
    log_info_list = ti.xcom_pull(task_ids='check_extract_data_result')
    for log_info in log_info_list:
        # 日志入库
        recording_common_log_function(log_info)


def recording_common_log_function(result_info):
    headers = {'Content-Type': 'application/json'}
    if 'clean_data_file_path' in result_info:
        result_info.pop('clean_data_file_path')
    hook_post(endpoint='etl/admin/api/ext/log', data=result_info, headers=headers)


def common_recording_log_result(
        lambda_name,
        table_name,
        query_date,
        task_type,
        **event):
    source_id = event['source_id']
    cmid = source_id.split("Y")[0]
    start_time = time.time()
    response = invoke_lambda(lambda_name, **event)
    end_time = time.time()
    cost_time = int(end_time - start_time)
    return response, dict(source_id=source_id,
                          cmid=cmid,
                          table_name=table_name,
                          extract_date=query_date,
                          task_type=task_type,
                          start_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
                          end_time=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time)),
                          cost_time=cost_time)


def common_return_clean_result(table_name, query_date, **event):
    response, clean_common_result_dict = common_recording_log_result(
        'lambda_clean_data',
        table_name,
        query_date,
        2,
        **event
    )

    if 'FunctionError' not in response:
        payload_body = response['Payload']
        payload_str = payload_body.read()
        clean_data_file_path = json.loads(payload_str)
        match = re.match('.*rowcount=(\d*).*', clean_data_file_path)
        count_str = match.group(1)
        record_num = int(count_str)
        clean_common_result_dict['clean_data_file_path'] = clean_data_file_path
        clean_common_result_dict['record_num'] = record_num
        clean_common_result_dict['result'] = 1
        clean_common_result_dict['remark'] = '清洗成功'
    else:
        clean_common_result_dict['result'] = 0
        clean_common_result_dict['remark'] = '清洗失败，lambda执行过程发生错误'
        recording_common_log_function(clean_common_result_dict)
        raise RuntimeError('清洗失败, lambda执行中出现错误')

    return clean_common_result_dict


def hook_get(endpoint, data=None):
    hook = HttpHook(method='GET', http_conn_id='host_id')
    response = hook.run(endpoint=endpoint, data=data)
    event_dict = json.loads(response.text)

    return event_dict


def hook_post(endpoint, data, headers=None):
    hook = HttpHook(method='POST', http_conn_id='host_id')
    hook.run(endpoint=endpoint, data=json.dumps(data), headers=headers)


def recording_clean_store_log_function(**kwargs):
    ti = kwargs['ti']
    clean_store_info = ti.xcom_pull(task_ids='clean_store')
    print("clean_store\t", clean_store_info)
    recording_common_log_function(clean_store_info)


def recording_clean_goods_log_function(**kwargs):
    ti = kwargs['ti']
    clean_goods_info = ti.xcom_pull(task_ids='clean_goods')
    recording_common_log_function(clean_goods_info)


def recording_clean_category_log_function(**kwargs):
    ti = kwargs['ti']
    clean_category_info = ti.xcom_pull(task_ids='clean_category')
    recording_common_log_function(clean_category_info)


def recording_clean_goodsflow_log_function(**kwargs):
    ti = kwargs['ti']
    clean_goodsflow_info = ti.xcom_pull(task_ids='clean_goodsflow')
    recording_common_log_function(clean_goodsflow_info)


def recording_clean_cost_log_function(**kwargs):
    ti = kwargs['ti']
    clean_cost_info = ti.xcom_pull(task_ids='clean_cost')
    recording_common_log_function(clean_cost_info)


DATA_KEY_TEMPLATE = "{S3_BUCKET}/{clean_data_file_path}"


def load_store_function(**kwargs):
    ti = kwargs['ti']
    cmid = kwargs['cmid']
    source_id = kwargs['source_id']
    clean_store_info = ti.xcom_pull(task_ids='clean_store')
    extract_date = clean_store_info['extract_date']
    clean_data_file_path = clean_store_info['clean_data_file_path']
    event = {
***REMOVED***
        "target_table": "chain_store",
        "warehouse_type": "upsert",
        'cmid': cmid,
        'data_date': extract_date,
        'data_key': DATA_KEY_TEMPLATE.format(S3_BUCKET=S3_BUCKET, clean_data_file_path=clean_data_file_path),
        'source_id': source_id
    }

    print('beanan\t', event)

    response, load_result = common_recording_log_result('etl-warehouse', 'chain_store', extract_date, 3, **event)

    if 'FunctionError' not in response:
        load_result['result'] = 1
        load_result['remark'] = '入库成功'
        match = re.match('.*rowcount=(\d*).*', clean_data_file_path)
        count_str = match.group(1)
        record_num = int(count_str)
        load_result['record_num'] = record_num
    else:
        load_result['result'] = 0
        load_result['remark'] = '入库lambda报错'
        recording_common_log_function(load_result)
        raise RuntimeError('入库lambda报错')

    return load_result


def recording_load_store_function(**kwargs):
    ti = kwargs['ti']
    load_store_info = ti.xcom_pull(task_ids='load_store')
    print("load_stor_store\t", load_store_info)
    recording_common_log_function(load_store_info)


def load_goods_function(**kwargs):
    ti = kwargs['ti']
    cmid = kwargs['cmid']
    source_id = kwargs['source_id']
    clean_goods_info = ti.xcom_pull(task_ids='clean_goods')
    extract_date = clean_goods_info['extract_date']
    clean_data_file_path = clean_goods_info['clean_data_file_path']
    event = {
***REMOVED***
        "target_table": "chain_goods",
        "warehouse_type": "upsert",
        'cmid': cmid,
        'data_date': extract_date,
        'data_key': DATA_KEY_TEMPLATE.format(S3_BUCKET=S3_BUCKET, clean_data_file_path=clean_data_file_path),
        'source_id': source_id

    }
    response, load_result = common_recording_log_result('etl-warehouse', 'chain_goods', extract_date, 3, **event)

    if 'FunctionError' not in response:
        load_result['result'] = 1
        load_result['remark'] = '入库成功'
        match = re.match('.*rowcount=(\d*).*', clean_data_file_path)
        count_str = match.group(1)
        record_num = int(count_str)
        load_result['record_num'] = record_num
    else:
        load_result['result'] = 0
        load_result['remark'] = '入库lambda报错'
        recording_common_log_function(load_result)
        raise RuntimeError('入库lambda报错')

    return load_result


def recording_load_goods_function(**kwargs):
    ti = kwargs['ti']
    load_goods_info = ti.xcom_pull(task_ids='load_goods')
    print("load_stor_goods\t", load_goods_info)
    recording_common_log_function(load_goods_info)


def load_category_function(**kwargs):
    ti = kwargs['ti']
    cmid = kwargs['cmid']
    source_id = kwargs['source_id']
    clean_category_info = ti.xcom_pull(task_ids='clean_category')
    extract_date = clean_category_info['extract_date']
    clean_data_file_path = clean_category_info['clean_data_file_path']
    event = {
***REMOVED***
        "target_table": "chain_category",
        "warehouse_type": "upsert",
        'cmid': cmid,
        'data_date': extract_date,
        'data_key': DATA_KEY_TEMPLATE.format(S3_BUCKET=S3_BUCKET, clean_data_file_path=clean_data_file_path),
        'source_id': source_id

    }
    response, load_result = common_recording_log_result('etl-warehouse', 'chain_category', extract_date, 3, **event)

    if 'FunctionError' not in response:
        load_result['result'] = 1
        load_result['remark'] = '入库成功'
        match = re.match('.*rowcount=(\d*).*', clean_data_file_path)
        count_str = match.group(1)
        record_num = int(count_str)
        load_result['record_num'] = record_num
    else:
        load_result['result'] = 0
        load_result['remark'] = '入库lambda报错'
        recording_common_log_function(load_result)
        raise RuntimeError('入库lambda报错')

    return load_result


def recording_load_category_function(**kwargs):
    ti = kwargs['ti']
    load_category_info = ti.xcom_pull(task_ids='load_category')
    print("load_stor_category\t", load_category_info)
    recording_common_log_function(load_category_info)


def load_goodsflow_function(**kwargs):
    ti = kwargs['ti']
    cmid = kwargs['cmid']
    source_id = kwargs['source_id']
    clean_goodsflow_info = ti.xcom_pull(task_ids='clean_goodsflow')
    extract_date = clean_goodsflow_info['extract_date']
    clean_data_file_path = clean_goodsflow_info['clean_data_file_path']
    event = {
***REMOVED***
        "target_table": f"goodsflow_{source_id}",
        "warehouse_type": "copy",
        'cmid': cmid,
        'data_date': extract_date,
        'data_key': DATA_KEY_TEMPLATE.format(S3_BUCKET=S3_BUCKET, clean_data_file_path=clean_data_file_path),
        'source_id': source_id
    }
    response, load_result = common_recording_log_result('etl-warehouse', 'chain_goodsflow', extract_date, 3, **event)

    if 'FunctionError' not in response:
        load_result['result'] = 1
        load_result['remark'] = '入库成功'
        match = re.match('.*rowcount=(\d*).*', clean_data_file_path)
        count_str = match.group(1)
        record_num = int(count_str)
        load_result['record_num'] = record_num
    else:
        load_result['result'] = 0
        load_result['remark'] = '入库lambda报错'
        recording_common_log_function(load_result)
        raise RuntimeError('入库lambda报错')

    return load_result


def recording_load_goodsflow_function(**kwargs):
    ti = kwargs['ti']
    load_goodsflow_info = ti.xcom_pull(task_ids='load_goodsflow')
    print("load_stor_goodsflow\t", load_goodsflow_info)
    recording_common_log_function(load_goodsflow_info)


def load_cost_function(**kwargs):
    ti = kwargs['ti']
    cmid = kwargs['cmid']
    source_id = kwargs['source_id']
    clean_cost_info = ti.xcom_pull(task_ids='clean_cost')
    extract_date = clean_cost_info['extract_date']
    clean_data_file_path = clean_cost_info['clean_data_file_path']
    event = {
***REMOVED***
        "target_table": f"cost_{source_id}",
        "warehouse_type": "copy",
        'cmid': cmid,
        'data_date': extract_date,
        'data_key': DATA_KEY_TEMPLATE.format(S3_BUCKET=S3_BUCKET, clean_data_file_path=clean_data_file_path),
        'source_id': source_id

    }
    response, load_result = common_recording_log_result('etl-warehouse', 'chain_cost', extract_date, 3, **event)

    if 'FunctionError' not in response:
        load_result['result'] = 1
        load_result['remark'] = '入库成功'
        match = re.match('.*rowcount=(\d*).*', clean_data_file_path)
        count_str = match.group(1)
        record_num = int(count_str)
        load_result['record_num'] = record_num
    else:
        load_result['result'] = 0
        load_result['remark'] = '入库lambda报错'
        recording_common_log_function(load_result)
        raise RuntimeError('入库lambda报错')

    return load_result


def recording_load_cost_function(**kwargs):
    ti = kwargs['ti']
    load_cost_info = ti.xcom_pull(task_ids='load_cost')
    print("load_stor_cost\t", load_cost_info)
    recording_common_log_function(load_cost_info)


def clean_common_function(**kwargs):
    ti = kwargs['ti']
    source_id = kwargs['source_id']
    erp_name = kwargs['erp_name']
    target_table = kwargs['target_table']
    target = kwargs['target']

    query_date = ti.xcom_pull(task_ids='generator_extract_date')
    params = dict(source_id=source_id, target=target)
    result_dict = hook_get(endpoint='etl/admin/api/ext_clean_info/target', data=params)
    store_event = dict(source_id=source_id,
                       erp_name=erp_name,
                       date=query_date,
                       target_table=target_table)
    store_event['origin_table_columns'] = result_dict['data']['target']['origin_table']
    store_event['converts'] = result_dict['data']['target']['covert_str']

    return common_return_clean_result(target_table, query_date, **store_event)
