# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import requests
from airflow.hooks.http_hook import HttpHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import json
import boto3
from collections import defaultdict
import time
import re
from common import *

lambda_client = boto3.client('lambda')
S3_CLIENT = boto3.resource('s3')
source_id = '72YYYYYYYYYYYYY'
erp_name = '思迅'
cmid = source_id.split("Y")[0]
SQL_PREFIX = 'sql/source_id={source_id}/{date}/'
S3_BUCKET = 'ext-etl-data'

# 调用web接口来生成source_id的指定cron表达式
# response = requests.get('http://172.31.16.17:5000/etl/admin/api/crontab/' + source_id)
# result = json.loads(response.text)
# interval = result['data']
args = {
    'owner': 'BeanNan',
    'depends_on_past': False,
    'email': ['fanjianan@chaomengdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': datetime(2018, 9, 4),
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

dag = DAG(dag_id='ext_72', schedule_interval='0 22 * * *', default_args=args)

"""
 先创建3个task
 1. 生成我们要抓数的日期(调用接口) 然后将日期放入到xcom中
 2. 我们从xcom中拿到日期，然后调用接口来生成全量sql,接口返回sql的filename
 3. 然后我们filename和一系列的数据进行组装,执行抓数的lambda, 到此抓数的逻辑告一段落
"""

generate_extract_date = PythonOperator(
    task_id='generator_extract_date',
    python_callable=generator_extract_date_function,
    dag=dag,
    op_kwargs=dict(source_id=source_id)
)

generate_sql = PythonOperator(
    task_id='generator_sql',
    python_callable=generator_sql_function,
    dag=dag,
    op_kwargs=dict(source_id=source_id)
)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_by_filename,
    dag=dag,
    op_kwargs=dict(source_id=source_id)
)

generate_extract_date >> generate_sql >> extract_data

"""
    我们抓数完成，我们需要进行验证,验证我们的抓数是否成功,我们抓数完成之后,会生成一个json文件,里面存放着每个sql对应的文件地址
    我们拿sql-json和file-json文件中，两者相对应的表进行验证看看数量是否相等
    sql-json 可以通过filename来进行获取
    file-json 可以通过extract_data来获取
"""

check_extract_data_result = PythonOperator(
    task_id='check_extract_data_result',
    dag=dag,
    python_callable=check_extract_data_result_function,
    op_kwargs=dict(source_id=source_id)
)

extract_data >> check_extract_data_result

recording_extract_data_log = PythonOperator(
    dag=dag,
    task_id='recording_extract_data_log',
    python_callable=recording_extract_data_log_function
)

check_extract_data_result >> recording_extract_data_log

"""
    抓数的过程已经完成，现在需要做的就是将清洗的的lambda放入到lambda中,然后我们进行调用
    1. 这个clean_data lambda每次只支持清洗一个表，所以我们要将我们要清洗的表分别做成一个分支来清洗
    2. 当所有分支清洗完成之后,我们需要进行校验，验证我们的清洗是否完成,
"""





clean_store = PythonOperator(
    task_id='clean_store',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='chain_store',
                   target_table='store')
)

clean_category = PythonOperator(
    task_id='clean_category',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='chain_category',
                   target_table='category')
)

clean_goods = PythonOperator(
    task_id='clean_goods',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='chain_goods',
                   target_table='goods')
)

clean_goodsflow = PythonOperator(
    task_id='clean_goodsflow',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='goodsflow',
                   target_table='goodsflow')
)

clean_cost = PythonOperator(
    task_id='clean_cost',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='cost',
                   target_table='cost')
)

'''
    开始对每一步的清洗动作的结果进行记录日志
'''

recording_clean_store_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_store_log',
    python_callable=recording_clean_store_log_function
)

recording_clean_goods_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_goods_log',
    python_callable=recording_clean_goods_log_function
)

recording_clean_category_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_category_log',
    python_callable=recording_clean_category_log_function
)

recording_clean_goodsflow_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_goodsflow_log',
    python_callable=recording_clean_goodsflow_log_function
)

recording_clean_cost_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_cost_log',
    python_callable=recording_clean_cost_log_function
)

recording_extract_data_log >> clean_store >> recording_clean_store_log
recording_extract_data_log >> clean_goods >> recording_clean_goods_log
recording_extract_data_log >> clean_category >> recording_clean_category_log
recording_extract_data_log >> clean_goodsflow >> recording_clean_goodsflow_log
recording_extract_data_log >> clean_cost >> recording_clean_cost_log

"""
现在要开始进行执行入库逻辑,每个goods对应一套入库和记录日志
"""

DATA_KEY_TEMPLATE = "{S3_BUCKET}/{clean_data_file_path}"

load_store = PythonOperator(
    task_id='load_store',
    dag=dag,
    python_callable=load_store_function,
    op_kwargs=dict(cmid=cmid, source_id=source_id)
)

recording_load_store = PythonOperator(
    task_id='recording_load_store',
    dag=dag,
    python_callable=recording_load_store_function
)

recording_clean_store_log >> load_store >> recording_load_store

load_goods = PythonOperator(
    task_id='load_goods',
    dag=dag,
    python_callable=load_goods_function,
    op_kwargs=dict(cmid=cmid, source_id=source_id)
)

recording_load_goods = PythonOperator(
    task_id='recording_load_goods',
    dag=dag,
    python_callable=recording_load_goods_function
)

recording_clean_goods_log >> load_goods >> recording_load_goods

load_category = PythonOperator(
    task_id='load_category',
    dag=dag,
    python_callable=load_category_function,
    op_kwargs=dict(cmid=cmid, source_id=source_id)
)

recording_load_category = PythonOperator(
    task_id='recording_load_category',
    dag=dag,
    python_callable=recording_load_category_function
)

recording_clean_category_log >> load_category >> recording_load_category

# goodsflow

load_goodsflow = PythonOperator(
    task_id='load_goodsflow',
    dag=dag,
    python_callable=load_goodsflow_function,
    op_kwargs=dict(cmid=cmid, source_id=source_id)
)

recording_load_goodsflow = PythonOperator(
    task_id='recording_load_goodsflow',
    dag=dag,
    python_callable=recording_load_goodsflow_function
)

recording_clean_goodsflow_log >> load_goodsflow >> recording_load_goodsflow

load_cost = PythonOperator(
    task_id='load_cost',
    dag=dag,
    python_callable=load_cost_function,
    op_kwargs=dict(cmid=cmid, source_id=source_id)
)

recording_load_cost = PythonOperator(
    task_id='recording_load_cost',
    dag=dag,
    python_callable=recording_load_cost_function
)

recording_clean_cost_log >> load_cost >> recording_load_cost
