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
source_id = '34YYYYYYYYYYYYY'
erp_name = '宏业'
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
    'start_date': datetime(2018, 9, 8),
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

dag = DAG(dag_id='ext_34', schedule_interval='0 22 * * *', default_args=args)

"""
 先创建3个task
 1. 生成我们要抓数的日期(调用接口) 然后将日期放入到xcom中
 2. 我们从xcom中拿到日期，然后调用接口来生成全量sql,接口返回sql的filename
 3. 然后我们filename和一系列的数据进行组装,执行抓数的lambda, 到此抓数的逻辑告一段落
"""

target_list = ['chain_store',
               'chain_goods',
               'chain_category',
               'goodsflow',
               'cost',
               'chain_sales_target',
               'chain_goods_loss'
               ]

generate_common_task(source_id, cmid, erp_name, dag, target_list)


DATA_KEY_TEMPLATE = "{S3_BUCKET}/{clean_data_file_path}"

