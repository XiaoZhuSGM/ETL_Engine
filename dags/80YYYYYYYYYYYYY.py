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
import airflow


lambda_client = boto3.client('lambda')
S3_CLIENT = boto3.resource('s3')
source_id = '80YYYYYYYYYYYYY'
erp_name = '海鼎'
cmid = source_id.split("Y")[0]
SQL_PREFIX = 'sql/source_id={source_id}/{date}/'
S3_BUCKET = 'ext-etl-data'

# 调用web接口来生成source_id的指定cron表达式
response = requests.get('http://localhost:5000/etl/admin/api/crontab/full/' + source_id)
result = json.loads(response.text)
interval = result['data']
args = {
    'owner': 'BeanNan',
    'depends_on_past': False,
    'email': ['fanjianan@chaomengdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

dag = DAG(dag_id='ext_80', schedule_interval=interval, default_args=args)


target_list = ['chain_store',
               'chain_goods',
               'chain_category',
               'goodsflow',
               'cost',
               'requireorder',
               'delivery',
               'purchase_warehouse',
               'purchase_store',
               'move_store',
               'move_warehouse',
               'check_warehouse',
               'chain_goods_loss',
               ]

generate_common_task(source_id, cmid, erp_name, dag, target_list)


