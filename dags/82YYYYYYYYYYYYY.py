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
source_id = '82YYYYYYYYYYYYY'
erp_name = '海鼎'
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
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

dag = DAG(dag_id='ext_82', schedule_interval='0 22 * * *', default_args=args)


target_list = ['chain_store',
               'chain_goods',
               'chain_category',
               'goodsflow',
               'cost',
               'chain_goods_loss',
               ]

generate_common_task(source_id, cmid, erp_name, dag, target_list)


