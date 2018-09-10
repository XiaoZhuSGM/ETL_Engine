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



source_id = '67YYYYYYYYYYYYY'
erp_name = '海鼎'
cmid = source_id.split("Y")[0]
SQL_PREFIX = 'sql/source_id={source_id}/{date}/'
S3_BUCKET = 'ext-etl-data'

# 调用web接口来生成source_id的指定cron表达式
interval = generate_crontab(source_id)
args = {
    'owner': 'ETL',
    'depends_on_past': False,
    'email': ['lvxiang@chaomengdata.com', 'fanjianan@chaomengdata.com', 'guojiaqi@chaomengdata.com', 'yumujun@chaomengdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

dag = DAG(dag_id='ext_67', schedule_interval=interval, default_args=args)


target_list = ['chain_store',
               'chain_goods',
               'chain_category',
               'goodsflow',
               'cost',
               'chain_goods_loss',
               ]

generate_common_task(source_id, cmid, erp_name, dag, target_list)

