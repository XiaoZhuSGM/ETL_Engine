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
source_id = '43YYYYYYYYYYYYY'
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
    'start_date': datetime(2018, 9, 4),
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True
}

dag = DAG(dag_id='ext_43', schedule_interval='0 22 * * *', default_args=args)

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

clean_requireorder = PythonOperator(
    task_id='clean_requireorder',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='requireorder',
                   target_table='requireorder')
)

clean_delivery = PythonOperator(
    task_id='clean_delivery',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='delivery',
                   target_table='delivery')
)

clean_purchase_warehouse = PythonOperator(
    task_id='clean_purchase_warehouse',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='purchase_warehouse',
                   target_table='purchase_warehouse')
)

clean_purchase_store = PythonOperator(
    task_id='clean_purchase_store',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='purchase_store',
                   target_table='purchase_store')
)

clean_move_store = PythonOperator(
    task_id='clean_move_store',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='move_store',
                   target_table='move_store')
)

clean_move_warehouse = PythonOperator(
    task_id='clean_move_warehouse',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='move_warehouse',
                   target_table='move_warehouse')
)

clean_check_warehouse = PythonOperator(
    task_id='clean_check_warehouse',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='check_warehouse',
                   target_table='check_warehouse')
)

clean_sales_promotion = PythonOperator(
    task_id='clean_sales_promotion',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='sales_promotion',
                   target_table='sales_promotion')
)

clean_goods_loss = PythonOperator(
    task_id='clean_goods_loss',
    dag=dag,
    python_callable=clean_common_function,
    op_kwargs=dict(source_id=source_id,
                   erp_name=erp_name,
                   target='chain_goods_loss',
                   target_table='goods_loss')
)

'''
    开始对每一步的清洗动作的结果进行记录日志
'''

recording_clean_store_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_store_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_store')
)

recording_clean_goods_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_goods_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_goods')
)

recording_clean_category_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_category_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_category')
)

recording_clean_goodsflow_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_goodsflow_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_goodsflow')
)

recording_clean_cost_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_cost_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_cost')
)

recording_clean_requireorder_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_requireorder_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_requireorder')
)

recording_clean_delivery_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_delivery_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_delivery')
)

recording_clean_purchase_warehouse_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_purchase_warehouse_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_purchase_warehouse')
)

recording_clean_purchase_store_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_purchase_store_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_purchase_store')
)

recording_clean_move_store_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_move_store_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_move_store')
)

recording_clean_move_warehouse_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_move_warehouse_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_move_warehouse')
)

recording_clean_check_warehouse_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_check_warehouse_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_check_warehouse')
)

recording_clean_sales_promotion_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_sales_promotion_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_sales_promotion')
)

recording_clean_goods_loss_log = PythonOperator(
    dag=dag,
    task_id='recording_clean_goods_loss_log',
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='clean_goods_loss')
)

recording_extract_data_log >> clean_store >> recording_clean_store_log
recording_extract_data_log >> clean_goods >> recording_clean_goods_log
recording_extract_data_log >> clean_category >> recording_clean_category_log
recording_extract_data_log >> clean_goodsflow >> recording_clean_goodsflow_log
recording_extract_data_log >> clean_cost >> recording_clean_cost_log

recording_extract_data_log >> clean_requireorder >> recording_clean_requireorder_log
recording_extract_data_log >> clean_delivery >> recording_clean_delivery_log
recording_extract_data_log >> clean_purchase_warehouse >> recording_clean_purchase_warehouse_log
recording_extract_data_log >> clean_purchase_store >> recording_clean_purchase_store_log
recording_extract_data_log >> clean_move_warehouse >> recording_clean_move_warehouse_log
recording_extract_data_log >> clean_move_store >> recording_clean_move_store_log
recording_extract_data_log >> clean_sales_promotion >> recording_clean_sales_promotion_log
recording_extract_data_log >> clean_goods_loss >> recording_clean_goods_loss_log
recording_extract_data_log >> clean_check_warehouse >> recording_clean_check_warehouse_log


"""
现在要开始进行执行入库逻辑,每个goods对应一套入库和记录日志
"""

DATA_KEY_TEMPLATE = "{S3_BUCKET}/{clean_data_file_path}"

load_store = PythonOperator(
    task_id='load_store',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_store',
                   target_table='chain_store'
                   )
)

recording_load_store = PythonOperator(
    task_id='recording_load_store',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_store')
)

recording_clean_store_log >> load_store >> recording_load_store

load_goods = PythonOperator(
    task_id='load_goods',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_goods',
                   target_table='chain_goods'
                   )
)

recording_load_goods = PythonOperator(
    task_id='recording_load_goods',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_goods')
)

recording_clean_goods_log >> load_goods >> recording_load_goods

load_category = PythonOperator(
    task_id='load_category',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_category',
                   target_table='chain_category'
                   )
)

recording_load_category = PythonOperator(
    task_id='recording_load_category',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_category')
)

recording_clean_category_log >> load_category >> recording_load_category

# goodsflow

load_goodsflow = PythonOperator(
    task_id='load_goodsflow',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_goodsflow',
                   target_table='goodsflow'
                   )
)

recording_load_goodsflow = PythonOperator(
    task_id='recording_load_goodsflow',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_goodsflow')
)

recording_clean_goodsflow_log >> load_goodsflow >> recording_load_goodsflow

load_cost = PythonOperator(
    task_id='load_cost',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_cost',
                   target_table='cost'
                   )
)

recording_load_cost = PythonOperator(
    task_id='recording_load_cost',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_cost')
)

recording_clean_cost_log >> load_cost >> recording_load_cost

# requireorder
load_requireorder = PythonOperator(
    task_id='load_requireorder',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_requireorder',
                   target_table='requireorder'
                   )
)

recording_load_requireorder = PythonOperator(
    task_id='recording_load_requireorder',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_requireorder')
)

recording_clean_requireorder_log >> load_requireorder >> recording_load_requireorder

# delivery

load_delivery = PythonOperator(
    task_id='load_delivery',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_delivery',
                   target_table='delivery'
                   )
)

recording_load_delivery = PythonOperator(
    task_id='recording_load_delivery',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_delivery')
)

recording_clean_delivery_log >> load_delivery >> recording_load_delivery

# purchase_warehouse

load_purchase_warehouse = PythonOperator(
    task_id='load_purchase_warehouse',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_purchase_warehouse',
                   target_table='purchase_warehouse'
                   )
)

recording_load_purchase_warehouse = PythonOperator(
    task_id='recording_load_purchase_warehouse',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_purchase_warehouse')
)

recording_clean_purchase_warehouse_log >> load_purchase_warehouse >> recording_load_purchase_warehouse

# purchase_store

load_purchase_store = PythonOperator(
    task_id='load_purchase_store',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_purchase_store',
                   target_table='purchase_store'
                   )
)

recording_load_purchase_store = PythonOperator(
    task_id='recording_load_purchase_store',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_purchase_store')
)

recording_clean_purchase_store_log >> load_purchase_store >> recording_load_purchase_store

# move_store

load_move_store = PythonOperator(
    task_id='load_move_store',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_move_store',
                   target_table='move_store'
                   )
)

recording_load_move_store = PythonOperator(
    task_id='recording_load_move_store',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_move_store')
)

recording_clean_move_store_log >> load_move_store >> recording_load_move_store

# move_warehouse

load_move_warehouse = PythonOperator(
    task_id='load_move_warehouse',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_move_warehouse',
                   target_table='move_warehouse'
                   )
)

recording_load_move_warehouse = PythonOperator(
    task_id='recording_load_move_warehouse',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_move_warehouse')
)

recording_clean_move_warehouse_log >> load_move_warehouse >> recording_load_move_warehouse

# check_warehouse

load_check_warehouse = PythonOperator(
    task_id='load_check_warehouse',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_check_warehouse',
                   target_table='check_warehouse'
                   )
)

recording_load_check_warehouse = PythonOperator(
    task_id='recording_load_check_warehouse',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_check_warehouse')
)

recording_clean_check_warehouse_log >> load_check_warehouse >> recording_load_check_warehouse

# sales_promotion

load_sales_promotion = PythonOperator(
    task_id='load_sales_promotion',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_sales_promotion',
                   target_table='sales_promotion'
                   )
)

recording_load_sales_promotion = PythonOperator(
    task_id='recording_load_sales_promotion',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_sales_promotion')
)

recording_clean_sales_promotion_log >> load_sales_promotion >> recording_load_sales_promotion

# chain_goods_loss

load_goods_loss = PythonOperator(
    task_id='load_goods_loss',
    dag=dag,
    python_callable=load_common_function,
    op_kwargs=dict(cmid=cmid,
                   source_id=source_id,
                   task_id='clean_goods_loss',
                   target_table='chain_goods_loss'
                   )
)

recording_load_goods_loss = PythonOperator(
    task_id='recording_load_goods_loss',
    dag=dag,
    python_callable=recording_clean_common_log_function,
    op_kwargs=dict(task_id='load_goods_loss')
)

recording_clean_goods_loss_log >> load_goods_loss >> recording_load_goods_loss
