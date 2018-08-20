# -*- coding: utf-8 -*-
"""
清洗逻辑的入口
"""
import json
from collections import defaultdict

import boto3
import pandas as pd
import pytz

S3_BUCKET = "ext-etl-data"

HISTORY_HUMP_JSON = 'datapipeline/source_id={source_id}/ext_date={date}/history_dump_json/'

_TZINFO = pytz.timezone('Asia/Shanghai')
S3 = boto3.client("s3")


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        resp = S3.list_objects_v2(**kwargs)
        for obj in resp['Contents'][::-1]:  # 倒序
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def handler(event, context):
    # Check if the incoming message was sent by SNS
    if 'Records' in event:
        message = json.loads(event['Records'][0]['Sns']['Message'])
    else:
        message = event

    source_id = message["source_id"]
    erp_name = message["erp_name"]
    date = message["date"]
    target_table = message["target_table"]
    origin_table_columns = message["origin_table_columns"]
    converts = message["converts"]

    keys = get_matching_s3_keys(S3_BUCKET,
                                prefix=HISTORY_HUMP_JSON.format(source_id=source_id, date=date),
                                suffix='.json')

    # origin_table_columns = {"t_sl_master": ['fbrh_no', 'fflow_no', 'ftrade_date', 'fcr_time', 'fsell_way'],
    #                         "t_sl_detail": ['fprice', 'fpack_qty', 'famt', 'fflow_no', 'fitem_subno', 'fitem_id'],
    #                         "t_br_master": ['fbrh_name', 'fbrh_no'],
    #                         "t_bi_master": ['fitem_id', 'fitem_subno', 'fitem_name', 'funit_no', 'fitem_clsno'],
    #                         "t_bc_master": ['fitem_clsno', 'fitem_clsname', 'fprt_no'],
    #                         "t_bi_barcode": ['funit_qty', 'fitem_id', 'fitem_subno']}
    #
    # coverts = {"t_sl_master": {"fbrh_no": str}, "t_br_master": {"fbrh_no": str},
    #            "t_bi_master": {"fitem_clsno": str},
    #            "t_bc_master": {"fitem_clsno": str, "fprt_no": str}}

    datas = defaultdict(list)
    for key in keys:
        content = S3.get_object(Bucket=S3_BUCKET, Key=key)
        data = json.loads(content["Body"].read().decode("utf-8"))
        for table_info in data["extract_data"]:
            if table_info["table"] in origin_table_columns.keys() and table_info["table"] not in datas.keys():
                datas[table_info["table"]] = table_info["records"]

    data_frames = dict()

    for table, columns in origin_table_columns.items():
        frame_table = None
        for csv_path in datas[table]:
            key = "s3://" + S3_BUCKET + "/" + csv_path
            if table in converts:
                frame = pd.read_csv(key, compression="gzip", usecols=columns, converters=converts[table])
            else:
                frame = pd.read_csv(key, compression="gzip", usecols=columns)

            if frame_table is None:
                frame_table = frame.copy(deep=True)
            else:
                frame_table = frame_table.append(frame)
            data_frames[table] = frame_table

    if erp_name == "科脉云鼎":
        from lambda_fun.kemaiyunding import clean_kemaiyunding
        clean_kemaiyunding(source_id, date, target_table, data_frames)
        pass
    elif erp_name == "海鼎":
        pass
    elif erp_name == "思迅":
        from lambda_fun.sixun import clean_sixun
        clean_sixun(source_id, date, target_table, data_frames)


if __name__ == '__main__':
    store_event = {
        "source_id": "72YYYYYYYYYYYYY",
        "erp_name": "思迅",
        "date": "2018-08-13",
        "target_table": "store",
        'origin_table_columns': {
            "t_bd_branch_info": ['branch_no',
                                 'branch_name',
                                 'address',
                                 'dj_yw',
                                 'init_date',
                                 'branch_no',
                                 'branch_tel',
                                 'branch_fax',
                                 'other1',
                                 'trade_type',
                                 'property',
                                 ]
        },

        'converts': {"t_bd_branch_info": {'branch_no': str, 'property': int, 'trade_type': int, 'dj_yw': int}}
    }

    category_event = {
        "source_id": "72YYYYYYYYYYYYY",
        "erp_name": "思迅",
        "date": "2018-08-13",
        "target_table": "category",
        'origin_table_columns': {
            "t_bd_item_cls": ['item_clsno',
                              'item_clsname',
                              'cls_parent',
                              ]
        },

        'converts': {"t_bd_item_cls": {'item_clsno': str, 'item_clsname': str, 'cls_parent': str}}
    }

    goods_event = {
        "source_id": "72YYYYYYYYYYYYY",
        "erp_name": "思迅",
        "date": "2018-08-13",
        "target_table": "goods",
        'origin_table_columns': {
            't_bd_item_info': ['item_clsno',
                               'main_supcust',
                               'status',
                               'num2',
                               'item_no',
                               'item_name',
                               'price',
                               'sale_price',
                               'unit_no',
                               'item_subno',
                               'item_brandname',
                               'build_date'
                               ],
            't_bd_supcust_info': ['supcust_no', 'sup_name', 'supcust_flag'],
            "t_bd_item_cls": ['item_clsno']

        },

        'converts': {
            "t_bd_item_cls": {'item_clsno': str},
            't_bd_item_info': {'item_clsno': str, 'num2': float,
                               'main_supcust': str,
                               'status': int,
                               'item_no': str,

                               },
            't_bd_supcust_info': {'supcust_no': str}
        }
    }

    goodsflow_event = {
        "source_id": "72YYYYYYYYYYYYY",
        "erp_name": "思迅",
        "date": "2018-08-13",
        "target_table": "goodsflow",
        'origin_table_columns': {
            't_rm_saleflow': ['branch_no',
                              'item_no',
                              'sale_price',
                              'sale_qnty',
                              'sell_way',
                              'sale_money',
                              'flow_no',
                              'oper_date'],
            't_bd_branch_info': ['branch_no', 'branch_name'],
            't_bd_item_info': ['item_no', 'item_clsno', 'item_name', 'unit_no', ],
            't_bd_item_cls': ['item_clsno', 'item_clsname'],
        },

        'converts': {
            't_rm_saleflow': {'branch_no': str,
                              'item_no': str,
                              'sale_price': float,
                              'sale_qnty': float,
                              'sell_way': str,
                              'sale_money': float,
                              'flow_no': str
                              },

            't_bd_branch_info': {'branch_no': str},
            't_bd_item_info': {'item_no': str, 'item_clsno': str},
            't_bd_item_cls': {'item_clsno': str},
        }
    }

    cost_event = {
        "source_id": "72YYYYYYYYYYYYY",
        "erp_name": "思迅",
        "date": "2018-08-13",
        "target_table": "cost",
        'origin_table_columns': {
            "t_bd_item_cls": ['item_clsno', ],
            't_da_jxc_daysum': [
                'branch_no',
                'oper_date',
                'so_qty',
                'pos_qty',
                'so_amt',
                'pos_amt',
                'fifo_cost_amt',
                'item_no'
            ],
            't_bd_item_info': ['item_no', 'item_clsno']
        },

        'converts': {
            "t_bd_item_cls": {'item_clsno': str},
            't_bd_item_info': {'item_no': str, 'item_clsno': str},
            't_da_jxc_daysum': {
                'branch_no': str,
                'so_qty': float,
                'pos_qty': float,
                'so_amt': float,
                'pos_amt': float,
                'fifo_cost_amt': float,
                'item_no': str
            }
        }
    }

    handler(cost_event, None)
