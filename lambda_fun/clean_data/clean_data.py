# -*- coding: utf-8 -*-
"""
清洗逻辑的入口
"""
import json

import boto3
import pandas as pd
import pytz

S3_BUCKET = "ext-etl-data"

HISTORY_HUMP_JSON = (
    "datapipeline/source_id={source_id}/ext_date={date}/history_dump_json/"
)

CONVERTERS = {"str": str, "int": int, 'float': float}

_TZINFO = pytz.timezone("Asia/Shanghai")
S3 = boto3.resource("s3")


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """

    objects = S3.Bucket(bucket).objects.filter(Prefix=prefix)
    for obj in sorted(
            objects, key=lambda obj: int(obj.last_modified.strftime("%s")), reverse=True
    ):
        if obj.key.endswith(suffix):
            yield obj.key


def map_converter(converts: dict):
    for table, convert in converts.items():
        for column, value in convert.items():
            convert[column] = CONVERTERS[value]
        converts[table] = convert


def fetch_data_frames(keys, origin_table_columns, converts):
    datas = {}
    for key in keys:
        content = S3.Object(S3_BUCKET, key).get()
        data = json.loads(content["Body"].read().decode("utf-8"))
        extract_data_dict = data['extract_data']
        for table_name, records in extract_data_dict.items():
            if table_name in origin_table_columns.keys() and table_name not in datas.keys():
                datas[table_name] = records

    data_frames = {}

    for table, columns in origin_table_columns.items():
        frame_table = None
        for csv_path in datas[table]:
            key = f"s3://{S3_BUCKET}/{csv_path}"
            if table in converts:
                frame = pd.read_csv(
                    key, compression="gzip", usecols=columns, converters=converts[table]
                )
            else:
                frame = pd.read_csv(key, compression="gzip", usecols=columns)

            if frame_table is None:
                frame_table = frame.copy(deep=True)
            else:
                frame_table = frame_table.append(frame)
            data_frames[table] = frame_table

    return data_frames


def handler(event, context):
    # Check if the incoming message was sent by SNS
    if "Records" in event:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
    else:
        message = event

    source_id = message["source_id"]
    erp_name = message["erp_name"]
    date = message["date"]
    target_table = message["target_table"]
    origin_table_columns = message["origin_table_columns"]
    converts = message["converts"]

    map_converter(converts)

    keys = get_matching_s3_keys(
        S3_BUCKET,
        prefix=HISTORY_HUMP_JSON.format(source_id=source_id, date=date),
        suffix=".json",
    )

    data_frames = fetch_data_frames(keys, origin_table_columns, converts)

    if erp_name == "科脉云鼎":
        from kemaiyunding import clean_kemaiyunding

        return clean_kemaiyunding(source_id, date, target_table, data_frames)
    elif erp_name == "海鼎":
        from haiding import HaiDingCleaner

        cleaner = HaiDingCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name == "思迅":
        from sixun import clean_sixun
        return clean_sixun(source_id, date, target_table, data_frames)
    elif erp_name == "宏业":
        from hongye import HongYeCleaner
        cleaner = HongYeCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name == "美食林":
        from meishilin import MeiShiLinCleaner
        cleaner = MeiShiLinCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name == '智百威':
        from zhibaiwei import ZhiBaiWeiCleaner
        cleaner = ZhiBaiWeiCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name == '商海导航':
        from shanghaidaohang import ShangHaiDaoHangCleaner
        cleaner = ShangHaiDaoHangCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name == "富基融通":
        from chaoshifa import clean_chaoshifa

        return clean_chaoshifa(source_id, date, target_table, data_frames)
    elif erp_name == "便宅家中间库":
        from bianzhaijia import clean_bianzhaijia

        return clean_bianzhaijia(source_id, date, target_table, data_frames)
    elif erp_name == "海信商定天下":
        from haixin import clean_haixin

        return clean_haixin(source_id, date, target_table, data_frames)
    elif erp_name == "百年":
        from bainian import clean_bainian

        return clean_bainian(source_id, date, target_table, data_frames)
    elif erp_name == "九垠":
        from jiuyin import clean_jiuyin

        return clean_jiuyin(source_id, date, target_table, data_frames)


if __name__ == '__main__':
    event = {
        "source_id": "56YYYYYYYYYYYYY",
        "erp_name": "思迅",
        "date": "2018-09-06",
        "target_table": "goods_loss",
        'origin_table_columns': {
            "t_bd_item_cls": ['item_clsno', ],
            't_im_check_master': [
                'check_no', 'branch_no', 'sheet_no', 'oper_date', 'approve_flag'
            ],
            't_im_check_sum': ['item_no', 'sheet_no', 'branch_no', 'balance_qty', 'sale_price'],
            't_bd_branch_info': ['branch_no', 'branch_name'],
            't_bd_item_info': ['item_no', 'item_subno', 'item_name', 'unit_no', 'item_clsno'],

        },

        'converts': {
            "t_bd_item_cls": {'item_clsno': 'str'},
            't_im_check_master': {
                "check_no": "str",
                'branch_no': "str",
                'sheet_no': 'str',
                'oper_date': 'str',
                'approve_flag': 'str'
            },
            't_im_check_sum': {
                'item_no': 'str',
                'sheet_no': 'str',
                'branch_no': 'str',
                'balance_qty': 'str',
                'sale_price': 'float'
            },
            't_bd_branch_info': {
                'branch_no': 'str'
            },
            't_bd_item_info': {
                'item_no': 'str',
                'item_subno': 'str',
                'item_name': 'str',
                'unit_no': 'str',
                'item_clsno': 'str'
            },
        }
    }

    handler(event, None)
