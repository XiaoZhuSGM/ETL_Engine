# -*- coding: utf-8 -*-
"""
清洗逻辑的入口
"""
import json

import boto3
import pandas as pd
import pytz
import re

S3_BUCKET = "ext-etl-data"

HISTORY_HUMP_JSON = (
    "datapipeline/source_id={source_id}/ext_date={date}/history_dump_json/"
)

CONVERTERS = {"str": str, "int": int, "float": float}

BLANK_CHAR = re.compile(r"[\r\n\t]+")

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
        extract_data_dict = data["extract_data"]
        for table_name, records in extract_data_dict.items():
            if (
                table_name in origin_table_columns.keys()
                and table_name not in datas.keys()
            ):
                datas[table_name] = records

    data_frames = {}

    for table, columns in origin_table_columns.items():
        for csv_path in datas[table]:
            key = f"s3://{S3_BUCKET}/{csv_path}"
            if table in converts:
                frame = pd.read_csv(
                    key, compression="gzip", usecols=columns, converters=converts[table]
                )
            else:
                frame = pd.read_csv(key, compression="gzip", usecols=columns)

            if table in data_frames:
                data_frames[table] = data_frames[table].append(frame)
            else:
                data_frames[table] = frame.copy(deep=True)
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
    for k, v in data_frames.items():
        data_frames[k] = v.applymap(
            lambda e: BLANK_CHAR.sub(" ", e).strip() if isinstance(e, str) else e
        )

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
    elif erp_name == "智百威":
        from zhibaiwei import ZhiBaiWeiCleaner

        cleaner = ZhiBaiWeiCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name in ("商海导航", "商海（连锁版）"):
        from shanghaidaohang import ShangHaiDaoHangCleaner

        cleaner = ShangHaiDaoHangCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name == "富基融通":
        from fujirongtong import clean_fujirongtong

        return clean_fujirongtong(source_id, date, target_table, data_frames)
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

    elif erp_name == "晋中田森":
        from jinzhongtiansen import clean_jinzhong

        return clean_jinzhong(source_id, date, target_table, data_frames)

    elif erp_name == "易客来":
        from yikelai import HongYeCleaner

        cleaner = HongYeCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)

    elif erp_name == "海信商定天下v5":
        from haixinv5 import clean_haixin

        return clean_haixin(source_id, date, target_table, data_frames)

    elif erp_name == "超赢":
        from chaoying import clean_chaoying

        return clean_chaoying(source_id, date, target_table, data_frames)

    elif erp_name == "科脉御商v9":
        from kemaiyushang_v9 import clean_kemaiyushang

        return clean_kemaiyushang(source_id, date, target_table, data_frames)

    elif erp_name == "友琪":
        from youqi import YouQiCleaner

        cleaner = YouQiCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)
    elif erp_name == "衡阳联邦":
        from hengyanglianbang import clean_lianbang

        return clean_lianbang(source_id, date, target_table, data_frames)

    elif erp_name == "昂捷-中间库":
        from liuyi import LiuYiCleaner

        cleaner = LiuYiCleaner(source_id, date, data_frames)
        return cleaner.clean(target_table)


if __name__ == "__main__":

    event = {
        "source_id": "53YYYYYYYYYYYYY",
        "erp_name": "智百威",
        "date": "2019-01-11",
        "target_table": "goods_loss",
        "origin_table_columns": {
            "bi_t_branch_info": ["branch_no", "branch_name"],
            "bi_t_item_cls": ["item_clsno", "item_flag"],
            "bi_t_item_info": [
                "item_no",
                "item_clsno",
                "base_price",
                "item_subno",
                "barcode",
                "unit_no",
                "item_name",
            ],
            "ic_t_check_detail": ["sheet_no", "balance_qty", "item_no"],
            "ic_t_check_master": [
                "check_no",
                "oper_date",
                "sheet_no",
                "branch_no",
                "approve_flag",
                "del_flag",
            ],
        },
        "converts": {
            "bi_t_branch_info": {"branch_no": "str"},
            "bi_t_item_cls": {"item_clsno": "str", "item_flag": "str"},
            "bi_t_item_info": {
                "base_price": "str",
                "item_clsno": "str",
                "item_no": "str",
            },
            "ic_t_check_detail": {
                "balance_qty": "str",
                "item_no": "str",
                "sheet_no": "str",
            },
            "ic_t_check_master": {
                "approve_flag": "str",
                "branch_no": "str",
                "del_flag": "str",
                "sheet_no": "str",
            },
        },
    }
    handler(event, None)
