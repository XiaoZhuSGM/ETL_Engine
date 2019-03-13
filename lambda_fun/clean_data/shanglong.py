"""
77好邻居中间库清洗逻辑
销售，成本，库存，商品， 分类
"""
from datetime import datetime
import pandas as pd
import boto3
import tempfile
import pytz
import time
import numpy as np

_TZINFO = pytz.timezone("Asia/Shanghai")

S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")

CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


def clean_shanglong(source_id, date, target_table, data_frames):
    if target_table == "goodsflow":
        return clean_goodsflow(source_id, date, target_table, data_frames)
    elif target_table == "cost":
        return clean_cost(source_id, date, target_table, data_frames)
    elif target_table == "store":
        return clean_store(source_id, date, target_table, data_frames)
    elif target_table == "goods":
        return clean_goods(source_id, date, target_table, data_frames)
    elif target_table == "category":
        return clean_category(source_id, date, target_table, data_frames)
    else:
        pass


def clean_goodsflow(source_id, date, target_table, data_frames):
    """
    清洗销售流水
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
    """
    columns = [
        'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime',
        'last_updated',
        'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
        'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
        'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
        'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id']
    cmid = source_id.split("Y")[0]
    flow_frames = data_frames["hs_posdataa_history"].rename(columns=lambda x: f"flow.{x}")
    store_frames = data_frames["f_shop"].rename(columns=lambda x: f"store.{x}")
    item_frames = data_frames["f_thing_base"].rename(columns=lambda x: f"item.{x}")
    lv_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv.{x}")
    lv1_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv3.{x}")
    lv4_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv4.{x}")
    lv_frames["lv1.dkindno"] = lv_frames["lv.dkindno"].apply(lambda x: x[:1])
    lv_frames["lv2.dkindno"] = lv_frames["lv.dkindno"].apply(lambda x: x[:3])
    lv_frames["lv3.dkindno"] = lv_frames["lv.dkindno"].apply(lambda x: x[:5])
    lv_frames["lv4.dkindno"] = lv_frames["lv.dkindno"].apply(lambda x: x[:7])

    if len(flow_frames) == 0:
        goodsflow_frames = pd.DataFrame(columns=columns)
    else:
        goodsflow_frames = flow_frames.merge(store_frames, how="left", left_on="flow.dsubshop",
                                             right_on="store.dshopno") \
            .merge(item_frames, how="left", left_on="flow.dthingcode", right_on="item.dthingcode") \
            .merge(lv_frames, how="left", left_on="flow.dkindcode", right_on="lv.dkindcode") \
            .merge(lv1_frames, how="left", on="lv1.dkindno") \
            .merge(lv2_frames, how="left", on="lv2.dkindno") \
            .merge(lv3_frames, how="left", on="lv3.dkindno") \
            .merge(lv4_frames, how="left", on="lv4.dkindno")

    goodsflow_frames["source_id"] = source_id
    goodsflow_frames["cmid"] = cmid
    goodsflow_frames["consumer_id"] = ""
    goodsflow_frames["last_updated"] = datetime.now(_TZINFO)
    goodsflow_frames["foreign_category_lv1"] = goodsflow_frames["lv1.dkindno"].fillna("无")
    goodsflow_frames["foreign_category_lv2"] = goodsflow_frames["lv2.dkindno"].fillna("无")
    goodsflow_frames["foreign_category_lv3"] = goodsflow_frames["lv3.dkindno"].fillna("无")
    goodsflow_frames["foreign_category_lv4"] = goodsflow_frames["lv4.dkindno"].fillna("无")
    goodsflow_frames["foreign_category_lv5"] = goodsflow_frames["lv.dkindno"].fillna("无")
    goodsflow_frames["pos_id"] = ""
    goodsflow_frames["item_name"] = goodsflow_frames["item.dfullname"].fillna("无名称")

    goodsflow_frames = goodsflow_frames.rename(columns={
        "store.dshopno": "foreign_store_id",
        "store.dshopname": "store_name",
        "flow.dno": "receipt_id",
        "flow.ddate": "saletime",
        "flow.dthingcode": "foreign_item_id",
        "flow.dbarcode": "barcode",
        "item.dunitname": "item_unit",
        "flow.dprice": "saleprice",
        "flow.dnum": "quantity",
        "flow.dmoney_ss": "subtotal",
        "lv1.dkindname": "foreign_category_lv1_name",
        "lv2.dkindname": "foreign_category_lv2_name",
        "lv3.dkindname": "foreign_category_lv3_name",
        "lv4.dkindname": "foreign_category_lv4_name",
        "lv.dkindname": "foreign_category_lv5_name"
    })

    goodsflow_frames = goodsflow_frames[columns]
    return upload_to_s3(goodsflow_frames, source_id, date, target_table)


def clean_cost(source_id, date, target_table, data_frames):
    """
    清洗成本
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    columns = [
        "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
        "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3",
        "foreign_category_lv4", "foreign_category_lv5", "cmid"]
    cmid = source_id.split("Y")[0]
    cost = data_frames["sale_thing"].rename(columns=lambda x: f"cost.{x}")
    lv = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv.{x}")

    if len(cost) == 0:
        cost_frames = pd.DataFrame(columns=columns)
    else:
        cost_frames = cost.merge(lv, how="left", left_on="cost.dkindcode", right_on="lv.dkindcode")
        cost_frames["foreign_category_lv1"] = cost_frames["lv.dkindno"].apply(lambda x: x[:1])
        cost_frames["foreign_category_lv2"] = cost_frames["lv.dkindno"].apply(lambda x: x[:3])
        cost_frames["foreign_category_lv3"] = cost_frames["lv.dkindno"].apply(lambda x: x[:5])
        cost_frames["foreign_category_lv4"] = cost_frames["lv.dkindno"].apply(lambda x: x[:7])
        cost_frames["foreign_category_lv5"] = cost_frames["lv.dkindno"]
        cost_frames["source_id"] = source_id
        cost_frames["cmid"] = cmid

        cost_frames = cost_frames.rename(columns={
            "cost.dshopno": "foreign_store_id",
            "cost.dthingcode": "foreign_item_id",
            "cost.ddate": "date",
            "cost.dsalemode": "cost_type",
            "cost.dnum": "total_quantity",
            "cost.dmoney_ss": "total_sale",
            "cost.dmoney_in": "total_cost"
        })
        cost_frames["date"] = cost_frames["date"].apply(lambda row: row.split()[0])
        cost_frames = cost_frames[columns]

    return upload_to_s3(cost_frames, source_id, date, target_table)


def clean_goods(source_id, date, target_table, data_frames):
    """
    清洗商品
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]

    item_frames = data_frames["f_thing_base"].rename(columns=lambda x: f"item.{x}")
    lv_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv.{x}")
    brand_frames = data_frames["f_breed"].rename(columns=lambda x: f"brand.{x}")

    goods_frames = (item_frames.merge(lv_frames, how="left", left_on="item.dkindcode", right_on="lv.dkindcode")
                    .merge(brand_frames, how="left", left_on="item.dbreedcode", right_on="brand.dbreedcode"))

    goods_frames["foreign_category_lv1"] = goods_frames["lv.dkindno"].apply(lambda x: x[:1])
    goods_frames["foreign_category_lv2"] = goods_frames["lv.dkindno"].apply(lambda x: x[:3])
    goods_frames["foreign_category_lv3"] = goods_frames["lv.dkindno"].apply(lambda x: x[:5])
    goods_frames["foreign_category_lv4"] = goods_frames["lv.dkindno"].apply(lambda x: x[:7])
    goods_frames["foreign_category_lv5"] = goods_frames["lv.dkindno"]

    goods_frames["cmid"] = cmid
    goods_frames["storage_time"] = datetime.now(_TZINFO)
    goods_frames["last_updated"] = datetime.now(_TZINFO)
    goods_frames["lastin_price"] = ""
    goods_frames["sale_price"] = ""
    goods_frames["isvalid"] = ""
    goods_frames["warranty"] = ""
    goods_frames["supplier_name"] = ""
    goods_frames["supplier_code"] = ""
    goods_frames["allot_method"] = ""
    goods_frames["show_code"] = goods_frames["item.dthingcode"]

    goods_frames = goods_frames.rename(columns={
        "item.dmasterbarcode": "barcode",
        "item.dthingcode": "foreign_item_id",
        "item.dfullname": "item_name",
        "item.dunitname": "item_unit",
        "item.dthingtype": "item_status",
        "brand.dbreedname": "brand_name"
    })

    goods_frames = goods_frames[[
        "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
        "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4", "storage_time",
        "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method", "supplier_name",
        "supplier_code",
        "brand_name",
    ]]

    return upload_to_s3(goods_frames, source_id, date, target_table)


def clean_category(source_id, date, target_table, data_frames):
    """
    分类清洗
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    columns = [
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]
    cmid = source_id.split("Y")[0]

    lv1_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv3.{x}")
    lv4_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv4.{x}")
    lv5_frames = data_frames["f_thing_kind"].rename(columns=lambda x: f"lv5.{x}")

    category1 = lv1_frames.copy()
    category1 = category1[(category1["lv1.dlevel"] == 1) & (category1["lv1.dkindno"].str.len() == 1)]
    category1["cmid"] = cmid
    category1["level"] = 1
    category1["foreign_category_lv2"] = ""
    category1["foreign_category_lv2_name"] = ""
    category1["foreign_category_lv3"] = ""
    category1["foreign_category_lv3_name"] = ""
    category1["foreign_category_lv4"] = ""
    category1["foreign_category_lv4_name"] = ""
    category1["foreign_category_lv5"] = ""
    category1["foreign_category_lv5_name"] = ""
    category1["last_updated"] = datetime.now(_TZINFO)
    category1 = category1.rename(columns={
        "lv1.dkindno": "foreign_category_lv1",
        "lv1.dkindname": "foreign_category_lv1_name"
    })
    category1 = category1[columns]
    lv2_frames_sub = lv2_frames.copy()
    lv2_frames_sub["lv2.dkindno_sub"] = lv2_frames_sub["lv2.dkindno"].apply(lambda x: x[:1])

    category2 = lv2_frames_sub.merge(lv1_frames.copy(), how="left", left_on="lv2.dkindno_sub", right_on="lv1.dkindno")
    category2 = category2[category2["lv2.dlevel"] == 2]
    category2 = category2[category2["lv2.dkindno"].str.len() == 3]
    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = ""
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = ""
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = ""
    category2["last_updated"] = datetime.now(_TZINFO)
    category2 = category2.rename(columns={
        "lv1.dkindno": "foreign_category_lv1",
        "lv1.dkindname": "foreign_category_lv1_name",
        "lv2.dkindno": "foreign_category_lv2",
        "lv2.dkindname": "foreign_category_lv2_name",
    })
    category2 = category2[columns]

    lv3_frames_sub = lv3_frames.copy()
    lv3_frames_sub["lv3.dkindno_sub"] = lv3_frames_sub["lv3.dkindno"].apply(lambda x: x[:3])

    category3 = (lv3_frames_sub.merge(lv2_frames_sub, how="left", left_on="lv3.dkindno_sub", right_on="lv2.dkindno")
                 .merge(lv1_frames.copy(), how="left", left_on="lv2.dkindno_sub", right_on="lv1.dkindno"))
    category3 = category3[(category3["lv3.dlevel"] == 3) & (category3["lv3.dkindno"].str.len() == 5)]
    category3["cmid"] = cmid
    category3["level"] = 3
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = ""
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = ""
    category3["last_updated"] = datetime.now(_TZINFO)
    category3 = category3.rename(columns={
        "lv1.dkindno": "foreign_category_lv1",
        "lv1.dkindname": "foreign_category_lv1_name",
        "lv2.dkindno": "foreign_category_lv2",
        "lv2.dkindname": "foreign_category_lv2_name",
        "lv3.dkindno": "foreign_category_lv3",
        "lv3.dkindname": "foreign_category_lv3_name",
    })
    category3 = category3[columns]

    lv4_frames_sub = lv4_frames.copy()
    lv4_frames_sub["lv4.dkindno_sub"] = lv4_frames_sub["lv4.dkindno"].apply(lambda x: x[:5])
    category4 = (lv4_frames_sub.merge(lv3_frames_sub, how="left", left_on="lv4.dkindno_sub", right_on="lv3.dkindno")
                 .merge(lv2_frames_sub, how="left", left_on="lv3.dkindno_sub", right_on="lv2.dkindno")
                 .merge(lv1_frames.copy(), how="left", left_on="lv2.dkindno_sub", right_on="lv1.dkindno"))

    category4 = category4[(category4["lv4.dlevel"] == 4) & (category4["lv4.dkindno"].str.len() == 7)]
    category4["cmid"] = cmid
    category4["level"] = 4
    category4["foreign_category_lv5"] = ""
    category4["foreign_category_lv5_name"] = ""
    category4["last_updated"] = datetime.now(_TZINFO)
    category4 = category4.rename(columns={
        "lv1.dkindno": "foreign_category_lv1",
        "lv1.dkindname": "foreign_category_lv1_name",
        "lv2.dkindno": "foreign_category_lv2",
        "lv2.dkindname": "foreign_category_lv2_name",
        "lv3.dkindno": "foreign_category_lv3",
        "lv3.dkindname": "foreign_category_lv3_name",
        "lv4.dkindno": "foreign_category_lv4",
        "lv4.dkindname": "foreign_category_lv4_name",

    })
    category4 = category4[columns]

    lv5_frames_sub = lv5_frames.copy()
    lv5_frames_sub["lv5.dkindno_sub"] = lv5_frames_sub["lv5.dkindno"].apply(lambda x: x[:7])
    category5 = (lv5_frames_sub.merge(lv4_frames_sub, how="left", left_on="lv5.dkindno_sub", right_on="lv4.dkindno")
                 .merge(lv3_frames_sub, how="left", left_on="lv4.dkindno_sub", right_on="lv3.dkindno")
                 .merge(lv2_frames_sub, how="left", left_on="lv3.dkindno_sub", right_on="lv2.dkindno")
                 .merge(lv1_frames.copy(), how="left", left_on="lv2.dkindno_sub", right_on="lv1.dkindno"))
    category5 = category5[(category5["lv5.dlevel"] == 5) & (category5["lv5.dkindno"].str.len() == 9)]
    category5["cmid"] = cmid
    category5["level"] = 5
    category5["last_updated"] = datetime.now(_TZINFO)
    category5 = category5.rename(columns={
        "lv1.dkindno": "foreign_category_lv1",
        "lv1.dkindname": "foreign_category_lv1_name",
        "lv2.dkindno": "foreign_category_lv2",
        "lv2.dkindname": "foreign_category_lv2_name",
        "lv3.dkindno": "foreign_category_lv3",
        "lv3.dkindname": "foreign_category_lv3_name",
        "lv4.dkindno": "foreign_category_lv4",
        "lv4.dkindname": "foreign_category_lv4_name",
        "lv5.dkindno": "foreign_category_lv5",
        "lv5.dkindname": "foreign_category_lv5_name",

    })
    category5 = category5[columns]
    category = pd.concat([category1, category2, category3, category4, category5])

    return upload_to_s3(category, source_id, date, target_table)


def clean_store(source_id, date, target_table, data_frames):
    """
    门店清洗
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    store_frames = data_frames["f_shop"]
    store_frames["cmid"] = cmid
    store_frames["source_id"] = source_id
    store_frames["address_code"] = ""
    store_frames["device_id"] = ""
    store_frames["area_code"] = ""
    store_frames["area_name"] = ""
    store_frames["business_area"] = ""
    store_frames["property_id"] = ""
    store_frames["lat"] = None
    store_frames["lng"] = None
    store_frames["last_updated"] = datetime.now(_TZINFO)
    store_frames["show_code"] = store_frames["dshopno"]

    def generate_status_method(x):
        if x == "1":
            return "正常"
        elif x == "0":
            return "闭店"

    store_frames["store_status"] = store_frames["disused"].apply(generate_status_method)

    store_frames = store_frames.rename(columns={
        "dshopno": "foreign_store_id",
        "dshopname": "store_name",
        "daddress": "store_address",
        "dpracticedate": "create_date",
        "dphone": "phone_number",
        "dzjm": "contacts",
        "dshoptype": "property",
    })
    store_frames = store_frames[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    return upload_to_s3(store_frames, source_id, date, target_table)


def upload_to_s3(frame, source_id, date, target_table):
    filename = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
    count = len(frame)
    frame.to_csv(filename.name, index=False, compression="gzip", float_format='%.4f')
    filename.seek(0)
    key = CLEANED_PATH.format(
        source_id=source_id,
        target_table=target_table,
        date=date,
        timestamp=now_timestamp(),
        rowcount=count,
    )
    S3.Bucket(S3_BUCKET).upload_file(filename.name, key)
    return key


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp
