"""
海信商定天下清洗逻辑 参照cmid 64的sql
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

ALLOT_METHOD = ["52"]


def clean_haixin(source_id, date, target_table, data_frames):
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


def clean_goodsflow_52(source_id, date, target_table, data_frames):
    columns = [
        "source_id",
        "cmid",
        "foreign_store_id",
        "store_name",
        "receipt_id",
        "consumer_id",
        "saletime",
        "last_updated",
        "foreign_item_id",
        "barcode",
        "item_name",
        "item_unit",
        "saleprice",
        "quantity",
        "subtotal",
        "foreign_category_lv1",
        "foreign_category_lv1_name",
        "foreign_category_lv2",
        "foreign_category_lv2_name",
        "foreign_category_lv3",
        "foreign_category_lv3_name",
        "foreign_category_lv4",
        "foreign_category_lv4_name",
        "foreign_category_lv5",
        "foreign_category_lv5_name",
        "pos_id",
    ]

    head = data_frames["tsalsale"]
    detail = data_frames["tsalsaleplu"]
    store = data_frames["torgmanage"]
    item = data_frames["tskuplu"]
    category = data_frames["tcatcategory"]

    if not len(head):
        return upload_to_s3(
            pd.DataFrame(columns=columns), source_id, date, target_table
        )

    category["clscode_lv2"] = category.apply(lambda row: row["clscode"][:2], axis=1)

    result = (
        head.merge(detail, how="inner", on=["orgcode", "saleno"])
        .merge(store, on="orgcode", how="inner")
        .merge(item, on="pluid", how="inner")
        .merge(category, on="clsid")
        .merge(
            category, left_on="clscode_lv2", right_on="clscode", suffixes=("", "_lv1")
        )
    )

    result = result[(result["isactive_lv1"] == "1") & (result["trantype"] != "5")]

    if not len(result):
        return upload_to_s3(
            pd.DataFrame(columns=columns), source_id, date, target_table
        )
    else:
        cmid = source_id.split("Y")[0]
        result["source_id"] = source_id
        result["cmid"] = cmid
        result["consumer_id"] = ""
        result["last_updated"] = datetime.now(_TZINFO)
        result["foreign_category_lv3"] = ""
        result["foreign_category_lv3_name"] = None
        result["foreign_category_lv4"] = ""
        result["foreign_category_lv4_name"] = None
        result["foreign_category_lv5"] = ""
        result["foreign_category_lv5_name"] = None
        result["pos_id"] = ""

        result = result.rename(
            columns={
                "orgcode": "foreign_store_id",
                "orgname": "store_name",
                "saleno": "receipt_id",
                "xsdate": "saletime",
                "pluid": "foreign_item_id",
                "barcode": "barcode",
                "pluname": "item_name",
                "unit": "item_unit",
                "price": "saleprice",
                "xscount": "quantity",
                "sstotal": "subtotal",
                "clscode_lv1": "foreign_category_lv1",
                "clsname_lv1": "foreign_category_lv1_name",
                "clscode": "foreign_category_lv2",
                "clsname": "foreign_category_lv2_name",
            }
        )

        return upload_to_s3(result[columns], source_id, date, target_table)


def clean_goodsflow_64(source_id, date, target_table, data_frames):
    """
    清洗销售流水
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
    """

    cmid = source_id.split("Y")[0]
    head_frames = data_frames["tsalsale"].rename(columns=lambda x: f"head.{x}")
    detail_frames = data_frames["tsalsaleplu"].rename(columns=lambda x: f"detail.{x}")
    stores_frames = data_frames["torgmanage"].rename(columns=lambda x: f"stores.{x}")
    item_frames = data_frames["tskuplu"].rename(columns=lambda x: f"item.{x}")
    lv3_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv3.{x}")
    lv2_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv2.{x}")
    lv1_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv1.{x}")
    goodsflow1 = (
        head_frames.merge(
            detail_frames,
            left_on=["head.saleno", "head.orgcode"],
            right_on=["detail.saleno", "detail.orgcode"],
        )
        .merge(stores_frames, left_on="head.orgcode", right_on="stores.orgcode")
        .merge(item_frames, left_on="detail.pluid", right_on="item.pluid")
    )

    if len(goodsflow1) == 0:
        goodsflow1 = pd.DataFrame(
            columns=[
                "source_id",
                "cmid",
                "foreign_store_id",
                "store_name",
                "receipt_id",
                "consumer_id",
                "saletime",
                "last_updated",
                "foreign_item_id",
                "barcode",
                "item_name",
                "item_unit",
                "saleprice",
                "quantity",
                "subtotal",
                "foreign_category_lv1",
                "foreign_category_lv1_name",
                "foreign_category_lv2",
                "foreign_category_lv2_name",
                "foreign_category_lv3",
                "foreign_category_lv3_name",
                "foreign_category_lv4",
                "foreign_category_lv4_name",
                "foreign_category_lv5",
                "foreign_category_lv5_name",
                "pos_id",
            ]
        )
    else:

        goodsflow1 = goodsflow1.merge(
            lv3_frames, how="left", left_on="item.clsid", right_on="lv3.clsid"
        )
        goodsflow1 = goodsflow1[goodsflow1["lv3.isactive"] == "1"]
        goodsflow1["lv2.clscode"] = goodsflow1["lv3.clscode"].apply(lambda x: x[:4])
        goodsflow1["lv1.clscode"] = goodsflow1["lv3.clscode"].apply(lambda x: x[:2])
        goodsflow1 = goodsflow1.merge(lv2_frames, how="left", on="lv2.clscode")
        goodsflow1 = goodsflow1[goodsflow1["lv2.isactive"] == "1"]
        goodsflow1 = goodsflow1.merge(lv1_frames, how="left", on="lv1.clscode")
        goodsflow1 = goodsflow1[goodsflow1["lv1.isactive"] == "1"]
        goodsflow1 = goodsflow1[
            (goodsflow1["head.trantype"] != "5")
            & (goodsflow1["lv3.clscode"].map(len) == 6)
            & (goodsflow1["lv3.islast"] == "1")
        ]

        goodsflow1["source_id"] = source_id
        goodsflow1["cmid"] = cmid
        goodsflow1["consumer_id"] = ""
        goodsflow1["last_updated"] = datetime.now(_TZINFO)
        goodsflow1["foreign_category_lv4"] = ""
        goodsflow1["foreign_category_lv4_name"] = None
        goodsflow1["foreign_category_lv5"] = ""
        goodsflow1["foreign_category_lv5_name"] = None
        goodsflow1["pos_id"] = ""

        goodsflow1 = goodsflow1.rename(
            columns={
                "stores.orgcode": "foreign_store_id",
                "stores.orgname": "store_name",
                "head.saleno": "receipt_id",
                "head.xsdate": "saletime",
                "item.pluid": "foreign_item_id",
                "item.barcode": "barcode",
                "item.pluname": "item_name",
                "item.unit": "item_unit",
                "detail.price": "saleprice",
                "detail.xscount": "quantity",
                "detail.sstotal": "subtotal",
                "lv1.clscode": "foreign_category_lv1",
                "lv1.clsname": "foreign_category_lv1_name",
                "lv2.clscode": "foreign_category_lv2",
                "lv2.clsname": "foreign_category_lv2_name",
                "lv3.clscode": "foreign_category_lv3",
                "lv3.clsname": "foreign_category_lv3_name",
            }
        )
        goodsflow1["foreign_category_lv1"] = goodsflow1["foreign_category_lv1"].apply(
            lambda x: x.zfill(2)
        )
        goodsflow1["foreign_category_lv2"] = goodsflow1["foreign_category_lv2"].apply(
            lambda x: x.zfill(4)
        )
        goodsflow1["foreign_category_lv3"] = goodsflow1["foreign_category_lv3"].apply(
            lambda x: x.zfill(6)
        )
        goodsflow1 = goodsflow1[
            [
                "source_id",
                "cmid",
                "foreign_store_id",
                "store_name",
                "receipt_id",
                "consumer_id",
                "saletime",
                "last_updated",
                "foreign_item_id",
                "barcode",
                "item_name",
                "item_unit",
                "saleprice",
                "quantity",
                "subtotal",
                "foreign_category_lv1",
                "foreign_category_lv1_name",
                "foreign_category_lv2",
                "foreign_category_lv2_name",
                "foreign_category_lv3",
                "foreign_category_lv3_name",
                "foreign_category_lv4",
                "foreign_category_lv4_name",
                "foreign_category_lv5",
                "foreign_category_lv5_name",
                "pos_id",
            ]
        ]

    goodsflow2 = (
        head_frames.merge(
            detail_frames,
            left_on=["head.saleno", "head.orgcode"],
            right_on=["detail.saleno", "detail.orgcode"],
        )
        .merge(stores_frames, left_on="head.orgcode", right_on="stores.orgcode")
        .merge(item_frames, left_on="detail.pluid", right_on="item.pluid")
        .merge(lv3_frames, how="left", left_on="item.clsid", right_on="lv3.clsid")
    )

    if len(goodsflow2) == 0:
        goodsflow2 = pd.DataFrame(
            columns=[
                "source_id",
                "cmid",
                "foreign_store_id",
                "store_name",
                "receipt_id",
                "consumer_id",
                "saletime",
                "last_updated",
                "foreign_item_id",
                "barcode",
                "item_name",
                "item_unit",
                "saleprice",
                "quantity",
                "subtotal",
                "foreign_category_lv1",
                "foreign_category_lv1_name",
                "foreign_category_lv2",
                "foreign_category_lv2_name",
                "foreign_category_lv3",
                "foreign_category_lv3_name",
                "foreign_category_lv4",
                "foreign_category_lv4_name",
                "foreign_category_lv5",
                "foreign_category_lv5_name",
                "pos_id",
            ]
        )
    else:
        goodsflow2 = goodsflow2.merge(
            lv2_frames, how="left", left_on="item.clsid", right_on="lv2.clsid"
        )
        goodsflow2["lv1.clscode"] = goodsflow2["lv2.clscode"].apply(lambda x: x[:2])
        goodsflow2 = goodsflow2.merge(lv1_frames, how="left", on="lv1.clscode")
        goodsflow2 = goodsflow2[goodsflow2["lv1.isactive"] == "1"]

        goodsflow2 = goodsflow2[
            (goodsflow2["head.trantype"] != "5")
            & (goodsflow2["lv3.clscode"].map(len) == 4)
            & (goodsflow2["lv3.islast"] == "1")
        ]

        goodsflow2["source_id"] = source_id
        goodsflow2["cmid"] = cmid
        goodsflow2["consumer_id"] = ""
        goodsflow2["last_updated"] = datetime.now(_TZINFO)
        goodsflow2["foreign_category_lv3"] = ""
        goodsflow2["foreign_category_lv3_name"] = None
        goodsflow2["foreign_category_lv4"] = ""
        goodsflow2["foreign_category_lv4_name"] = None
        goodsflow2["foreign_category_lv5"] = ""
        goodsflow2["foreign_category_lv5_name"] = None
        goodsflow2["pos_id"] = ""

        goodsflow2 = goodsflow2.rename(
            columns={
                "stores.orgcode": "foreign_store_id",
                "stores.orgname": "store_name",
                "head.saleno": "receipt_id",
                "head.xsdate": "saletime",
                "item.pluid": "foreign_item_id",
                "item.barcode": "barcode",
                "item.pluname": "item_name",
                "item.unit": "item_unit",
                "detail.price": "saleprice",
                "detail.xscount": "quantity",
                "detail.sstotal": "subtotal",
                "lv1.clscode": "foreign_category_lv1",
                "lv1.clsname": "foreign_category_lv1_name",
                "lv2.clscode": "foreign_category_lv2",
                "lv2.clsname": "foreign_category_lv2_name",
            }
        )
        goodsflow2["foreign_category_lv1"] = goodsflow2["foreign_category_lv1"].apply(
            lambda x: x.zfill(2)
        )
        goodsflow2["foreign_category_lv2"] = goodsflow2["foreign_category_lv2"].apply(
            lambda x: x.zfill(4)
        )
        goodsflow2 = goodsflow2[
            [
                "source_id",
                "cmid",
                "foreign_store_id",
                "store_name",
                "receipt_id",
                "consumer_id",
                "saletime",
                "last_updated",
                "foreign_item_id",
                "barcode",
                "item_name",
                "item_unit",
                "saleprice",
                "quantity",
                "subtotal",
                "foreign_category_lv1",
                "foreign_category_lv1_name",
                "foreign_category_lv2",
                "foreign_category_lv2_name",
                "foreign_category_lv3",
                "foreign_category_lv3_name",
                "foreign_category_lv4",
                "foreign_category_lv4_name",
                "foreign_category_lv5",
                "foreign_category_lv5_name",
                "pos_id",
            ]
        ]

    goodsflow_frames = pd.concat([goodsflow1, goodsflow2])
    return upload_to_s3(goodsflow_frames, source_id, date, target_table)


def clean_goodsflow(source_id, date, target_table, data_frames):
    if source_id == "52YYYYYYYYYYYYY":
        return clean_goodsflow_52(source_id, date, target_table, data_frames)
    else:
        return clean_goodsflow_64(source_id, date, target_table, data_frames)


def clean_cost_52(source_id, date, target_table, data_frames):
    columns = [
        "source_id",
        "foreign_store_id",
        "foreign_item_id",
        "date",
        "cost_type",
        "total_quantity",
        "total_sale",
        "total_cost",
        "foreign_category_lv1",
        "foreign_category_lv2",
        "foreign_category_lv3",
        "foreign_category_lv4",
        "foreign_category_lv5",
        "cmid",
    ]

    cost = data_frames["tsalpludetail"]
    if not len(cost):
        return upload_to_s3(
            pd.DataFrame(columns=columns), source_id, date, target_table
        )

    item = data_frames["tskuplu"]
    category = data_frames["tcatcategory"]
    category["clscode_lv2"] = category.apply(lambda row: row["clscode"][:2], axis=1)
    result = (
        cost.merge(item, on="pluid", how="inner")
        .merge(category, on="clsid", how="left")
        .merge(
            category,
            left_on="clscode_lv2",
            right_on="clscode",
            how="left",
            suffixes=("", "_lv1"),
        )
    )

    result = result[(result["isactive_lv1"] == "1") & (result["orgcode"] != "00")]

    if not len(result):
        return upload_to_s3(
            pd.DataFrame(columns=columns), source_id, date, target_table
        )
    else:
        result["source_id"] = source_id
        result["cost_type"] = ""
        result["foreign_category_lv3"] = ""
        result["foreign_category_lv4"] = ""
        result["foreign_category_lv5"] = ""
        cmid = source_id.split("Y")[0]
        result["cmid"] = cmid
        result = result.rename(
            columns={
                "orgcode": "foreign_store_id",
                "pluid": "foreign_item_id",
                "rptdate": "date",
                "xscount": "total_quantity",
                "hxtotal": "total_sale",
                "hjcost": "total_cost",
                "clscode_lv1": "foreign_category_lv1",
                "clscode": "foreign_category_lv2",
            }
        )
        result["date"] = result["date"].apply(lambda row: row.split()[0])
        result = result[columns]

        return upload_to_s3(result, source_id, date, target_table)


def clean_cost_64(source_id, date, target_table, data_frames):
    """
    清洗成本
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    cost_frames = data_frames["tsalpludetail"].rename(columns=lambda x: f"cost.{x}")
    item_frames = data_frames["tskuplu"].rename(columns=lambda x: f"item.{x}")
    cate_frames = data_frames["tcatcategory"]

    cost = cost_frames.merge(
        item_frames, left_on="cost.pluid", right_on="item.pluid"
    ).merge(cate_frames, how="left", left_on="item.clsid", right_on="clsid")
    cost = cost[cost["isactive"] == "1"]
    if len(cost) == 0:
        cost = pd.DataFrame(
            columns=[
                "source_id",
                "foreign_store_id",
                "foreign_item_id",
                "date",
                "cost_type",
                "total_quantity",
                "total_sale",
                "total_cost",
                "foreign_category_lv1",
                "foreign_category_lv2",
                "foreign_category_lv3",
                "foreign_category_lv4",
                "foreign_category_lv5",
                "cmid",
            ]
        )
    else:
        cost1 = cost.copy()
        cost1["foreign_category_lv2"] = cost1["clscode"].apply(lambda x: x[:4])
        cost1["foreign_category_lv1"] = cost1["clscode"].apply(lambda x: x[:2])
        cost1 = cost1.merge(
            cate_frames,
            how="left",
            left_on="foreign_category_lv2",
            right_on="clscode",
            suffixes=("", ".lv2"),
        )
        cost1 = cost1[cost1["isactive.lv2"] == "1"]
        cost1 = cost1.merge(
            cate_frames,
            how="left",
            left_on="foreign_category_lv1",
            right_on="clscode",
            suffixes=("", ".lv1"),
        )
        cost1 = cost1[cost1["isactive.lv1"] == "1"]
        cost1 = cost1[
            (cost1["cost.orgcode"] != "00")
            & (cost1["clscode"].map(len) == 6)
            & (cost1["islast"] == "1")
        ]
        cost1["source_id"] = source_id
        cost1["cost_type"] = ""
        cost1["foreign_category_lv4"] = ""
        cost1["foreign_category_lv5"] = ""
        cost1["cmid"] = cmid
        cost1 = cost1.rename(
            columns={
                "cost.orgcode": "foreign_store_id",
                "item.pluid": "foreign_item_id",
                "cost.rptdate": "date",
                "cost.xscount": "total_quantity",
                "cost.hxtotal": "total_sale",
                "cost.hjcost": "total_cost",
                "clscode": "foreign_category_lv3",
            }
        )
        cost1["date"] = cost1["date"].apply(lambda row: row.split()[0])
        cost1 = cost1[
            [
                "source_id",
                "foreign_store_id",
                "foreign_item_id",
                "date",
                "cost_type",
                "total_quantity",
                "total_sale",
                "total_cost",
                "foreign_category_lv1",
                "foreign_category_lv2",
                "foreign_category_lv3",
                "foreign_category_lv4",
                "foreign_category_lv5",
                "cmid",
            ]
        ]

        cost2 = cost.copy()
        cost2["foreign_category_lv1"] = cost2["clscode"].apply(lambda x: x[:2])
        cost2 = cost2.merge(cate_frames, how="left", left_on="foreign_category_lv1", right_on="clscode",
                            suffixes=("", ".lv1"))
        cost2 = cost2[cost2["isactive.lv1"] == "1"]
        cost2 = cost2[
            (cost2["cost.orgcode"] != "00")
            & (cost2["clscode"].map(len) == 4)
            & (cost2["islast"] == "1")
        ]

        cost2["source_id"] = source_id
        cost2["cost_type"] = ""
        cost2["foreign_category_lv3"] = ""
        cost2["foreign_category_lv4"] = ""
        cost2["foreign_category_lv5"] = ""
        cost2["cmid"] = cmid
        cost2 = cost2.rename(columns={
            "cost.orgcode": "foreign_store_id",
            "item.pluid": "foreign_item_id",
            "cost.rptdate": "date",
            "cost.xscount": "total_quantity",
            "cost.hxtotal": "total_sale",
            "cost.hjcost": "total_cost",
            "clscode": "foreign_category_lv2"
        })
        cost2["date"] = cost2["date"].apply(lambda row: row.split()[0])
        cost2 = cost2[
            [
                "source_id",
                "foreign_store_id",
                "foreign_item_id",
                "date",
                "cost_type",
                "total_quantity",
                "total_sale",
                "total_cost",
                "foreign_category_lv1",
                "foreign_category_lv2",
                "foreign_category_lv3",
                "foreign_category_lv4",
                "foreign_category_lv5",
                "cmid",
            ]
        ]

        cost = pd.concat([cost1, cost2])
    return upload_to_s3(cost, source_id, date, target_table)


def clean_cost(source_id, date, target_table, data_frames):
    if source_id == "52YYYYYYYYYYYYY":
        return clean_cost_52(source_id, date, target_table, data_frames)
    else:
        return clean_cost_64(source_id, date, target_table, data_frames)


def clean_goods_52(source_id, date, target_table, dataframes):
    item = dataframes["tskuplu"]
    category = dataframes["tcatcategory"]
    brand = dataframes["tbasbrand"]
    category["clscode_lv2"] = category.apply(lambda row: row["clscode"][:2], axis=1)
    result = (
        item.merge(category, how="left", on="clsid")
        .merge(
            category,
            left_on="clscode_lv2",
            rigth_on="clscode",
            suffixes=("", "_lv1"),
            how="left",
        )
        .merge(brand, on="brand_code", how="left")
    )

    result = result[(result["isactive_lv1"] != "1") & (result["isactive"] != "1")]

    cmid = source_id.split("Y")[0]
    result["cmid"] = cmid
    result["foreign_category_lv3"] = ""
    result["foreign_category_lv4"] = ""
    result["foreign_category_lv5"] = ""
    result["isvalid"] = 1
    result["allot_method"] = result["udp3"]
    result["supplier_name"] = ""
    result["supplier_code"] = ""
    result["brand_name"] = result["brandname"]
    result["last_updated"] = datetime.now(_TZINFO)

    def generate_item_status(row):
        res = None
        if row == "1":
            res = "正常"
        elif row == "2":
            res = "预淘汰"
        elif row == "3":
            res = "淘汰"
        return res

    result["item_status"] = result["ywstatus"].apply(generate_item_status)

    result = result.rename(
        columns={
            "barcode": "barcode",
            "pluid": "foreign_item_id",
            "pluname": "item_name",
            "hjprice": "lastin_price",
            "price": "sale_price",
            "unit": "item_unit",
            "lrdate": "storage_time",
            "plucode": "show_code",
            "clscode_lv1": "foreign_category_lv1",
            "clscode": "foreign_category_lv2",
            "bzdays": "warranty",
        }
    )

    result = result[
        [
            "cmid",
            "barcode",
            "foreign_item_id",
            "item_name",
            "lastin_price",
            "sale_price",
            "item_unit",
            "item_status",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "storage_time",
            "last_updated",
            "isvalid",
            "warranty",
            "show_code",
            "foreign_category_lv5",
            "allot_method",
            "supplier_name",
            "supplier_code",
            "brand_name",
        ]
    ]

    return upload_to_s3(result, source_id, date, target_table)


def clean_goods_64(source_id, date, target_table, data_frames):
    """
    清洗商品
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    item_frames = data_frames["tskuplu"]
    cate_frames = data_frames["tcatcategory"]
    brand_frames = data_frames["tbasbrand"]
    goods = item_frames.merge(cate_frames, how="left", on="clsid")
    goods = goods[goods["isactive"] == "1"]

    goods1 = goods.copy()
    goods1["foreign_category_lv2"] = goods1["clscode"].apply(lambda x: x[:4])
    goods1["foreign_category_lv1"] = goods1["clscode"].apply(lambda x: x[:2])
    goods1 = goods1.merge(
        cate_frames,
        how="left",
        left_on="foreign_category_lv2",
        right_on="clscode",
        suffixes=("", ".lv2"),
    )
    goods1 = goods1[goods1["isactive.lv2"] == "1"]
    goods1 = goods1.merge(
        cate_frames,
        how="left",
        left_on="foreign_category_lv1",
        right_on="clscode",
        suffixes=("", ".lv1"),
    )
    goods1 = goods1[goods1["isactive.lv1"] == "1"]
    goods1 = goods1.merge(brand_frames, how="left", on="brandcode")
    goods1 = goods1[(goods1["islast"] == "1") & (goods1["clscode"].map(len) == 6)]
    goods1["cmid"] = cmid
    goods1["foreign_category_lv4"] = ""
    goods1["foreign_category_lv5"] = ""
    goods1["isvalid"] = 1
    if cmid in ALLOT_METHOD:
        goods1["allot_method"] = goods1["udp3"]
    else:
        goods1["allot_method"] = ""
    goods1["supplier_name"] = ""
    goods1["supplier_code"] = ""
    goods1["brand_name"] = goods1["brandname"]
    goods1["last_updated"] = datetime.now(_TZINFO)

    def generate_item_status(row):
        res = None
        if row == "1":
            res = "正常"
        elif row == "2":
            res = "预淘汰"
        elif row == "3":
            res = "淘汰"
        return res

    goods1["item_status"] = goods1["ywstatus"].apply(generate_item_status)

    goods1 = goods1.rename(
        columns={
            "barcode": "barcode",
            "pluid": "foreign_item_id",
            "pluname": "item_name",
            "hjprice": "lastin_price",
            "price": "sale_price",
            "unit": "item_unit",
            "lrdate": "storage_time",
            "plucode": "show_code",
            "clscode": "foreign_category_lv3",
            "bzdays": "warranty",
        }
    )

    goods1 = goods1[
        [
            "cmid",
            "barcode",
            "foreign_item_id",
            "item_name",
            "lastin_price",
            "sale_price",
            "item_unit",
            "item_status",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "storage_time",
            "last_updated",
            "isvalid",
            "warranty",
            "show_code",
            "foreign_category_lv5",
            "allot_method",
            "supplier_name",
            "supplier_code",
            "brand_name",
        ]
    ]

    goods2 = goods.copy()
    goods2["foreign_category_lv1"] = goods2["clscode"].apply(lambda x: x[:2])
    goods2 = goods2.merge(
        cate_frames,
        how="left",
        left_on="foreign_category_lv1",
        right_on="clscode",
        suffixes=("", ".lv1"),
    )
    goods2 = goods2[goods2["isactive.lv1"] == "1"]
    goods2 = goods2.merge(brand_frames, how="left", on="brandcode")
    goods2 = goods2[(goods2["islast"] == "1") & (goods2["clscode"].map(len) == 4)]
    goods2["cmid"] = cmid
    goods2["foreign_category_lv3"] = ""
    goods2["foreign_category_lv4"] = ""
    goods2["foreign_category_lv5"] = ""
    goods2["isvalid"] = 1
    if cmid in ALLOT_METHOD:
        goods2["allot_method"] = goods2["udp3"]
    else:
        goods2["allot_method"] = ""
    goods2["supplier_name"] = ""
    goods2["supplier_code"] = ""
    goods2["brand_name"] = goods2["brandname"]
    goods2["last_updated"] = datetime.now(_TZINFO)

    goods2["item_status"] = goods2["ywstatus"].apply(generate_item_status)

    goods2 = goods2.rename(
        columns={
            "barcode": "barcode",
            "pluid": "foreign_item_id",
            "pluname": "item_name",
            "hjprice": "lastin_price",
            "price": "sale_price",
            "unit": "item_unit",
            "lrdate": "storage_time",
            "plucode": "show_code",
            "clscode": "foreign_category_lv2",
            "bzdays": "warranty",
        }
    )

    goods2 = goods2[
        [
            "cmid",
            "barcode",
            "foreign_item_id",
            "item_name",
            "lastin_price",
            "sale_price",
            "item_unit",
            "item_status",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "storage_time",
            "last_updated",
            "isvalid",
            "warranty",
            "show_code",
            "foreign_category_lv5",
            "allot_method",
            "supplier_name",
            "supplier_code",
            "brand_name",
        ]
    ]
    goods = pd.concat([goods1, goods2])
    return upload_to_s3(goods, source_id, date, target_table)


def clean_goods(source_id, date, target_table, data_frames):
    if source_id == "52YYYYYYYYYYYYYY":
        return clean_goods_52(source_id, date, target_table, data_frames)
    else:
        return clean_goods_64(source_id, date, target_table, data_frames)


def clean_category_52(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    lv1_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv2.{x}")

    category1 = lv1_frames[
        (lv1_frames["lv1.clscode"].map(len) == 2) & (lv1_frames["lv1.isactive"] == "1")
    ]

    category1 = category1.rename(
        columns={
            "lv1.clscode": "foreign_category_lv1",
            "lv1.clsname": "foreign_category_lv1_name",
        }
    )
    category1["cmid"] = cmid
    category1["level"] = 1
    category1["foreign_category_lv2"] = ""
    category1["foreign_category_lv2_name"] = None
    category1["foreign_category_lv3"] = ""
    category1["foreign_category_lv3_name"] = None
    category1["foreign_category_lv4"] = ""
    category1["foreign_category_lv4_name"] = None
    category1["foreign_category_lv5"] = ""
    category1["foreign_category_lv5_name"] = None
    category1["last_updated"] = datetime.now(_TZINFO)
    category1 = category1[
        [
            "cmid",
            "level",
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "last_updated",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
            "foreign_category_lv5",
            "foreign_category_lv5_name",
        ]
    ]

    category2 = lv2_frames.copy()
    category2["lv1.clscode"] = category2["lv2.clscode"].apply(lambda x: x[:2])
    category2 = category2.merge(lv1_frames, how="left", on="lv1.clscode")
    category2 = category2[
        (category2["lv2.clscode"].map(len) == 4)
        & (category2["lv2.isactive"] == "1")
        & (category2["lv1.isactive"] == "1")
    ]
    category2 = category2.rename(
        columns={
            "lv1.clscode": "foreign_category_lv1",
            "lv1.clsname": "foreign_category_lv1_name",
            "lv2.clscode": "foreign_category_lv2",
            "lv2.clsname": "foreign_category_lv2_name",
        }
    )
    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = None
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = None
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = None
    category2["last_updated"] = datetime.now(_TZINFO)
    category2 = category2[
        [
            "cmid",
            "level",
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "last_updated",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
            "foreign_category_lv5",
            "foreign_category_lv5_name",
        ]
    ]

    return upload_to_s3(
        pd.concat([category1, category2]), source_id, date, target_table
    )


def clean_category_64(source_id, date, target_table, data_frames):
    """
    分类清洗
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    lv1_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv3.{x}")

    category1 = lv1_frames[
        (lv1_frames["lv1.clscode"].map(len) == 2) & (lv1_frames["lv1.isactive"] == "1")
    ]

    category1 = category1.rename(
        columns={
            "lv1.clscode": "foreign_category_lv1",
            "lv1.clsname": "foreign_category_lv1_name",
        }
    )
    category1["cmid"] = cmid
    category1["level"] = 1
    category1["foreign_category_lv2"] = ""
    category1["foreign_category_lv2_name"] = None
    category1["foreign_category_lv3"] = ""
    category1["foreign_category_lv3_name"] = None
    category1["foreign_category_lv4"] = ""
    category1["foreign_category_lv4_name"] = None
    category1["foreign_category_lv5"] = ""
    category1["foreign_category_lv5_name"] = None
    category1["last_updated"] = datetime.now(_TZINFO)
    category1 = category1[
        [
            "cmid",
            "level",
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "last_updated",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
            "foreign_category_lv5",
            "foreign_category_lv5_name",
        ]
    ]

    category2 = lv2_frames.copy()
    category2["lv1.clscode"] = category2["lv2.clscode"].apply(lambda x: x[:2])
    category2 = category2.merge(lv1_frames, how="left", on="lv1.clscode")
    category2 = category2[
        (category2["lv2.clscode"].map(len) == 4)
        & (category2["lv2.isactive"] == "1")
        & (category2["lv1.isactive"] == "1")
    ]
    category2 = category2.rename(
        columns={
            "lv1.clscode": "foreign_category_lv1",
            "lv1.clsname": "foreign_category_lv1_name",
            "lv2.clscode": "foreign_category_lv2",
            "lv2.clsname": "foreign_category_lv2_name",
        }
    )
    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = None
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = None
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = None
    category2["last_updated"] = datetime.now(_TZINFO)
    category2 = category2[
        [
            "cmid",
            "level",
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "last_updated",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
            "foreign_category_lv5",
            "foreign_category_lv5_name",
        ]
    ]

    category3 = lv3_frames.copy()
    category3["lv2.clscode"] = category3["lv3.clscode"].apply(lambda x: x[:4])
    category3["lv1.clscode"] = category3["lv3.clscode"].apply(lambda x: x[:2])
    category3 = category3.merge(lv2_frames, how="left", on="lv2.clscode").merge(
        lv1_frames, how="left", on="lv1.clscode"
    )
    category3 = category3[
        (category3["lv3.clscode"].map(len) == 6)
        & (category3["lv3.isactive"] == "1")
        & (category3["lv2.isactive"] == "1")
        & (category3["lv1.isactive"] == "1")
    ]
    category3 = category3.rename(
        columns={
            "lv1.clscode": "foreign_category_lv1",
            "lv1.clsname": "foreign_category_lv1_name",
            "lv2.clscode": "foreign_category_lv2",
            "lv2.clsname": "foreign_category_lv2_name",
            "lv3.clscode": "foreign_category_lv3",
            "lv3.clsname": "foreign_category_lv3_name",
        }
    )
    category3["cmid"] = cmid
    category3["level"] = 3
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = None
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = None
    category3["last_updated"] = datetime.now(_TZINFO)
    category3 = category3[
        [
            "cmid",
            "level",
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "last_updated",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
            "foreign_category_lv5",
            "foreign_category_lv5_name",
        ]
    ]

    category = pd.concat([category1, category2, category3])
    return upload_to_s3(category, source_id, date, target_table)


def clean_category(source_id, date, target_table, data_frames):
    if source_id == "52YYYYYYYYYYYYYY":
        return clean_category_52(source_id, date, target_table, data_frames)
    else:
        return clean_category_64(source_id, date, target_table, data_frames)


def clean_store(source_id, date, target_table, data_frames):
    """
    门店清洗
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
    """
    cmid = source_id.split("Y")[0]

    stores_frames = data_frames["torgmanage"].rename(columns=lambda x: f"stores.{x}")
    area_frames = data_frames["torgmanage"]
    area_frames = area_frames[area_frames["orgclass"] == "0"]
    store = stores_frames.merge(
        area_frames, how="left", left_on="stores.preorgcode", right_on="orgcode"
    )
    store = store[store["stores.orgclass"] != "0"]

    store = store.rename(
        columns={
            "stores.orgcode": "foreign_store_id",
            "stores.orgname": "store_name",
            "orgcode": "area_code",
            "orgname": "area_name",
        }
    )
    store["show_code"] = store["foreign_store_id"]
    store["cmid"] = cmid
    store["store_address"] = ""
    store["address_code"] = None
    store["device_id"] = None
    store["store_status"] = ""
    store["create_date"] = datetime.now(_TZINFO)
    store["lat"] = None
    store["lng"] = None
    store["phone_number"] = ""
    store["contacts"] = ""
    store["business_area"] = None
    store["property_id"] = None
    store["property"] = ""
    store["source_id"] = source_id
    store["last_updated"] = datetime.now(_TZINFO)

    store = store[
        [
            "cmid",
            "foreign_store_id",
            "store_name",
            "store_address",
            "address_code",
            "device_id",
            "store_status",
            "create_date",
            "lat",
            "lng",
            "show_code",
            "phone_number",
            "contacts",
            "area_code",
            "area_name",
            "business_area",
            "property_id",
            "property",
            "source_id",
            "last_updated",
        ]
    ]

    return upload_to_s3(store, source_id, date, target_table)


def upload_to_s3(frame, source_id, date, target_table):
    filename = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
    count = len(frame)
    frame.to_csv(filename.name, index=False, compression="gzip", float_format="%.4f")
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
