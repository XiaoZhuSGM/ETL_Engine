"""
海信商定天下清洗逻辑
销售，成本，库存，商品， 分类

# goodsflow
        'origin_table_columns': {
            "tcatcategory": ["clsid", "clscode", "clsname", "isactive"],
            "torgmanage": ["orgcode", "orgname"],
            "tsalsale": ["saleno", "xsdate", "orgcode", "trantype"],
            "tsalsaleplu": ["saleno", "orgcode", "price", "xscount", "sstotal", "pluid"],
            "tskuplu": ["pluid", "barcode", "pluname", "unit", "clsid"]
        },

        'converts': {
            "tcatcategory": {"isactive": "str", "clscode": "str"},
            "torgmanage": {"orgcode": "str"},
            "tskuplu": {"pluid": "str", "isactive": "str"},
            "tsalsaleplu": {"pluid": "str", "orgcode": "str"}
        }

# cost
        'origin_table_columns': {
            "tcatcategory": ["clsid", "clscode"],
            "tsalpludetail": ["orgcode", "rptdate", "xscount", "hxtotal", "hjcost", "pluid"],
            "tskuplu": ["pluid", "clsid"]
        },
        'converts': {
            "tcatcategory": {"clscode": "str"},
            "tsalpludetail": {"pluid": "str", "orgcode": "str"},
            "tskuplu": {"pluid": "str"}
        }


# goods
        'origin_table_columns': {
            "tcatcategory": ["clsid", "clscode", "isactive"],
            "tskuplu": ["clsid", "barcode", "pluid", "pluname", "hjprice", "price", "unit", "ywstatus", "plucode",
                        "lrdate"]
        },
        'converts': {
            "tcatcategory": {"clscode": "str", "isactive": "str"},
            "tskuplu": {"pluid": "str", "plucode": "str"}
        }



#category
        'origin_table_columns': {
            "tcatcategory": ["clscode", "clsname", "isactive", "clsid"]
        },
        'converts': {
            "tcatcategory": {"clscode": "str", "isactive": "str", "clsid": "str"}
        }


#store
        'origin_table_columns': {
            "torgmanage": ["orgcode", "orgname", "preorgcode", "orgclass"]
        },
        'converts': {
            "torgmanage": {"orgclass": "str", "orgcode": "str"}

        }

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


def clean_goodsflow(source_id, date, target_table, data_frames):
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
    lv4_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv4.{x}")
    lv3_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv3.{x}")
    lv2_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv2.{x}")
    lv1_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv1.{x}")

    goodsflow1 = head_frames.\
        merge(detail_frames, left_on=["head.saleno", "head.orgcode"], right_on=["detail.saleno", "detail.orgcode"])\
        .merge(stores_frames, left_on="head.orgcode", right_on="stores.orgcode")\
        .merge(item_frames, left_on="detail.pluid", right_on="item.pluid")\
        .merge(lv4_frames, how="left", left_on="item.clsid", right_on="lv4.clsid")

    def genarate_clscode(row, clslen):
        if row is np.NAN:
            return
        length = len(row) - clslen
        return row[:length]

    goodsflow1["lv3.clscode"] = goodsflow1["lv4.clscode"].apply(genarate_clscode, args=(2,))
    goodsflow1["lv2.clscode"] = goodsflow1["lv4.clscode"].apply(genarate_clscode, args=(4,))
    goodsflow1["lv1.clscode"] = goodsflow1["lv4.clscode"].apply(genarate_clscode, args=(6,))

    goodsflow1 = goodsflow1.merge(lv3_frames, how="left", on="lv3.clscode")
    goodsflow1 = goodsflow1[goodsflow1["lv3.isactive"] == "1"]
    goodsflow1 = goodsflow1.merge(lv2_frames, how="left", on="lv2.clscode")
    goodsflow1 = goodsflow1[goodsflow1["lv2.isactive"] == "1"]
    goodsflow1 = goodsflow1.merge(lv1_frames, how="left", on="lv1.clscode")
    goodsflow1 = goodsflow1[goodsflow1["lv1.isactive"] == "1"]
    goodsflow1 = goodsflow1[(goodsflow1["head.trantype"] != "5") & (goodsflow1["lv4.clscode"].map(len) >= 9)]

    goodsflow1["source_id"] = source_id
    goodsflow1["cmid"] = cmid
    goodsflow1["consumer_id"] = ""
    goodsflow1["last_updated"] = datetime.now(_TZINFO)
    goodsflow1["foreign_category_lv5"] = ""
    goodsflow1["foreign_category_lv5_name"] = None
    goodsflow1["pos_id"] = ""

    goodsflow1 = goodsflow1.rename(columns={
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
        "lv4.clscode": "foreign_category_lv4",
        "lv4.clsname": "foreign_category_lv4_name",
    })

    goodsflow1 = goodsflow1[[
        'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime', 'last_updated',
        'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
        'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
        'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
        'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'
    ]]

    goodsflow2 = head_frames. \
        merge(detail_frames, left_on=["head.saleno", "head.orgcode"], right_on=["detail.saleno", "detail.orgcode"]). \
        merge(stores_frames, left_on="head.orgcode", right_on="stores.orgcode"). \
        merge(item_frames, left_on="detail.pluid", right_on="item.pluid"). \
        merge(lv3_frames, how="left", left_on="item.clsid", right_on="lv3.clsid")

    goodsflow2["lv2.clscode"] = goodsflow2["lv3.clscode"].apply(lambda x: x[:4])
    goodsflow2["lv1.clscode"] = goodsflow2["lv3.clscode"].apply(lambda x: x[:2])
    goodsflow2 = goodsflow2.merge(lv2_frames, how="left", on="lv2.clscode").\
        merge(lv1_frames, how="left", on="lv1.clscode")

    goodsflow2 = goodsflow2[(goodsflow2["head.trantype"] != "5") & (goodsflow2["lv3.clscode"].map(len) == 6)]

    goodsflow2["source_id"] = source_id
    goodsflow2["cmid"] = cmid
    goodsflow2["consumer_id"] = ""
    goodsflow2["last_updated"] = datetime.now(_TZINFO)
    goodsflow2["foreign_category_lv4"] = ""
    goodsflow2["foreign_category_lv4_name"] = None
    goodsflow2["foreign_category_lv5"] = ""
    goodsflow2["foreign_category_lv5_name"] = None
    goodsflow2["pos_id"] = ""

    goodsflow2 = goodsflow2.rename(columns={
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
    })

    goodsflow2 = goodsflow2[[
        'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime', 'last_updated',
        'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
        'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
        'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
        'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'
    ]]

    goodsflow_frames = pd.concat([goodsflow1, goodsflow2])
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
    cmid = source_id.split("Y")[0]
    cost_frames = data_frames["tsalpludetail"].rename(columns=lambda x: f"cost.{x}")
    item_frames = data_frames["tskuplu"].rename(columns=lambda x: f"item.{x}")
    cate_frames = data_frames["tcatcategory"]

    cost = cost_frames.merge(item_frames, left_on="cost.pluid", right_on="item.pluid")\
        .merge(cate_frames, how="left", left_on="item.clsid", right_on="clsid")
    cost1 = cost.copy()
    cost1 = cost1[(cost1["cost.orgcode"] != "00") & (cost1["clscode"].map(len) == 9)]
    cost1["source_id"] = source_id
    cost1["cost_type"] = ""

    def genarate_clscode(row, clslen):
        if row is np.NAN:
            return
        length = len(row) - clslen
        return row[:length]

    cost1["foreign_category_lv1"] = cost1["clscode"].apply(genarate_clscode, args=(6,))
    cost1["foreign_category_lv2"] = cost1["clscode"].apply(genarate_clscode, args=(4,))
    cost1["foreign_category_lv3"] = cost1["clscode"].apply(genarate_clscode, args=(2,))
    cost1["foreign_category_lv5"] = ""
    cost1["cmid"] = ""

    cost1 = cost1.rename(columns={
        "cost.orgcode": "foreign_store_id",
        "item.pluid": "foreign_item_id",
        "cost.rptdate": "date",
        "cost.xscount": "total_quantity",
        "cost.hxtotal": "total_sale",
        "cost.hjcost": "total_cost",
        "clscode": "foreign_category_lv4"
    })
    cost1["date"] = cost1["date"].apply(lambda row: row.split()[0])
    cost1 = cost1[[
        "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
        "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
        "foreign_category_lv5", "cmid"
    ]]
    cost2 = cost.copy()
    cost2 = cost2[(cost2["cost.orgcode"] != "00") & (cost2["clscode"].map(len) == 6)]

    cost2["source_id"] = source_id
    cost2["cost_type"] = ""
    cost2["foreign_category_lv1"] = cost2["clscode"].apply(genarate_clscode, args=(4,))
    cost2["foreign_category_lv2"] = cost2["clscode"].apply(genarate_clscode, args=(2,))
    cost2["foreign_category_lv4"] = ""
    cost2["foreign_category_lv5"] = ""
    cost2["cmid"] = ""

    cost2 = cost2.rename(columns={
        "cost.orgcode": "foreign_store_id",
        "item.pluid": "foreign_item_id",
        "cost.rptdate": "date",
        "cost.xscount": "total_quantity",
        "cost.hxtotal": "total_sale",
        "cost.hjcost": "total_cost",
        "clscode": "foreign_category_lv3"
    })
    cost2["date"] = cost2["date"].apply(lambda row: row.split()[0])
    cost2 = cost2[[
        "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
        "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
        "foreign_category_lv5", "cmid"
    ]]

    cost = pd.concat([cost1, cost2])
    return upload_to_s3(cost, source_id, date, target_table)


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
    item_frames = data_frames["tskuplu"]
    lv4_frames = data_frames["tcatcategory"]

    goods = item_frames.merge(lv4_frames, how="left", on="clsid")
    goods = goods[goods["isactive"] == "1"]
    goods["cmid"] = cmid
    goods["foreign_category_lv5"] = ""
    goods["isvalid"] = 1
    goods["warranty"] = ""
    goods["allot_method"] = ""
    goods["supplier_name"] = ""
    goods["supplier_code"] = ""
    goods["brand_name"] = ""
    goods["last_updated"] = datetime.now(_TZINFO)

    def generate_item_status(row):
        res = None
        if row == "1":
            res = "正常"
        elif row == "2":
            res = "预淘汰"
        elif row == "3":
            res = "淘汰"
        return res
    goods["item_status"] = goods["ywstatus"].apply(generate_item_status)

    def genarate_clscode(row, clslen):
        if row is np.NAN:
            return
        length = len(row) - clslen
        return row[:length]

    goods["foreign_category_lv1"] = goods["clscode"].apply(genarate_clscode, args=(6,))
    goods["foreign_category_lv2"] = goods["clscode"].apply(genarate_clscode, args=(4,))
    goods["foreign_category_lv3"] = goods["clscode"].apply(genarate_clscode, args=(2,))

    goods = goods.rename(columns={
        "barcode": "barcode",
        "pluid": "foreign_item_id",
        "pluname": "item_name",
        "hjprice": "lastin_price",
        "price": "sale_price",
        "unit": "item_unit",
        "clscode": "foreign_category_lv4",
        "lrdate": "storage_time",
        "plucode": "show_code",
    })

    goods = goods[[
        "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
        "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4", "storage_time",
        "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method", "supplier_name",
        "supplier_code",
        "brand_name",
    ]]

    return upload_to_s3(goods, source_id, date, target_table)


def clean_category(source_id, date, target_table, data_frames):
    """
    分类清洗
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    lv1_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv3.{x}")
    lv4_frames = data_frames["tcatcategory"].rename(columns=lambda x: f"lv4.{x}")

    category1 = lv1_frames[((lv1_frames["lv1.clscode"].map(len) == 3)
                           | (lv1_frames["lv1.clscode"].map(len) == 2))
                           & (lv1_frames["lv1.isactive"] == "1")]
    category1 = category1.rename(columns={
        "lv1.clscode": "foreign_category_lv1",
        "lv1.clsname": "foreign_category_lv1_name"
    })
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
    category1 = category1[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    def genarate_clscode(row, clslen):
        if row is np.NAN:
            return
        length = len(row) - clslen
        return row[:length]

    category2 = lv2_frames.copy()
    category2["lv1.clscode"] = category2["lv2.clscode"].apply(genarate_clscode, args=(2,))
    category2 = category2.merge(lv1_frames, how="left", on="lv1.clscode")
    category2 = category2[((category2["lv2.clscode"].map(len) == 5) |
                          (category2["lv2.clscode"].map(len) == 4)) &
                          (category2["lv2.isactive"] == "1") &
                          (category2["lv1.isactive"] == "1")]
    category2 = category2.rename(columns={
        "lv1.clscode": "foreign_category_lv1",
        "lv1.clsname": "foreign_category_lv1_name",
        "lv2.clscode": "foreign_category_lv2",
        "lv2.clsname": "foreign_category_lv2_name"
    })
    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = None
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = None
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = None
    category2["last_updated"] = datetime.now(_TZINFO)
    category2 = category2[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category3 = lv3_frames.copy()
    category3["lv2.clscode"] = category3["lv3.clscode"].apply(genarate_clscode, args=(2,))
    category3["lv1.clscode"] = category3["lv3.clscode"].apply(genarate_clscode, args=(4,))
    category3 = category3.merge(lv2_frames, how="left", on="lv2.clscode")\
        .merge(lv1_frames, how="left", on="lv1.clscode")
    category3 = category3[((category3["lv3.clscode"].map(len) == 7) |
                          (category3["lv3.clscode"].map(len) == 6)) &
                          (category3["lv3.isactive"] == "1") &
                          (category3["lv2.isactive"] == "1") &
                          (category3["lv1.isactive"] == "1")]
    category3 = category3.rename(columns={
        "lv1.clscode": "foreign_category_lv1",
        "lv1.clsname": "foreign_category_lv1_name",
        "lv2.clscode": "foreign_category_lv2",
        "lv2.clsname": "foreign_category_lv2_name",
        "lv3.clscode": "foreign_category_lv3",
        "lv3.clsname": "foreign_category_lv3_name"
    })
    category3["cmid"] = cmid
    category3["level"] = 3
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = None
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = None
    category3["last_updated"] = datetime.now(_TZINFO)
    category3 = category3[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category4 = lv4_frames.copy()
    category4["lv3.clscode"] = category4["lv4.clscode"].apply(genarate_clscode, args=(2,))
    category4["lv2.clscode"] = category4["lv4.clscode"].apply(genarate_clscode, args=(4,))
    category4["lv1.clscode"] = category4["lv4.clscode"].apply(genarate_clscode, args=(6,))
    category4 = category4.merge(lv3_frames, how="left", on="lv3.clscode")\
        .merge(lv2_frames, how="left", on="lv2.clscode").merge(lv1_frames, how="left", on="lv1.clscode")
    category4 = category4[(category4["lv4.clscode"].map(len) == 9) &
                          (category4["lv4.clsid"] != "10000008765") &
                          (category4["lv3.isactive"] == "1") &
                          (category4["lv2.isactive"] == "1") &
                          (category4["lv1.isactive"] == "1")]
    category4 = category4.rename(columns={
        "lv1.clscode": "foreign_category_lv1",
        "lv1.clsname": "foreign_category_lv1_name",
        "lv2.clscode": "foreign_category_lv2",
        "lv2.clsname": "foreign_category_lv2_name",
        "lv3.clscode": "foreign_category_lv3",
        "lv3.clsname": "foreign_category_lv3_name",
        "lv4.clscode": "foreign_category_lv4",
        "lv4.clsname": "foreign_category_lv4_name"
    })
    category4["cmid"] = cmid
    category4["level"] = 4
    category4["foreign_category_lv5"] = ""
    category4["foreign_category_lv5_name"] = None
    category4["last_updated"] = datetime.now(_TZINFO)
    category4 = category4[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category = pd.concat([category1, category2, category3, category4])
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

    stores_frames = data_frames["torgmanage"].rename(columns=lambda x: f"stores.{x}")
    area_frames = data_frames["torgmanage"]
    area_frames = area_frames[area_frames["orgclass"] == "0"]
    store = stores_frames.merge(area_frames, how="left", left_on="stores.preorgcode", right_on="orgcode")
    store = store[store["stores.orgclass"] != "0"]

    store = store.rename(columns={
        "stores.orgcode": "foreign_store_id",
        "stores.orgname": "store_name",
        "orgcode": "area_code",
        "orgname": "area_name",
    })
    store["show_code"] = store["foreign_store_id"]
    store["cmid"] = cmid
    store["store_address"] = ""
    store["address_code"] = None
    store["device_id"] = None
    store["store_status"] = ""
    store["create_date"] = ""
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
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    return upload_to_s3(store, source_id, date, target_table)


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
