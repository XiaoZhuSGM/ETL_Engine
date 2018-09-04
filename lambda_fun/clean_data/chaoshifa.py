"""
超市发清洗逻辑
销售，成本，库存，商品， 分类

# goodsflow
origin_table_columns =
{"sale_j": ['listno', 'sdate', 'stime', 'usebarcodeid', 'price',
            'amount', 'x', 'salevalue', 'discvalue', 'posid', 'goodsid', 'shopid'],
"pay_j": ['paytype', 'cardno', 'listno', 'posid', 'sdate', 'shopid', 'stime'],
"goods": ['goodsid', 'name', 'unitname', 'deptid'],
"shop": ['id', 'name'],
"sgroup": ['id', 'name'],
"dept": ['id', 'name'],
}

coverts = {
            "sale_j": {"listno": "str", "usebarcodeid": "str", "posid": "str", "goodsid": "str", "shopid": "str"},
            "pay_j": {"cardno": "str", "listno": "str", "posid":"str", "shopid":"str"},
            "goods": {"goodsid": "str", "deptid": "str"},
            "shop": {"id": "str"},
            "sgroup": {"id": "str"}
            "dept": {"id": "str"},
           }


# cost
origin_table_columns = {
"salecost": [shopid,goodsid,sdate,qty,salevalue,discvalue,costvalue],
"goods": [goodsid,deptid],
"sgroup": [id,deptlevelid],
"dept": [id]
}

coverts = {
"salecost": {"shopid": str, "goodsid": str},
"goods": {"goodsid": str, "deptid": str},
"sgroup": {"id": str, "deptlevelid": str},
"dept": {"id": str},
}



# goods
origin_table_columns = {
"goods": [barcodeid,goodsid,name,unitname,flag,deptid,keepdays],
"cost": [cost,goodsid,shopid,flag],
"goodsshop": [price,goodsid,shopid],
}

coverts = {
"goods": {"barcodeid": str, "goodsid": str, "deptid": str},
"cost": {"goodsid": str, "shopid": str},
"goodsshop": {"goodsid": str}
           }



#category
origin_table_columns =
{"sgroup": [deptlevelid,id,name]}

coverts = {"t_bd_item_cls": {"item_clsno": str, "cls_parent": str}}



#store
origin_table_columns =
{"shop": [id,name,address,shoptype,linktele]}

coverts = {"shop": {"id": str}}

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


def clean_chaoshifa(source_id, date, target_table, data_frames):
    if target_table == "goodsflow":
        clean_goodsflow(source_id, date, target_table, data_frames)
    elif target_table == "cost":
        clean_cost(source_id, date, target_table, data_frames)
    elif target_table == "store":
        clean_store(source_id, date, target_table, data_frames)
    elif target_table == "goods":
        clean_goods(source_id, date, target_table, data_frames)
    elif target_table == "category":
        clean_category(source_id, date, target_table, data_frames)
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
    flow_frames = data_frames["sale_j"]
    pay_frames = data_frames["pay_j"]
    pay_frames = pay_frames[pay_frames["paytype"] == "h"].drop_duplicates()

    goodsflow_frames = pd.merge(flow_frames, pay_frames, how="left", on=["listno", "posid", "sdate", "shopid", "stime"])
    goodsflow_frames = goodsflow_frames.merge(data_frames["goods"], how="left", on="goodsid")
    goodsflow_frames = goodsflow_frames.merge(data_frames["shop"], how="left", left_on="shopid", right_on="id", suffixes=("", ".store"))

    sgroup_frames = data_frames["sgroup"]
    lv3 = sgroup_frames[sgroup_frames["deptlevelid"] == "3"]
    lv2 = sgroup_frames[sgroup_frames["deptlevelid"] == "2"]
    lv1 = sgroup_frames[sgroup_frames["deptlevelid"] == "1"]
    sgroup_frames["id_1"] = sgroup_frames.apply(lambda row: row["id"][:1], axis=1)
    sgroup_frames["id_2"] = sgroup_frames.apply(lambda row: row["id"][:3], axis=1)
    sgroup_frames["id_3"] = sgroup_frames.apply(lambda row: row["id"][:5], axis=1)
    sgroup_frames = sgroup_frames.merge(lv3, how="left", left_on="id_3", right_on="id", suffixes=("", "_lv3"))
    sgroup_frames = sgroup_frames.merge(lv2, how="left", left_on="id_2", right_on="id", suffixes=("", "_lv2"))
    sgroup_frames = sgroup_frames.merge(lv1, how="left", left_on="id_1", right_on="id", suffixes=("", "_lv1"))
    sgroup_frames = sgroup_frames[sgroup_frames["deptlevelid"] == "4"]
    sgroup_frames = sgroup_frames[["id", "name", "id_lv3", "name_lv3", "id_lv2", "name_lv2", "id_lv1", "name_lv1"]]
    sgroup_frames = sgroup_frames.rename(columns={
        "id": "foreign_category_lv4",
        "name": "foreign_category_lv4_name",
        "id_lv3": "foreign_category_lv3",
        "name_lv3": "foreign_category_lv3_name",
        "id_lv2": "foreign_category_lv2",
        "name_lv2": "foreign_category_lv2_name",
        "id_lv1": "foreign_category_lv1",
        "name_lv1": "foreign_category_lv1_name",
    })

    goodsflow_frames["deptid1"] = goodsflow_frames.apply(lambda row: row["deptid"][:7], axis=1)
    goodsflow_frames = goodsflow_frames.merge(sgroup_frames, how="left", left_on="deptid1", right_on="foreign_category_lv4")
    goodsflow_frames = goodsflow_frames.merge(data_frames["dept"], how="left", left_on="deptid", right_on="id", suffixes=("", "_lv5"))
    goodsflow_frames = goodsflow_frames[(goodsflow_frames["id"].notna()) & (goodsflow_frames["goodsid"].notna())]

    goodsflow_frames["source_id"] = source_id
    goodsflow_frames["cmid"] = cmid
    goodsflow_frames["last_updated"] = datetime.now()

    def quatity(row):

        x = row["amount"]/row["x"] if row["x"] != 0 else 0
        return x
    goodsflow_frames["quantity"] = goodsflow_frames.apply(quatity, axis=1)

    def subtotal(row):
        x = row["salevalue"] - row["discvalue"]
        return x
    goodsflow_frames["subtotal"] = goodsflow_frames.apply(subtotal, axis=1)

    def saletime(row):
        date = row["sdate"].split(" ")[0]
        hour = row["stime"][0:2]
        minute = row["stime"][2:4]
        secend = row["stime"][4:6]
        time = f"{date} {hour}:{minute}:{secend}"
        return time

    goodsflow_frames["saletime"] = goodsflow_frames.apply(saletime, axis=1)
    goodsflow_frames = goodsflow_frames.rename(columns={
        "id": "foreign_store_id",
        "name.store": "store_name",
        "listno": "receipt_id",
        "cardno": "consumer_id",
        "goodsid": "foreign_item_id",
        "usebarcodeid": "barcode",
        "name": "item_name",
        "unitname": "item_unit",
        "price": "saleprice",
        "id_lv5": "foreign_category_lv5",
        "name_lv5": "foreign_category_lv5_name",
        "posid": "pos_id"
    })

    goodsflow_frames = goodsflow_frames[[
        'source_id',
        'cmid',
        'foreign_store_id',
        'store_name',
        'receipt_id',
        'consumer_id',
        'saletime',
        'last_updated',
        'foreign_item_id',
        'barcode',
        'item_name',
        'item_unit',
        'saleprice',
        'quantity',
        'subtotal',
        'foreign_category_lv1',
        'foreign_category_lv1_name',
        'foreign_category_lv2',
        'foreign_category_lv2_name',
        'foreign_category_lv3',
        'foreign_category_lv3_name',
        'foreign_category_lv4',
        'foreign_category_lv4_name',
        'foreign_category_lv5',
        'foreign_category_lv5_name',
        'pos_id'
    ]]
    upload_to_s3(goodsflow_frames, source_id, date, target_table)

    return True


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
    cost_frames = data_frames["salecost"]
    cost_frames = cost_frames.merge(data_frames["goods"], on="goodsid")

    sgroup_frames = data_frames["sgroup"]
    lv3 = sgroup_frames[sgroup_frames["deptlevelid"] == "3"]
    lv2 = sgroup_frames[sgroup_frames["deptlevelid"] == "2"]
    lv1 = sgroup_frames[sgroup_frames["deptlevelid"] == "1"]
    sgroup_frames["id_1"] = sgroup_frames.apply(lambda row: row["id"][:1], axis=1)
    sgroup_frames["id_2"] = sgroup_frames.apply(lambda row: row["id"][:3], axis=1)
    sgroup_frames["id_3"] = sgroup_frames.apply(lambda row: row["id"][:5], axis=1)
    sgroup_frames = sgroup_frames.merge(lv3, how="left", left_on="id_3", right_on="id", suffixes=("", "_lv3"))
    sgroup_frames = sgroup_frames.merge(lv2, how="left", left_on="id_2", right_on="id", suffixes=("", "_lv2"))
    sgroup_frames = sgroup_frames.merge(lv1, how="left", left_on="id_1", right_on="id", suffixes=("", "_lv1"))
    sgroup_frames = sgroup_frames[sgroup_frames["deptlevelid"] == "4"]
    sgroup_frames = sgroup_frames[["id", "id_lv3", "id_lv2", "id_lv1"]]
    sgroup_frames = sgroup_frames.rename(columns={
        "id": "foreign_category_lv4",
        "id_lv3": "foreign_category_lv3",
        "id_lv2": "foreign_category_lv2",
        "id_lv1": "foreign_category_lv1",
    })

    cost_frames["deptid1"] = cost_frames.apply(lambda row: row["deptid"][:7], axis=1)
    cost_frames = cost_frames.merge(sgroup_frames, how="left", left_on="deptid1", right_on="foreign_category_lv4")
    cost_frames = cost_frames.merge(data_frames["dept"], how="left", left_on="deptid", right_on="id", suffixes=("", "_lv5"))

    def generate_total_sale(row):
        res = row["salevalue"] - row["discvalue"]
        return res
    cost_frames["total_sale"] = cost_frames.apply(generate_total_sale, axis=1)
    cost_frames = cost_frames.groupby([
        "shopid", "goodsid", "sdate", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4", "id"], as_index=False).\
        agg({"qty": np.sum, "costvalue": np.sum, "total_sale": np.sum})
    cost_frames["source_id"] = source_id
    cost_frames["cmid"] = cmid
    cost_frames["cost_type"] = ""
    cost_frames = cost_frames.rename(columns={
        "shopid": "foreign_store_id",
        "goodsid": "foreign_item_id",
        "sdate": "date",
        "qty": "total_quantity",
        "costvalue": "total_cost",
        "id": "foreign_category_lv5",
    })

    cost_frames = cost_frames[[
        "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
        "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
        "foreign_category_lv5", "cmid"
    ]]

    upload_to_s3(cost_frames, source_id, date, target_table)
    return True


def clean_goods(source_id, date, target_table, data_frames):
    """
    清洗商品
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    """
    origin_table_columns = {
        "goods": ['barcodeid', 'goodsid', 'name', 'unitname', 'flag', 'deptid', 'keepdays'],
        "cost": ['cost', 'goodsid', 'shopid', 'flag'],
        "goodsshop": ['price', 'goodsid', 'shopid'],
    }

    coverts = {"goods": {"barcodeid": str, "goodsid": str, "deptid": str, "goodsid": str},
               "cost": {"goodsid": str, "shopid": str},
               "goodsshop": {"goodsid": str}}
    """
    cmid = source_id.split("Y")[0]

    goods_frames = data_frames["goods"]
    goods_frames = goods_frames.merge(data_frames["cost"], on="goodsid", suffixes=("", ".b"))
    goods_frames = goods_frames[(goods_frames["shopid"] == "A00A") & (goods_frames["flag.b"] == "0")]
    goods_frames = goods_frames.merge(data_frames["goodsshop"], on="goodsid", suffixes=("", ".c"))
    goods_frames = goods_frames[goods_frames["shopid.c"] == 'A00A']

    def item_status(x):
        if x == "0":
            res = "正常"
        elif x == "8":
            res = "新品"
        elif x == "1":
            res = "暂停订货"
        elif x == "2":
            res = "暂停销售"
        elif x == "3":
            res = "清理"
        elif x == "4":
            res = "暂停经营"
        else:
            res = ""
        return res
    goods_frames['item_status'] = goods_frames.flag.apply(lambda x: item_status(x))
    goods_frames["cmid"] = cmid
    goods_frames['foreign_category_lv1'] = goods_frames.deptid.apply(lambda x: str(x)[:1])
    goods_frames['foreign_category_lv2'] = goods_frames.deptid.apply(lambda x: str(x)[:3])
    goods_frames['foreign_category_lv3'] = goods_frames.deptid.apply(lambda x: str(x)[:5])
    goods_frames['foreign_category_lv4'] = goods_frames.deptid.apply(lambda x: str(x)[:7])
    goods_frames["storage_time"] = datetime.now()
    goods_frames["last_updated"] = datetime.now()
    goods_frames["isvalid"] = "1"
    goods_frames["allot_method"] = ""
    goods_frames["supplier_name"] = ""
    goods_frames["supplier_code"] = ""
    goods_frames["brand_name"] = ""
    goods_frames["show_code"] = goods_frames["goodsid"]
    goods_frames = goods_frames.rename(columns={
        "barcodeid": "barcode",
        "goodsid": "foreign_item_id",
        "name": "item_name",
        "cost": "lastin_price",
        "price": "sale_price",
        "unitname": "item_unit",
        "deptid": "foreign_category_lv5",
        "keepdays": "warranty",
    })

    goods_frames = goods_frames[[
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
    ]]

    upload_to_s3(goods_frames, source_id, date, target_table)

    return True


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
    sgroup_frames = data_frames["sgroup"]
    lv1 = sgroup_frames[sgroup_frames["id"].str.len() == 1].rename(columns=lambda x: f"lv1_{x}")
    lv2 = sgroup_frames[sgroup_frames["deptlevelid"] == "2"].rename(columns=lambda x: f"lv2_{x}")
    lv3 = sgroup_frames[sgroup_frames["deptlevelid"] == "3"].rename(columns=lambda x: f"lv3_{x}")
    lv4 = sgroup_frames[sgroup_frames["deptlevelid"] == "4"].rename(columns=lambda x: f"lv4_{x}")
    lv2["lv1_id"] = sgroup_frames.id.map(lambda x: x[:1])
    lv3["lv2_id"] = sgroup_frames.id.map(lambda x: x[:3])
    lv4["lv3_id"] = sgroup_frames.id.map(lambda x: x[:5])

    category1 = lv1.copy()
    category1 = category1[category1["lv1_deptlevelid"] == "1"]
    category1 = category1.rename(columns={
        "lv1_deptlevelid": "level",
        "lv1_id": "foreign_category_lv1",
        "lv1_name": "foreign_category_lv1_name"
    })
    category1["foreign_category_lv2"] = ""
    category1["foreign_category_lv2_name"] = ""
    category1["foreign_category_lv3"] = ""
    category1["foreign_category_lv3_name"] = ""
    category1["foreign_category_lv4"] = ""
    category1["foreign_category_lv4_name"] = ""
    category1["foreign_category_lv5"] = ""
    category1["foreign_category_lv5_name"] = ""
    category1["cmid"] = cmid
    category1["last_updated"] = datetime.now()
    category1 = category1[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category2 = pd.merge(lv2.copy(), lv1.copy(), how="left", on="lv1_id")
    category2 = category2.rename(columns={
        "lv2_deptlevelid": "level",
        "lv1_id": "foreign_category_lv1",
        "lv1_name": "foreign_category_lv1_name",
        "lv2_id": "foreign_category_lv2",
        "lv2_name": "foreign_category_lv2_name"
    })
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = ""
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = ""
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = ""
    category2["cmid"] = cmid
    category2["last_updated"] = datetime.now()
    category2 = category2[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category3 = pd.merge(lv3.copy(), lv2.copy(), how="left", on="lv2_id").merge(lv1.copy(), how="left", on="lv1_id")
    category3 = category3.rename(columns={
        "lv3_deptlevelid": "level",
        "lv1_id": "foreign_category_lv1",
        "lv1_name": "foreign_category_lv1_name",
        "lv2_id": "foreign_category_lv2",
        "lv2_name": "foreign_category_lv2_name",
        "lv3_id": "foreign_category_lv3",
        "lv3_name": "foreign_category_lv3_name",
    })
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = ""
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = ""
    category3["cmid"] = cmid
    category3["last_updated"] = datetime.now()
    category3 = category3[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category4 = pd.merge(lv4.copy(), lv3.copy(), how="left", on="lv3_id").merge(lv2.copy(), how="left", on="lv2_id").\
        merge(lv1.copy(), how="left", on="lv1_id")
    category4 = category4.rename(columns={
        "lv4_deptlevelid": "level",
        "lv1_id": "foreign_category_lv1",
        "lv1_name": "foreign_category_lv1_name",
        "lv2_id": "foreign_category_lv2",
        "lv2_name": "foreign_category_lv2_name",
        "lv3_id": "foreign_category_lv3",
        "lv3_name": "foreign_category_lv3_name",
        "lv4_id": "foreign_category_lv4",
        "lv4_name": "foreign_category_lv4_name"
    })
    category4["foreign_category_lv5"] = ""
    category4["foreign_category_lv5_name"] = ""
    category4["cmid"] = cmid
    category4["last_updated"] = datetime.now()
    category4 = category4[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category5 = data_frames["dept"]
    category5["lv4_id"] = category5.id.apply(lambda x: x[:7])
    category5 = category5.merge(lv4.copy(), how="left", on="lv4_id").merge(lv3.copy(), how="left", on="lv3_id").\
        merge(lv2.copy(), how="left", on="lv2_id").merge(lv1.copy(), how="left", on="lv1_id")
    category5 = category5.rename(columns={
        "lv1_id": "foreign_category_lv1",
        "lv1_name": "foreign_category_lv1_name",
        "lv2_id": "foreign_category_lv2",
        "lv2_name": "foreign_category_lv2_name",
        "lv3_id": "foreign_category_lv3",
        "lv3_name": "foreign_category_lv3_name",
        "lv4_id": "foreign_category_lv4",
        "lv4_name": "foreign_category_lv4_name",
        "id": "foreign_category_lv5",
        "name": "foreign_category_lv5_name",
    })
    category5["cmid"] = cmid
    category5["last_updated"] = datetime.now()
    category5["level"] = "5"
    category5 = category5[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category = pd.concat([category1, category2, category3, category4, category5])
    upload_to_s3(category, source_id, date, target_table)

    return True


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
    store_frames = data_frames["shop"]
    store_frames["cmid"] = cmid
    store_frames["address_code"] = ""
    store_frames["device_id"] = ""

    def generate_stor_status(x):
        if x == "11":
            res = "正常营业"
        elif x == "98":
            res = "闭店"
        else:
            res = "其它"
        return res
    store_frames["store_status"] = store_frames.shoptype.apply(generate_stor_status)
    store_frames["create_date"] = datetime.now()
    store_frames["lat"] = None
    store_frames["lng"] = None
    store_frames["contacts"] = None
    store_frames["area_code"] = None
    store_frames["area_name"] = None
    store_frames["business_area"] = None
    store_frames["property_id"] = None
    store_frames["property"] = "直营店"
    store_frames["source_id"] = source_id
    store_frames["last_updated"] = datetime.now()
    store_frames["show_code"] = store_frames["id"]
    store_frames = store_frames.rename(columns={
        "id": "foreign_store_id",
        "name": "store_name",
        "address": "store_address",
        "linktele": "phone_number"
    })
    store_frames = store_frames[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    upload_to_s3(store_frames, source_id, date, target_table)

    return True


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
    pass


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp
