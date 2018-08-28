"""
便宅家清洗逻辑
销售，成本，库存，商品， 分类

# goodsflow
        'origin_table_columns': {
            "zetl_saleflow": ['billnumber', 'occurdate', 'price', 'qty', 'amount', 'storeuuid', 'productuuid'],
            "zetl_store": ['uuid', 'name'],
            "zetl_product": ['uuid', 'barcode', 'name', 'munit', 'categoryid'],
            "zetl_category": ['uuid', 'name'],
        },

        'converts': {
        }

# cost
        'origin_table_columns': {
            "zetl_product": ["uuid", "categoryid"],
            "zetl_salecost": ["storeuuid", "productuuid", "occurdate", "buscls", "qty", "saleamount", "costamount"]
        },

        'converts': {
        }


# goods
        'origin_table_columns': {
            "zetl_brand": ["name", "uuid"],
            "zetl_category": ["uuid"],
            "zetl_product": ["barcode", "uuid", "name", "inprc", "rtlprc", "munit", "lifecyclename", "statusid",
                             "validperiod", "id", "deliverytype", "categoryid", "provideruuid", "brandid"],
            "zetl_provider": ["name", "id", "uuid"]
        },

        'converts': {
        }



#category
        'origin_table_columns': {
            "zetl_category": ["uuid", "name", "parentid", "grade"]
        },

        'converts': {
            "zetl_category": {"grade": "str", "parentid": "str"}
        }


#store
        'origin_table_columns': {
            "zetl_store": ["uuid", "name", "address", "statusname", "createdate", "id", "contactorphone", "contactor",
                           "areaid", "areaname", "businessareaname", "typeid", "typename"]
        },

        'converts': {
        }


#goodsloss
        'origin_table_columns': {
            "zetl_invmod": ["billnumber", "occurdate", "qty", "amount", "storeuuid", "productuuid"],
            "zetl_store": ["uuid", "id", "name"],
            "zetl_product": ["uuid", "id", "barcode", "name", "munit", "uuid", "categoryid"],
            "zetl_category": ["uuid"]
        },

        'converts': {
            "zetl_product": {"categoryid": "str"}
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


def clean_bianzhaijia(source_id, date, target_table, data_frames):
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
    elif target_table == "goodsloss":
        clean_goodsloss(source_id, date, target_table, data_frames)
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
    flow_frames = data_frames["zetl_saleflow"].rename(columns=lambda x: f"flow.{x}")
    store_frames = data_frames["zetl_store"].rename(columns=lambda x: f"store.{x}")
    item_frames = data_frames["zetl_product"].rename(columns=lambda x: f"item.{x}")
    lv1_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv3.{x}")

    goodsflow_frames = flow_frames.merge(store_frames, how="left", left_on="flow.storeuuid", right_on="store.uuid").\
        merge(item_frames, how="left", left_on="flow.productuuid", right_on="item.uuid")

    goodsflow_frames["lv1.uuid"] = goodsflow_frames["item.categoryid"].apply(lambda x: x[:2])
    goodsflow_frames["lv2.uuid"] = goodsflow_frames["item.categoryid"].apply(lambda x: x[:4])
    goodsflow_frames["lv3.uuid"] = goodsflow_frames["item.categoryid"].apply(lambda x: x[:6])

    goodsflow_frames = goodsflow_frames.merge(lv1_frames, how="left", on="lv1.uuid").\
        merge(lv2_frames, how="left", on="lv2.uuid").merge(lv3_frames, how="left", on="lv3.uuid")

    goodsflow_frames["source_id"] = source_id
    goodsflow_frames["cmid"] = cmid
    goodsflow_frames["consumer_id"] = ""
    goodsflow_frames["last_updated"] = datetime.now()
    goodsflow_frames["foreign_category_lv4"] = ""
    goodsflow_frames["foreign_category_lv4_name"] = ""
    goodsflow_frames["foreign_category_lv5"] = ""
    goodsflow_frames["foreign_category_lv5_name"] = ""
    goodsflow_frames["pos_id"] = ""

    goodsflow_frames = goodsflow_frames.rename(columns={
        "store.uuid": "foreign_store_id",
        "store.name": "store_name",
        "flow.billnumber": "receipt_id",
        "flow.occurdate": "saletime",
        "item.uuid": "foreign_item_id",
        "item.barcode": "barcode",
        "item.name": "item_name",
        "item.munit": "item_unit",
        "flow.price": "saleprice",
        "flow.qty": "quantity",
        "flow.amount": "subtotal",
        "lv1.uuid": "foreign_category_lv1",
        "lv1.name": "foreign_category_lv1_name",
        "lv2.uuid": "foreign_category_lv2",
        "lv2.name": "foreign_category_lv2_name",
        "lv3.uuid": "foreign_category_lv3",
        "lv3.name": "foreign_category_lv3_name",
    })

    goodsflow_frames = goodsflow_frames[[
        'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime', 'last_updated',
        'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
        'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
        'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
        'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'
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
    cost_frames = data_frames["zetl_salecost"].merge(data_frames["zetl_product"],
                                                     how="left", left_on="productuuid", right_on="uuid")
    cost_frames["foreign_category_lv1"] = cost_frames["categoryid"].apply(lambda x: x[:2])
    cost_frames["foreign_category_lv2"] = cost_frames["categoryid"].apply(lambda x: x[:4])
    cost_frames["foreign_category_lv3"] = cost_frames["categoryid"].apply(lambda x: x[:6])
    cost_frames["foreign_category_lv4"] = ""
    cost_frames["foreign_category_lv5"] = ""
    cost_frames["source_id"] = source_id
    cost_frames["cmid"] = cmid

    cost_frames = cost_frames.rename(columns={
        "storeuuid": "foreign_store_id",
        "productuuid": "foreign_item_id",
        "occurdate": "date",
        "buscls": "cost_type",
        "qty": "total_quantity",
        "saleamount": "total_sale",
        "costamount": "total_cost"
    })
    cost_frames["date"] = cost_frames["date"].apply(lambda row: row.split()[0])
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

    item_frames = data_frames["zetl_product"].rename(columns=lambda x: f"item.{x}")
    lv1_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv3.{x}")
    vendor_frames = data_frames["zetl_provider"].rename(columns=lambda x: f"vendor.{x}")
    brand_frames = data_frames["zetl_brand"].rename(columns=lambda x: f"brand.{x}")

    item_frames["lv1.uuid"] = item_frames["item.categoryid"].apply(lambda x: x[:2])
    item_frames["lv2.uuid"] = item_frames["item.categoryid"].apply(lambda x: x[:4])
    item_frames["lv3.uuid"] = item_frames["item.categoryid"].apply(lambda x: x[:6])

    goods_frames = item_frames.merge(lv1_frames, how="left", on="lv1.uuid").\
                               merge(lv2_frames, how="left", on="lv2.uuid").\
                               merge(lv3_frames, how="left", on="lv3.uuid")

    goods_frames = goods_frames.merge(vendor_frames, how="left", left_on="item.provideruuid", right_on="vendor.uuid").\
        merge(brand_frames, how="left", left_on="item.brandid", right_on="brand.uuid")

    goods_frames["cmid"] = cmid
    goods_frames["foreign_category_lv4"] = ""
    goods_frames["foreign_category_lv5"] = ""
    goods_frames["storage_time"] = datetime.now()
    goods_frames["last_updated"] = datetime.now()

    goods_frames = goods_frames.rename(columns={
        "item.barcode": "barcode",
        "item.uuid": "foreign_item_id",
        "item.name": "item_name",
        "item.inprc": "lastin_price",
        "item.rtlprc": "sale_price",
        "item.munit": "item_unit",
        "item.lifecyclename": "item_status",
        "lv1.uuid": "foreign_category_lv1",
        "lv2.uuid": "foreign_category_lv2",
        "lv3.uuid": "foreign_category_lv3",
        "item.statusid": "isvalid",
        "item.validperiod": "warranty",
        "item.id": "show_code",
        "item.deliverytype": "allot_method",
        "vendor.name": "supplier_name",
        "vendor.id": "supplier_code",
        "brand.name": "brand_name"
    })

    goods_frames = goods_frames[[
        "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
        "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4", "storage_time",
        "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method", "supplier_name",
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

    lv1_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv3.{x}")

    category1 = lv1_frames.copy()
    category1 = category1[category1["lv1.grade"] == "1"]
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
    category1["last_updated"] = datetime.now()
    category1 = category1.rename(columns={
        "lv1.uuid": "foreign_category_lv1",
        "lv1.name": "foreign_category_lv1_name"
    })
    category1 = category1[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category2 = lv2_frames.copy().merge(lv1_frames.copy(), how="left", left_on="lv2.parentid", right_on="lv1.uuid")
    category2 = category2[category2["lv2.grade"] == "2"]
    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = ""
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = ""
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = ""
    category2["last_updated"] = datetime.now()
    category2 = category2.rename(columns={
        "lv1.uuid": "foreign_category_lv1",
        "lv1.name": "foreign_category_lv1_name",
        "lv2.uuid": "foreign_category_lv2",
        "lv2.name": "foreign_category_lv2_name",
    })
    category2 = category2[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category3 = lv3_frames.copy().merge(lv2_frames.copy(), how="left", left_on="lv3.parentid", right_on="lv2.uuid").\
                            merge(lv1_frames.copy(), how="left", left_on="lv2.parentid", right_on="lv1.uuid")
    category3 = category3[category3["lv3.grade"] == "3"]
    category3["cmid"] = cmid
    category3["level"] = 3
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = ""
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = ""
    category3["last_updated"] = datetime.now()
    category3 = category3.rename(columns={
        "lv1.uuid": "foreign_category_lv1",
        "lv1.name": "foreign_category_lv1_name",
        "lv2.uuid": "foreign_category_lv2",
        "lv2.name": "foreign_category_lv2_name",
        "lv3.uuid": "foreign_category_lv3",
        "lv3.name": "foreign_category_lv3_name",
    })
    category3 = category3[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category = pd.concat([category1, category2, category3])

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
    store_frames = data_frames["zetl_store"]
    store_frames["cmid"] = cmid
    store_frames["source_id"] = source_id
    store_frames["address_code"] = ""
    store_frames["device_id"] = ""
    store_frames["lat"] = None
    store_frames["lng"] = None
    store_frames["last_updated"] = datetime.now()

    store_frames = store_frames.rename(columns={
        "uuid": "foreign_store_id",
        "name": "store_name",
        "address": "store_address",
        "statusname": "store_status",
        "createdate": "create_date",
        "contactorphone": "phone_number",
        "contactor": "contacts",
        "areaid": "area_code",
        "areaname": "area_name",
        "businessareaname": "business_area",
        "typeid": "property_id",
        "typename": "property",
        "id": "show_code",
    })
    store_frames = store_frames[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    upload_to_s3(store_frames, source_id, date, target_table)
    return True


def clean_goodsloss(source_id, date, target_table, data_frames):
    """
    商品损耗
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    loss_frames = data_frames["zetl_invmod"].rename(columns=lambda x: f"loss.{x}")
    store_frames = data_frames["zetl_store"].rename(columns=lambda x: f"store.{x}")
    item_frames = data_frames["zetl_product"].rename(columns=lambda x: f"item.{x}")
    lv1_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["zetl_category"].rename(columns=lambda x: f"lv3.{x}")

    goodsloss = loss_frames.merge(store_frames, how="left", left_on="loss.storeuuid", right_on="store.uuid").\
        merge(item_frames, how="left", left_on="loss.productuuid", right_on="item.uuid")

    goodsloss["lv1.uuid"] = goodsloss["item.categoryid"].apply(lambda x: x[:2])
    goodsloss["lv2.uuid"] = goodsloss["item.categoryid"].apply(lambda x: x[:4])
    goodsloss["lv3.uuid"] = goodsloss["item.categoryid"].apply(lambda x: x[:6])

    goodsloss = goodsloss.merge(lv1_frames, how="left", on="lv1.uuid").merge(lv2_frames, how="left", on="lv2.uuid").\
        merge(lv3_frames, how="left", on="lv3.uuid")

    goodsloss["cmid"] = cmid
    goodsloss["source_id"] = source_id
    goodsloss["foreign_category_lv4"] = ""
    goodsloss["foreign_category_lv5"] = ""

    goodsloss = goodsloss.rename(columns={
        "loss.billnumber": "lossnum",
        "loss.occurdate": "lossdate",
        "store.uuid": "foreign_store_id",
        "store.id": "store_show_code",
        "store.name": "store_name",
        "item.uuid": "foreign_item_id",
        "item.id": "item_showcode",
        "item.barcode": "barcode",
        "item.name": "item_name",
        "item.munit": "item_unit",
        "loss.qty": "quantity",
        "loss.amount": "subtotal",
        "lv1.uuid": "foreign_category_lv1",
        "lv2.uuid": "foreign_category_lv2",
        "lv3.uuid": "foreign_category_lv3"
    })

    goodsloss["lossdate"] = goodsloss["lossdate"].apply(lambda row: row.split()[0])
    goodsloss = goodsloss[[
        "cmid", "source_id", "lossnum", "lossdate", "foreign_store_id", "store_show_code", "store_name",
        "foreign_item_id", "item_showcode", "barcode", "item_name", "item_unit", "quantity", "subtotal",
        "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
        "foreign_category_lv5",
    ]]

    upload_to_s3(goodsloss, source_id, date, target_table)
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
