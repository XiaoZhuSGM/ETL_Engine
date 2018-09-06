"""
海信商定天下清洗逻辑
销售，成本，库存，商品， 分类
#cost
        'origin_table_columns': {
            "tbgoods": ["goodscode", "categoryitemcode", "categorycode"],
            "tbgoodscategory": ["categoryitemcode", "categorylevel", "categorycode"],
            "tb_goodsdaypssm": ["nodecode", "goodscode", "occurdate", "saleamount", "saleincome", "saletax",
                                "taxsalecost"]
        },
        'converts': {
            "tbgoods": {"goodscode": "str", "categorycode": "str"},
            "tb_goodsdaypssm": {"goodscode": "str", "occurdate": "str"}
        }
    }

#goods
    goods_event = {
        "source_id": "60YYYYYYYYYYYYY",
        "erp_name": "百年",
        "date": "2018-08-16",
        "target_table": "goods",
        'origin_table_columns': {
            "tbgoods": ["basebarcode", "goodscode", "goodsname", "purchprice", "saleprice", "basemeasureunit",
                        "durability", "workstatecode", "circulationmodecode", "goodsbrand", "suppliercode",
                        "categorycode", "categoryitemcode"],
            "tbgoodsbrand": ["brandcode", "brandname"],
            "tbgoodscategory": ["categorycode", "categoryitemcode", "categorylevel"],
            "tbgoodscirculation": ["circulationmodecode", "circulationmodename"],
            "tbgoodsworkstate": ["workstatecode", "workstatename"],
            "tbsupplier": ["suppliercode","suppliername"]
        },
        'converts': {
            "tbgoods": {"goodscode": "str", "categorycode": "str"},
            "tbgoodscategory": {"categorycode": "str"}
        }
    }


# category
    category_event = {
        "source_id": "60YYYYYYYYYYYYY",
        "erp_name": "百年",
        "date": "2018-08-16",
        "target_table": "category",
        'origin_table_columns': {
            "tbgoodscategory": ["categorylevel", "categoryitemcode", "categoryname", "parentcategorycode",
                                "categorycode"]
        },
        'converts': {
            "tbgoodscategory": {"categorycode": "str", "categoryitemcode": "str"}
        }
    }



# store
    store_event = {
        "source_id": "60YYYYYYYYYYYYY",
        "erp_name": "百年",
        "date": "2018-08-16",
        "target_table": "store",
        'origin_table_columns': {
            "tbdepartment": ["nodecode", "address", "state", "opendate", "phonenum", "isjoin"],
            "tbnode": ["nodecode", "nodename", "nodetype"]
        },
        'converts': {
            "tbdepartment": {"opendate": "str"}
        }
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


def clean_bainian(source_id, date, target_table, data_frames):
    if target_table == "cost":
        clean_cost(source_id, date, target_table, data_frames)
    elif target_table == "store":
        clean_store(source_id, date, target_table, data_frames)
    elif target_table == "goods":
        clean_goods(source_id, date, target_table, data_frames)
    elif target_table == "category":
        clean_category(source_id, date, target_table, data_frames)
    else:
        pass


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
    cost_frames = data_frames["tb_goodsdaypssm"].rename(columns=lambda x: f"cost.{x}")
    item_frames = data_frames["tbgoods"].rename(columns=lambda x: f"item.{x}")
    lv1_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv3.{x}")
    lv4_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv4.{x}")
    lv5_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv5.{x}")

    cost = cost_frames.merge(item_frames, how="left", left_on="cost.goodscode", right_on="item.goodscode")
    cost["lv1"] = cost["item.categorycode"].apply(lambda x: x[:1] if x is not np.NAN else "")
    cost["lv2"] = cost["item.categorycode"].apply(lambda x: x[:3] if x is not np.NAN else "")
    cost["lv3"] = cost["item.categorycode"].apply(lambda x: x[:5] if x is not np.NAN else "")
    cost["lv4"] = cost["item.categorycode"].apply(lambda x: x[:7] if x is not np.NAN else "")
    cost["lv5"] = cost["item.categorycode"].apply(lambda x: x[:9] if x is not np.NAN else "")

    cost = cost.merge(lv1_frames,
                      how="left",
                      left_on=["lv1", "item.categoryitemcode"],
                      right_on=["lv1.categorycode", "lv1.categoryitemcode"]).\
        merge(lv2_frames,
              how="left",
              left_on=["lv2", "item.categoryitemcode"],
              right_on=["lv2.categorycode", "lv2.categoryitemcode"]).\
        merge(lv3_frames,
              how="left",
              left_on=["lv3", "item.categoryitemcode"],
              right_on=["lv3.categorycode", "lv3.categoryitemcode"])

    lv4_frames = lv4_frames[lv4_frames["lv4.categorylevel"] == 4]
    lv5_frames = lv5_frames[lv5_frames["lv5.categorylevel"] == 5]
    cost = cost.merge(lv4_frames,
                      how="left",
                      left_on=["lv4", "item.categoryitemcode"],
                      right_on=["lv4.categorycode", "lv4.categoryitemcode"])
    cost = cost.merge(lv5_frames,
                      how="left",
                      left_on=["lv5", "item.categoryitemcode"],
                      right_on=["lv5.categorycode", "lv5.categoryitemcode"])

    cost = cost.rename(columns={
        "cost.nodecode": "foreign_store_id",
        "cost.goodscode": "foreign_item_id",
        "cost.saleamount": "total_quantity",
        "cost.taxsalecost": "total_cost",
        "lv1.categorycode": "foreign_category_lv1",
        "lv2.categorycode": "foreign_category_lv2",
        "lv3.categorycode": "foreign_category_lv3",
    })

    cost["cmid"] = cmid
    cost["source_id"] = source_id
    cost["date"] = cost["cost.occurdate"].apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime('%Y-%m-%d'))
    cost["cost_type"] = ""
    cost["total_sale"] = cost.apply(lambda row: row["cost.saleincome"] + row["cost.saletax"], axis=1)
    cost["foreign_category_lv4"] = cost["lv4.categorycode"].apply(lambda x: x if x else "")
    cost["foreign_category_lv5"] = cost["lv5.categorycode"].apply(lambda x: x if x else "")

    cost = cost[[
        "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
        "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
        "foreign_category_lv5", "cmid"
    ]]
    upload_to_s3(cost, source_id, date, target_table)
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
    cmid = source_id.split("Y")[0]
    item_frames = data_frames["tbgoods"].rename(columns=lambda x: f"item.{x}")
    status_frames = data_frames["tbgoodsworkstate"].rename(columns=lambda x: f"status.{x}")
    allot_frames = data_frames["tbgoodscirculation"].rename(columns=lambda x: f"allot.{x}")
    brand_frames = data_frames["tbgoodsbrand"].rename(columns=lambda x: f"brand.{x}")
    supp_frames = data_frames["tbsupplier"].rename(columns=lambda x: f"supp.{x}")
    lv1_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv3.{x}")
    lv4_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv4.{x}")
    lv5_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv5.{x}")

    goods = item_frames.merge(status_frames, how="left", left_on="item.workstatecode", right_on="status.workstatecode")\
        .merge(allot_frames, how="left", left_on="item.circulationmodecode", right_on="allot.circulationmodecode")\
        .merge(brand_frames, how="left", left_on="item.goodsbrand", right_on="brand.brandcode")\
        .merge(supp_frames, how="left", left_on="item.suppliercode", right_on="supp.suppliercode")

    goods["lv1"] = goods["item.categorycode"].apply(lambda x: x[:1] if x is not np.NAN else "")
    goods["lv2"] = goods["item.categorycode"].apply(lambda x: x[:3] if x is not np.NAN else "")
    goods["lv3"] = goods["item.categorycode"].apply(lambda x: x[:5] if x is not np.NAN else "")
    goods["lv4"] = goods["item.categorycode"].apply(lambda x: x[:7] if x is not np.NAN else "")
    goods["lv5"] = goods["item.categorycode"].apply(lambda x: x[:9] if x is not np.NAN else "")
    goods = goods.merge(lv1_frames,
                        how="left",
                        left_on=["lv1", "item.categoryitemcode"],
                        right_on=["lv1.categorycode", "lv1.categoryitemcode"]).\
        merge(lv2_frames,
              how="left",
              left_on=["lv2", "item.categoryitemcode"],
              right_on=["lv2.categorycode", "lv2.categoryitemcode"]).\
        merge(lv3_frames,
              how="left",
              left_on=["lv3", "item.categoryitemcode"],
              right_on=["lv3.categorycode", "lv3.categoryitemcode"])
    lv4_frames = lv4_frames[lv4_frames["lv4.categorylevel"] == 4]
    lv5_frames = lv5_frames[lv5_frames["lv5.categorylevel"] == 5]
    goods = goods.merge(lv4_frames,
                        how="left",
                        left_on=["lv4", "item.categoryitemcode"],
                        right_on=["lv4.categorycode", "lv4.categoryitemcode"])
    goods = goods.merge(lv5_frames,
                        how="left",
                        left_on=["lv5", "item.categoryitemcode"],
                        right_on=["lv5.categorycode", "lv5.categoryitemcode"])

    goods = goods.rename(columns={
        "item.basebarcode": "barcode",
        "item.goodscode": "foreign_item_id",
        "item.goodsname": "item_name",
        "item.purchprice": "lastin_price",
        "item.saleprice": "sale_price",
        "item.basemeasureunit": "item_unit",
        "status.workstatename": "item_status",
        "lv1.categorycode": "foreign_category_lv1",
        "lv2.categorycode": "foreign_category_lv2",
        "lv3.categorycode": "foreign_category_lv3",
        "lv4.categorycode": "foreign_category_lv4",
        "lv5.categorycode": "foreign_category_lv5",
        "item.durability": "warranty",
        "allot.circulationmodename": "allot_method",
        "supp.suppliername": "supplier_name",
        "supp.suppliercode": "supplier_code",
        "brand.brandname": "brand_name"
    })

    goods["cmid"] = cmid
    goods["storage_time"] = datetime.now(_TZINFO)
    goods["last_updated"] = datetime.now(_TZINFO)
    goods["isvalid"] = "1"
    goods["show_code"] = goods["foreign_item_id"]
    goods = goods[[
        "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
        "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4", "storage_time",
        "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method", "supplier_name",
        "supplier_code",
        "brand_name",
    ]]

    upload_to_s3(goods, source_id, date, target_table)
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
    lv1_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv1.{x}")
    lv2_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv2.{x}")
    lv3_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv3.{x}")
    lv4_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv4.{x}")
    lv5_frames = data_frames["tbgoodscategory"].rename(columns=lambda x: f"lv5.{x}")

    category1 = lv1_frames[(lv1_frames["lv1.categorylevel"] == 1) & (lv1_frames["lv1.categoryitemcode"] == "0000")]

    category1 = category1.rename(columns={
        "lv1.categorycode": "foreign_category_lv1",
        "lv1.categoryname": "foreign_category_lv1_name"
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

    category2 = lv2_frames.copy()
    category2 = category2.merge(lv1_frames, how="left", left_on=["lv2.parentcategorycode", "lv2.categoryitemcode"],
                                right_on=["lv1.categorycode", "lv1.categoryitemcode"])
    category2 = category2[(category2["lv2.categorylevel"] == 2) & (category2["lv2.categoryitemcode"] == "0000")]
    category2 = category2.rename(columns={
        "lv1.categorycode": "foreign_category_lv1",
        "lv1.categoryname": "foreign_category_lv1_name",
        "lv2.categorycode": "foreign_category_lv2",
        "lv2.categoryname": "foreign_category_lv2_name"
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
    category3 = category3.merge(lv2_frames, how="left", left_on=["lv3.parentcategorycode", "lv3.categoryitemcode"],
                                right_on=["lv2.categorycode", "lv2.categoryitemcode"])\
        .merge(lv1_frames, how="left", left_on=["lv2.parentcategorycode", "lv2.categoryitemcode"],
                                right_on=["lv1.categorycode", "lv1.categoryitemcode"])
    category3 = category3[(category3["lv3.categorylevel"] == 3) & (category3["lv3.categoryitemcode"] == "0000")]
    category3 = category3.rename(columns={
        "lv1.categorycode": "foreign_category_lv1",
        "lv1.categoryname": "foreign_category_lv1_name",
        "lv2.categorycode": "foreign_category_lv2",
        "lv2.categoryname": "foreign_category_lv2_name",
        "lv3.categorycode": "foreign_category_lv3",
        "lv3.categoryname": "foreign_category_lv3_name"
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
    category4 = category4.merge(lv3_frames,
                                how="left",
                                left_on=["lv4.parentcategorycode", "lv4.categoryitemcode"],
                                right_on=["lv3.categorycode", "lv3.categoryitemcode"]).\
        merge(lv2_frames,
              how="left",
              left_on=["lv3.parentcategorycode", "lv3.categoryitemcode"],
              right_on=["lv2.categorycode", "lv2.categoryitemcode"]).\
        merge(lv1_frames,
              how="left",
              left_on=["lv2.parentcategorycode", "lv2.categoryitemcode"],
              right_on=["lv1.categorycode", "lv1.categoryitemcode"])
    category4 = category4[(category4["lv4.categorylevel"] == 4) & (category4["lv4.categoryitemcode"] == "0000")]
    category4 = category4.rename(columns={
        "lv1.categorycode": "foreign_category_lv1",
        "lv1.categoryname": "foreign_category_lv1_name",
        "lv2.categorycode": "foreign_category_lv2",
        "lv2.categoryname": "foreign_category_lv2_name",
        "lv3.categorycode": "foreign_category_lv3",
        "lv3.categoryname": "foreign_category_lv3_name",
        "lv4.categorycode": "foreign_category_lv4",
        "lv4.categoryname": "foreign_category_lv4_name"
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

    category5 = lv5_frames.copy()
    category5 = category5.merge(lv4_frames,
                                how="left",
                                left_on=["lv5.parentcategorycode", "lv5.categoryitemcode"],
                                right_on=["lv4.categorycode", "lv4.categoryitemcode"]).\
        merge(lv3_frames,
              how="left",
              left_on=["lv4.parentcategorycode", "lv4.categoryitemcode"],
              right_on=["lv3.categorycode", "lv3.categoryitemcode"]).\
        merge(lv2_frames,
              how="left",
              left_on=["lv3.parentcategorycode", "lv3.categoryitemcode"],
              right_on=["lv2.categorycode", "lv2.categoryitemcode"]).\
        merge(lv1_frames,
              how="left",
              left_on=["lv2.parentcategorycode", "lv2.categoryitemcode"],
              right_on=["lv1.categorycode", "lv1.categoryitemcode"])

    category5 = category5[(category5["lv5.categorylevel"] == 5) & (category5["lv5.categoryitemcode"] == "0000")]
    category5 = category5.rename(columns={
        "lv1.categorycode": "foreign_category_lv1",
        "lv1.categoryname": "foreign_category_lv1_name",
        "lv2.categorycode": "foreign_category_lv2",
        "lv2.categoryname": "foreign_category_lv2_name",
        "lv3.categorycode": "foreign_category_lv3",
        "lv3.categoryname": "foreign_category_lv3_name",
        "lv4.categorycode": "foreign_category_lv4",
        "lv4.categoryname": "foreign_category_lv4_name",
        "lv5.categorycode": "foreign_category_lv5",
        "lv5.categoryname": "foreign_category_lv5_name",
    })
    category5["cmid"] = cmid
    category5["level"] = 5
    category5["last_updated"] = datetime.now(_TZINFO)
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

    store = data_frames["tbnode"].merge(data_frames["tbdepartment"], how="left")
    store = store[store.nodetype == 0]
    store = store.rename(columns={
        "nodecode": "foreign_store_id",
        "nodename": "store_name",
        "address": "store_address",
        "phonenum": "phone_number",
        "isjoin": "property_id"
    })
    store["cmid"] = cmid
    store["address_code"] = ""
    store["device_id"] = ""
    store["store_status"] = store.state.apply(lambda x: "正常" if x == 0 else "闭店")
    store["create_date"] = store.opendate.apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime('%Y-%m-%d %H:%M:%S'))
    store["lat"] = None
    store["lng"] = None
    store["show_code"] = store["foreign_store_id"]
    store["contacts"] = ""
    store["area_code"] = ""
    store["area_name"] = ""
    store["business_area"] = None
    store["property"] = store.property_id.apply(lambda x: "自营" if x == 0 else "加盟")
    store["source_id"] = source_id
    store["last_updated"] = datetime.now(_TZINFO)

    store = store[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    upload_to_s3(store, source_id, date, target_table)
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
