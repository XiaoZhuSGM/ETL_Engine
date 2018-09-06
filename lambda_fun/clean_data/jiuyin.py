"""
九垠清洗逻辑
销售，成本，库存，商品， 分类

# goodsflow
        'origin_table_columns': {
            "app_fdinfo": ["fdbh", "fdmc"],
            "bm_spdlxx": ["dlbmid", "dlmc"],
            "bm_spzlxx": ["zlbmid", "zlmc"],
            "bm_supertype": ["superbm", "supermc"],
            "uv_saledetail": ["fdbh", "spbm", "xsdbh", "xsrq", "xssj", "zxnsjg", "spsl", "zxssze"],
            "uv_spbaseinfo": ["spbm", "superbm", "dlbmid", "zlbmid", "spsmm", "spmc", "dw"]
        },
        'converts': {
            "app_fdinfo": {"fdbh": "str"},
            "bm_spdlxx": {"dlbmid": "str"},
            "bm_spzlxx": {"zlbmid": "str"},
            "bm_supertype": {"superbm": "str"},
            "uv_saledetail": {"fdbh": "str", "spbm": "str"},
            "uv_spbaseinfo": {"spbm": "str", "superbm": "str", "dlbmid": "str", "zlbmid": "str"}
        }

# cost
        'origin_table_columns': {
            "uv_saledetail": ["spbm", "fdbh", "xsrq", "spsl", "zxssze", "zxcbje"],
            "uv_spbaseinfo": ["spbm", "superbm", "dlbmid", "zlbmid"]
        },
        'converts': {
            "uv_saledetail": {"fdbh": "str"},
            "uv_spbaseinfo": {"superbm": "str", "dlbmid": "str", "zlbmid": "str"}
        }


# goods
        'origin_table_columns': {
            "bm_ghsshfs": ["shfsid", "shfsmc"],
            "bm_ppxx": ["ppbmid", "ppmc"],
            "gh_sjbasicinfo": ["ghsbh", "ghsmc"],
            "uv_sjhtxx": ["sjbh", "ghsbh", "shfsid"],
            "uv_spbaseinfo": ["sjbh", "ppbmid", "spsmm", "spbm", "spmc", "pjjj", "nsjg", "dw", "superbm", "dlbmid",
                              "zlbmid", "ztbz", "bzqts"]
        },
        'converts': {
            "gh_sjbasicinfo": {"ghsbh": "str"},
            "uv_sjhtxx": {"ghsbh": "str"},
            "uv_spbaseinfo": {"superbm": "str", "dlbmid": "str", "zlbmid": "str", "spbm": "str", "spsmm": "str"}
        }



#category
        'origin_table_columns': {
            "bm_spdlxx": ["superbm", "dlbmid", "dlmc"],
            "bm_spzlxx": ["dlbmid", "zlbmid", "zlmc"],
            "bm_supertype": ["superbm", "supermc"]
        },
        'converts': {
            "bm_supertype": {"superbm": "str"},
            "bm_spdlxx": {"superbm": "str", "dlbmid": "str"},
            "bm_spzlxx": {"dlbmid": "str", "zlbmid": "str"}
        }


#store
        'origin_table_columns': {
            "app_fdinfo": ["fdbh", "fdmc", "dz", "kdrq", "telephone", "fzr", "dcbhid", "fdmode"],
            "bm_dqxx": ["dqmc", "dqbhid"]
        },
        'converts': {
            "app_fdinfo": {"fdbh": "str", "dcbhid": "str"},
            "bm_dqxx": {"dqbhid": "str"}
        }


#goodsloss
        'origin_table_columns': {
            dj_bsmx: [bsdbh,rq,bssl,nsjg,fdbh,spbm],
            app_fdinfo: [fdbh,fdmc],
            uv_spbaseinfo: [spbm,spmc,spsmm,dw,superbm,dlbmid,zlbmid],
            bm_supertype: [superbm],
            bm_spdlxx: [dlbmid],
            bm_spzlxx: [zlbmid]
        },
        'converts': {
            "dj_bsmx": {"fdbh": "str", "spbm": "str"},
            "app_fdinfo": {"fdbh": "str"},
            "bm_dqxx": {"dqbhid": "str"},
            "uv_spbaseinfo": {"superbm": "str", "dlbmid": "str", "zlbmid": "str", "spbm": "str", "spsmm": "str"},
            "bm_supertype": {"superbm": "str"},
            "bm_spdlxx": {"dlbmid": "str"},
            "bm_spzlxx": {"zlbmid": "str"}
        }

"""
from datetime import datetime
import pandas as pd
import boto3
import tempfile
import pytz
import time
_TZINFO = pytz.timezone("Asia/Shanghai")

S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")

CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


def clean_jiuyin(source_id, date, target_table, data_frames):
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
    saleflow = data_frames["uv_saledetail"]
    store = data_frames["app_fdinfo"]
    goods = data_frames["uv_spbaseinfo"]
    lv1 = data_frames["bm_supertype"]
    lv2 = data_frames["bm_spdlxx"]
    lv3 = data_frames["bm_spzlxx"]

    saleflow.fdbh = saleflow.fdbh.apply(lambda x: x.strip())
    saleflow.spbm = saleflow.spbm.apply(lambda x: x.strip())
    store.fdbh = store.fdbh.apply(lambda x: x.strip())
    goods.spbm = goods.spbm.apply(lambda x: x.strip())
    goods.superbm = goods.superbm.apply(lambda x: x.strip())
    goods.dlbmid = goods.dlbmid.apply(lambda x: x.strip())
    goods.zlbmid = goods.zlbmid.apply(lambda x: x.strip())
    lv1.superbm = lv1.superbm.apply(lambda x: x.strip())
    lv2.dlbmid = lv2.dlbmid.apply(lambda x: x.strip())
    lv3.zlbmid = lv3.zlbmid.apply(lambda x: x.strip())

    goodsflow = saleflow.merge(store, how="left").merge(goods, how="left").merge(lv1, how="left")\
        .merge(lv2, how="left").merge(lv3, how="left").merge(lv3, how="left").merge(lv3, how="left")

    goodsflow = goodsflow.rename(columns={
        "fdbh": "foreign_store_id",
        "fdmc": "store_name",
        "xsdbh": "receipt_id",
        "spbm": "foreign_item_id",
        "spsmm": "barcode",
        "spmc": "item_name",
        "dw": "item_unit",
        "zxnsjg": "saleprice",
        "spsl": "quantity",
        "zxssze": "subtotal",
        "superbm": "foreign_category_lv1",
        "supermc": "foreign_category_lv1_name",
        "dlbmid": "foreign_category_lv2",
        "dlmc": "foreign_category_lv2_name",
        "zlbmid": "foreign_category_lv3",
        "zlmc": "foreign_category_lv3_name"
    })

    goodsflow["source_id"] = source_id
    goodsflow["cmid"] = cmid
    goodsflow["consumer_id"] = None
    goodsflow["saletime"] = goodsflow.apply(lambda row: row["xsrq"].strip() + " " + row["xssj"].strip(), axis=1)
    goodsflow["last_updated"] = datetime.now(_TZINFO)
    goodsflow["foreign_category_lv4"] = ""
    goodsflow["foreign_category_lv4_name"] = None
    goodsflow["foreign_category_lv5"] = ""
    goodsflow["foreign_category_lv5_name"] = None
    goodsflow["pos_id"] = ""

    goodsflow = goodsflow[[
        'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime', 'last_updated',
        'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
        'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
        'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
        'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'
    ]]
    upload_to_s3(goodsflow, source_id, date, target_table)
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

    cost = data_frames["uv_saledetail"].merge(data_frames["uv_spbaseinfo"], how="left")
    cost = cost.groupby(["xsrq", "fdbh", "spbm", "superbm", "dlbmid", "zlbmid"], as_index=False)\
        .agg({"spsl": sum, "zxssze": sum, "zxcbje": sum})

    cost.fdbh = cost.fdbh.apply(lambda x: x.strip())
    cost.spbm = cost.spbm.apply(lambda x: x.strip())
    cost.superbm = cost.superbm.apply(lambda x: x.strip())
    cost.dlbmid = cost.dlbmid.apply(lambda x: x.strip())
    cost.zlbmid = cost.zlbmid.apply(lambda x: x.strip())

    cost = cost.rename(columns={
        "fdbh": "foreign_store_id",
        "spbm": "foreign_item_id",
        "xsrq": "date",
        "spsl": "total_quantity",
        "zxssze": "total_sale",
        "zxcbje": "total_cost",
        "superbm": "foreign_category_lv1",
        "dlbmid": "foreign_category_lv2",
        "zlbmid": "foreign_category_lv3"
    })
    cost["source_id"] = source_id
    cost["cost_type"] = ""
    cost["foreign_category_lv4"] = ""
    cost["foreign_category_lv5"] = ""
    cost["cmid"] = cmid

    cost = cost[[
        "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
        "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
        "foreign_category_lv5", "cmid"]]

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

    item = data_frames["uv_spbaseinfo"]
    supp = data_frames["uv_sjhtxx"]
    gh_sjbasic = data_frames["gh_sjbasicinfo"]
    shfs = data_frames["bm_ghsshfs"]
    brand = data_frames["bm_ppxx"]

    item.spbm = item.spbm.apply(lambda x: x.strip())
    item.superbm = item.superbm.apply(lambda x: x.strip())
    item.dlbmid = item.dlbmid.apply(lambda x: x.strip())
    item.zlbmid = item.zlbmid.apply(lambda x: x.strip())

    goods = item.merge(supp, how="left").merge(gh_sjbasic, how="left").merge(shfs, how="left").merge(brand, how="left")

    goods["cmid"] = cmid
    goods["item_status"] = "正常"
    goods["foreign_category_lv4"] = ""
    goods["foreign_category_lv5"] = ""
    goods["storage_time"] = ""
    goods["last_updated"] = datetime.now(_TZINFO)
    goods["show_code"] = goods.spbm

    goods = goods.rename(columns={
        "spsmm": "barcode",
        "spbm": "foreign_item_id",
        "spmc": "item_name",
        "pjjj": "lastin_price",
        "nsjg": "sale_price",
        "dw": "item_unit",
        "ztbz": "isvalid",
        "bzqts": "warranty",
        "shfsmc": "allot_method",
        "ghsmc": "supplier_name",
        "ghsbh": "supplier_code",
        "ppmc": "brand_name",
        "superbm": "foreign_category_lv1",
        "dlbmid": "foreign_category_lv2",
        "zlbmid": "foreign_category_lv3"
    })

    goods = goods[[
        "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
        "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4", "storage_time",
        "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method", "supplier_name",
        "supplier_code", "brand_name",
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
    lv2 = data_frames["bm_spdlxx"]
    lv1 = data_frames["bm_supertype"]
    lv3 = data_frames["bm_spzlxx"]
    lv1["superbm"] = lv1.superbm.apply(lambda x: x.strip())
    lv2["dlbmid"] = lv2.dlbmid.apply(lambda x: x.strip())
    lv3["zlbmid"] = lv3.zlbmid.apply(lambda x: x.strip())

    category1 = lv1.copy()
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
    category1 = category1.rename(columns={
        "superbm": "foreign_category_lv1",
        "supermc": "foreign_category_lv1_name"
    })
    category1 = category1[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category2 = lv2.merge(lv1, how="left")
    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = None
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = None
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = None
    category2["last_updated"] = datetime.now(_TZINFO)
    category2 = category2.rename(columns={
        "superbm": "foreign_category_lv1",
        "supermc": "foreign_category_lv1_name",
        "dlbmid": "foreign_category_lv2",
        "dlmc": "foreign_category_lv2_name",
    })
    category2 = category2[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category3 = lv3.merge(lv2, how="left").merge(lv1, how="left")
    category3["cmid"] = cmid
    category3["level"] = 3
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = None
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = None
    category3["last_updated"] = datetime.now(_TZINFO)
    category3 = category3.rename(columns={
        "superbm": "foreign_category_lv1",
        "supermc": "foreign_category_lv1_name",
        "dlbmid": "foreign_category_lv2",
        "dlmc": "foreign_category_lv2_name",
        "zlbmid": "foreign_category_lv3",
        "zlmc": "foreign_category_lv3_name",
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
    store_frames = data_frames["app_fdinfo"]
    dq = data_frames["bm_dqxx"]
    store_frames["fdbh"] = store_frames.fdbh.apply(lambda x: x.strip())
    store_frames["dcbhid"] = store_frames.dcbhid.apply(lambda x: x.strip())
    dq["dqbhid"] = dq.dqbhid.apply(lambda x: x.strip())

    store = store_frames.merge(dq, how="left", left_on="dcbhid", right_on="dqbhid")
    store["cmid"] = cmid
    store["address_code"] = ""
    store["device_id"] = ""
    store["store_status"] = None
    store["lat"] = None
    store["lng"] = None
    store["business_area"] = None
    store["source_id"] = source_id
    store["last_updated"] = datetime.now(_TZINFO)
    store["show_code"] = store.fdbh
    store["property"] = store.fdmode
    store = store.rename(columns={
        "fdbh": "foreign_store_id",
        "fdmc": "store_name",
        "dz": "store_address",
        "kdrq": "create_date",
        "telephone": "phone_number",
        "fzr": "contacts",
        "dcbhid": "area_code",
        "dqmc": "area_name",
        "fdmode": "property_id",
    })
    store = store[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    upload_to_s3(store, source_id, date, target_table)
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
    loss = data_frames["dj_bsmx"]
    store = data_frames["app_fdinfo"]
    item = data_frames["uv_spbaseinfo"]
    lv1 = data_frames["bm_supertype"]
    lv2 = data_frames["bm_spdlxx"]
    lv3 = data_frames["bm_spzlxx"]

    loss.fdbh = loss.fdbh.apply(lambda x: x.strip())
    loss.spbm = loss.spbm.apply(lambda x: x.strip())
    item.spbm = item.spbm.apply(lambda x: x.strip())
    item.superbm = item.superbm.apply(lambda x: x.strip())
    item.dlbmid = item.dlbmid.apply(lambda x: x.strip())
    item.zlbmid = item.zlbmid.apply(lambda x: x.strip())
    item.spsmm = item.spsmm.apply(lambda x: x.strip())
    lv1.superbm = lv1.superbm.apply(lambda x: x.strip())
    lv2.dlbmid = lv2.dlbmid.apply(lambda x: x.strip())
    lv3.zlbmid = lv3.zlbmid.apply(lambda x: x.strip())

    goodsloss = loss.merge(store, how="left").merge(item, how="left").merge(lv1, how="left").merge(lv2, how="left")\
        .merge(lv3, how="left")

    goodsloss["cmid"] = cmid
    goodsloss["source_id"] = source_id
    goodsloss["foreign_category_lv4"] = ""
    goodsloss["foreign_category_lv5"] = ""
    goodsloss["store_show_code"] = goodsloss.fdbh
    goodsloss["item_showcode"] = goodsloss.spbm
    goodsloss["quantity"] = goodsloss.bssl.apply(lambda x: -1 * x)
    goodsloss["subtotal"] = goodsloss.apply(lambda row: -1 * row["bssl"] * row["nsjg"], axis=1)
    goodsloss["rq"] = goodsloss.rq.apply(lambda x: x.split()[0])
    goodsloss = goodsloss.rename(columns={
        "bsdbh": "lossnum",
        "rq": "lossdate",
        "fdbh": "foreign_store_id",
        "fdmc": "store_name",
        "spbm": "foreign_item_id",
        "spsmm": "barcode",
        "spmc": "item_name",
        "dw": "item_unit",
        "superbm": "foreign_category_lv1",
        "dlbmid": "foreign_category_lv2",
        "zlbmid": "foreign_category_lv3"
    })
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
