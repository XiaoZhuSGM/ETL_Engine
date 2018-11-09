"""
超赢清洗逻辑
销售，成本，库存，商品， 分类
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


def clean_chaoying(source_id, date, target_table, data_frames):
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
        'last_updated', 'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
        'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
        'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
        'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'
    ]
    cmid = source_id.split("Y")[0]

    header = data_frames["td_ls_1c"]
    detail = data_frames["td_ls_2c"]
    store = data_frames["tb_gsjg"]
    item = data_frames["tb_sp"]
    lv2 = data_frames["tb_spfl"]
    lv1 = data_frames["tb_spfl"]

    if len(header) == 0:
        frames = pd.DataFrame(columns=columns)
    else:
        frames = (
            header.merge(detail, how="left", on="dh")
            .merge(store, how="left", left_on="id_gsjg", right_on="id")
            .merge(item, how="left", left_on="id_sp", right_on="id", suffixes=("_store", "_item"))
            .merge(lv2, how="left", left_on="id_spfl", right_on="id")
            .merge(lv1, how="left", left_on="id_1", right_on="id", suffixes=("_lv2", "_lv1"))
        )

        frames["source_id"] = source_id
        frames["cmid"] = cmid
        frames["last_updated"] = datetime.now(_TZINFO)
        frames["saleprice"] = frames.apply(lambda row: row["dj_hs"] / row["zhl"], axis=1)
        frames["quantity"] = frames.apply(lambda row: row["sl"] * row["zhl"], axis=1)
        frames["subtotal"] = frames.apply(lambda row: row["je_hs"] - row["je_yh"] - row["je_zr"] - row["je_zk"], axis=1)

        frames["foreign_category_lv1"] = frames["bm_lv1"].apply(lambda x: x if not pd.isnull(x) else "0")
        frames["foreign_category_lv1_name"] = frames["mc_lv1"].apply(lambda x: x if not pd.isnull(x) else "未定义")

        frames["foreign_category_lv3"] = ""
        frames["foreign_category_lv3_name"] = None
        frames["foreign_category_lv4"] = ""
        frames["foreign_category_lv4_name"] = None
        frames["foreign_category_lv5"] = ""
        frames["foreign_category_lv5_name"] = None

        frames = frames.rename(columns={
            "id_store": "foreign_store_id",
            "mc_store": "store_name",
            "dh": "receipt_id",
            "id_hyk": "consumer_id",
            "rq": "saletime",
            "id_item": "foreign_item_id",
            "barcode": "barcode",
            "mc_item": "item_name",
            "jldw": "item_unit",
            "bm_lv2": "foreign_category_lv2",
            "mc_lv2": "foreign_category_lv2_name",
            "id_pos": "pos_id"
        })

        frames = frames[columns]

    return upload_to_s3(frames, source_id, date, target_table)


def clean_cost(source_id, date, target_table, data_frames):
    """
    清洗成本
    """
    columns = [
        "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
        "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3",
        "foreign_category_lv4", "foreign_category_lv5", "cmid"
    ]
    cmid = source_id.split("Y")[0]

    cost = data_frames["v_tz_rj_sp_gys_jxc"].rename(columns=lambda x: f"cost.{x}")
    store = data_frames["tb_gsjg"].rename(columns=lambda x: f"store.{x}")
    item = data_frames["tb_sp"].rename(columns=lambda x: f"item.{x}")
    lv2 = data_frames["tb_spfl"].rename(columns=lambda x: f"lv2.{x}")
    lv1 = data_frames["tb_spfl"].rename(columns=lambda x: f"lv1.{x}")

    if len(cost) == 0:
        frames = pd.DataFrame(columns=columns)
    else:
        frames = (
            cost
            .merge(store, how="left", left_on="cost.id_gsjg", right_on="store.id")
            .merge(item, how="left", left_on="cost.id_sp", right_on="item.id")
            .merge(lv2, how="left", left_on="item.id_spfl", right_on="lv2.id")
            .merge(lv1, how="left", left_on="lv2.id_1", right_on="lv1.id")
        )

        frames["source_id"] = source_id
        frames["date"] = frames["cost.ymd"].apply(lambda x: datetime.strptime(x, "%Y%m%d").strftime("%Y-%m-%d"))
        frames["cost_type"] = ""
        frames["foreign_category_lv1"] = frames["lv1.bm"].apply(lambda x: x if not pd.isnull(x) else "0")
        frames["foreign_category_lv3"] = ""
        frames["foreign_category_lv4"] = ""
        frames["foreign_category_lv5"] = ""
        frames["cmid"] = cmid
        frames = frames.rename(columns={
            "store.id": "foreign_store_id",
            "item.id": "foreign_item_id",
            "cost.sl_ls": "total_quantity",
            "cost.je_hs_ls": "total_sale",
            "cost.je_cb_hs_ls": "total_cost",
            "lv2.bm": "foreign_category_lv2"
        })
        frames = frames[columns]

    return upload_to_s3(frames, source_id, date, target_table)


def clean_goods(source_id, date, target_table, data_frames):
    """
    清洗商品
    """

    cmid = source_id.split("Y")[0]

    item = data_frames["tb_sp"].rename(columns=lambda x: f"item.{x}")
    state = data_frames["tb_sp_state"]
    state = state[state["id_gsjg"] == 1].rename(columns=lambda x: f"attr.{x}")
    price = data_frames["tb_sp_dj"]
    price = price[(price["id_gsjg"] == 1) & (price["dw_bs"] == 1)].rename(columns=lambda x: f"price.{x}")
    supper = data_frames["tb_gys"].rename(columns=lambda x: f"supper.{x}")
    brand = data_frames["tb_pp"].rename(columns=lambda x: f"brand.{x}")
    lv1 = data_frames["tb_spfl"].rename(columns=lambda x: f"lv1.{x}")
    lv2 = data_frames["tb_spfl"].rename(columns=lambda x: f"lv2.{x}")
    category1 = lv1.copy()
    category1 = category1[category1["lv1.js"] == '1']

    category2 = lv2.copy()
    category2 = category2[category2["lv2.js"] == '2']

    frames = item.merge(category2, how="left", left_on="item.id_spfl", right_on="lv2.id") \
        .merge(category1, how="left", left_on="lv2.id_1", right_on="lv1.id") \
        .merge(state, how="left", left_on="item.id", right_on="attr.id_sp") \
        .merge(price, how="left", left_on="item.id", right_on="price.id_sp") \
        .merge(supper, how="left", left_on="attr.id_gys", right_on="supper.id") \
        .merge(brand, how="left", left_on="item.id_pp", right_on="brand.id")

    frames["cmid"] = cmid

    def generate_item_status(x):
        if x == "0":
            return "未审核"
        elif x == "1":
            return "建档"
        elif x == "2":
            return "新品"
        elif x == "3":
            return "正常"
        elif x == "4":
            return "暂停"
        elif x == "5":
            return "预淘汰"
        elif x == "6":
            return "淘汰"
        elif x == "7":
            return "停用"
        elif x == "8":
            return "删除"
        else:
            return "其他"

    def generate_allot_method(x):
        if x == "1":
            return "直送"
        elif x == "2":
            return "配送"

    frames["item_status"] = frames["attr.flag_state"].apply(generate_item_status)
    frames["foreign_category_lv1"] = frames["lv1.bm"].apply(lambda x: x if not pd.isnull(x) else 0)
    frames["foreign_category_lv2"] = frames["lv2.bm"].apply(lambda x: x if not pd.isnull(x) else 0)
    frames["foreign_category_lv3"] = ""
    frames["foreign_category_lv4"] = ""
    frames["foreign_category_lv5"] = ""
    frames["last_updated"] = datetime.now(_TZINFO)
    frames["isvalid"] = 1
    frames["allot_method"] = frames["attr.flag_sffs"].apply(generate_allot_method)
    frames["storage_time"] = frames["attr.rq_create"]
    frames = frames.rename(columns={
        "item.barcode": "barcode",
        "item.id": "foreign_item_id",
        "item.mc": "item_name",
        "price.dj_jh": "lastin_price",
        "price.dj_ls": "sale_price",
        "item.jldw": "item_unit",
        "item.yxq": "warranty",
        "item.bm": "show_code",
        "supper.bm": "supplier_name",
        "supper.mc": "supplier_code",
        "brand.mc": "brand_name"
    })

    frames = frames[[
        "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
        "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4", "storage_time",
        "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method", "supplier_name",
        "supplier_code", "brand_name"
    ]]

    return upload_to_s3(frames, source_id, date, target_table)


def clean_category(source_id, date, target_table, data_frames):
    """
    分类清洗
    """
    cmid = source_id.split("Y")[0]

    lv1 = data_frames["tb_spfl"].rename(columns=lambda x: f"lv1.{x}")
    lv2 = data_frames["tb_spfl"].rename(columns=lambda x: f"lv2.{x}")

    category1 = lv1.copy()
    category1 = category1[category1["lv1.js"] == '1']
    category1["cmid"] = cmid
    category1["level"] = 1
    category1["foreign_category_lv1"] = category1["lv1.bm"].apply(lambda x: x if not pd.isnull(x) else 0)
    category1["foreign_category_lv1_name"] = category1["lv1.mc"].apply(lambda x: x if not pd.isnull(x) else "未定义")
    category1["foreign_category_lv2"] = ""
    category1["foreign_category_lv2_name"] = ""
    category1["foreign_category_lv3"] = ""
    category1["foreign_category_lv3_name"] = ""
    category1["foreign_category_lv4"] = ""
    category1["foreign_category_lv4_name"] = ""
    category1["foreign_category_lv5"] = ""
    category1["foreign_category_lv5_name"] = ""
    category1["last_updated"] = datetime.now(_TZINFO)
    category1 = category1[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category2 = lv2.copy()
    category2 = category2[category2["lv2.js"] == '2']
    category2 = category2.merge(lv1.copy(), how="left", left_on="lv2.id_1", right_on="lv1.id")

    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv1"] = category2["lv1.bm"].apply(lambda x: x if not pd.isnull(x) else 0)
    category2["foreign_category_lv1_name"] = category2["lv1.mc"].apply(lambda x: x if not pd.isnull(x) else "未定义")
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = ""
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = ""
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = ""
    category2["last_updated"] = datetime.now(_TZINFO)
    category2 = category2.rename(columns={
        "lv2.bm": "foreign_category_lv2",
        "lv2.mc": "foreign_category_lv2_name",
    })
    category2 = category2[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category = pd.concat([category1, category2])

    return upload_to_s3(category, source_id, date, target_table)


def clean_store(source_id, date, target_table, data_frames):
    """
    门店清洗
    """
    cmid = source_id.split("Y")[0]
    store_frames = data_frames["tb_gsjg"]

    def generate_stor_status(x):
        if x == '1':
            res = "正常"
        else:
            res = "闭店"
        return res

    def generate_stor_property(x):
        if x == '0':
            res = '总部'
        elif x == '1':
            res = '配送中心'
        elif x == '2':
            res = '分公司（区域中心'
        elif x == '3':
            res = '直营独立分店'
        elif x == '4':
            res = '直营托管分店'
        elif x == '5':
            res = '加盟店'
        return res

    store_frames["cmid"] = cmid
    store_frames["source_id"] = source_id
    store_frames["address_code"] = None
    store_frames["device_id"] = None
    store_frames["store_status"] = store_frames.flag_state.apply(generate_stor_status)
    store_frames["last_updated"] = datetime.now(_TZINFO)
    store_frames["lat"] = None
    store_frames["lng"] = None
    store_frames["business_area"] = None
    store_frames["area_code"] = None
    store_frames["area_name"] = None
    store_frames["property"] = store_frames.flag_lx.apply(generate_stor_property)

    frames = store_frames.rename(columns={
        "id": "foreign_store_id",
        "mc": "store_name",
        "dz": "store_address",
        "rq_active": "create_date",
        "bm": "show_code",
        "tel": "phone_number",
        "lxr": "contacts",
        "flag_lx": "property_id"
    })
    frames = frames[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    return upload_to_s3(frames, source_id, date, target_table)


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
