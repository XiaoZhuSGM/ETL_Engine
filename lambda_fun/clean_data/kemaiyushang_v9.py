"""
科脉御商v9清洗逻辑
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


def clean_kemaiyushang(source_id, date, target_table, data_frames):
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

    master = data_frames["pos_t_salemaster"]
    store = data_frames["bi_t_branch_info"]
    sale = data_frames["pos_t_saleflow"]
    item = data_frames["bi_t_item_info"]
    temp = data_frames["bi_t_item_cls"]
    lv4 = temp[(temp.lever_num == 4) & (temp.item_flag == "0")]
    lv3 = temp[(temp.lever_num == 3) & (temp.item_flag == "0")]
    lv2 = temp[(temp.lever_num == 2) & (temp.item_flag == "0")]
    lv1 = temp[(temp.lever_num == 1) & (temp.item_flag == "0")]

    if len(master) == 0:
        frames = pd.DataFrame(columns=columns)
    else:
        frames = (
            master
            .merge(store, how="left", on="branch_id")
            .merge(sale, how="left", on="flow_no")
            .merge(item, how="left", on="item_id", suffixes=("_sale", "_item"))
        )

        frames["lv1_item"] = frames.item_clsno.apply(lambda x: x if pd.isnull(x) else x[:2])
        frames["lv2_item"] = frames.item_clsno.apply(lambda x: x if pd.isnull(x) else x[:4])
        frames["lv3_item"] = frames.item_clsno.apply(lambda x: x if pd.isnull(x) else x[:6])
        frames = (
            frames
            .merge(lv4, how="left", on="item_clsno")
            .merge(lv3, how="left", left_on="lv3_item", right_on="item_clsno", suffixes=("_lv4", "_lv3"))
            .merge(lv2, how="left", left_on="lv2_item", right_on="item_clsno")
            .merge(lv1, how="left", left_on="lv1_item", right_on="item_clsno", suffixes=("_lv2", "_lv1"))
        )
        frames["source_id"] = source_id
        frames["cmid"] = cmid
        frames["consumer_id"] = ""
        frames["last_updated"] = datetime.now(_TZINFO)
        frames["saleprice"] = frames.apply(lambda row: 0 if row["sell_way"] == "C" else row["sale_price"], axis=1)
        frames["quantity"] = frames.apply(
            lambda row:
            row["sale_qty"]
            if row["sell_way"] in ("A", "C")
            else -1 * row["sale_qty"], axis=1
        )

        def generate_subtotal(row):
            if row["sell_way"] == "A":
                return row["sale_money"]
            elif row["sell_way"] == "B":
                return -1 * row["sale_money"]
            elif row["sell_way"] == "C":
                return 0
        frames["subtotal"] = frames.apply(generate_subtotal, axis=1)

        frames["item_clsno_lv4"] = frames["item_clsno_lv4"].fillna("")
        frames["foreign_category_lv1"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 2 else row["item_clsno_lv1"], axis=1)
        )
        frames["foreign_category_lv1_name"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 2 else row["item_clsname_lv1"], axis=1)
        )
        frames["foreign_category_lv2"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 4 else row["item_clsno_lv2"], axis=1)
        )
        frames["foreign_category_lv2_name"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 4 else row["item_clsname_lv2"], axis=1)
        )
        frames["foreign_category_lv3"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 6 else row["item_clsno_lv3"], axis=1)
        )
        frames["foreign_category_lv3_name"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 6 else row["item_clsname_lv3"], axis=1)
        )
        frames["foreign_category_lv4"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 8 else row["item_clsno_lv4"], axis=1)
        )
        frames["foreign_category_lv4_name"] = (
            frames.apply(lambda row: "" if len(row["item_clsno_lv4"]) < 8 else row["item_clsname_lv4"], axis=1)
        )
        frames["foreign_category_lv5"] = ""
        frames["foreign_category_lv5_name"] = None
        frames["pos_id"] = ""
        frames = frames.rename(columns={
            "branch_id": "foreign_store_id",
            "branch_name": "store_name",
            "flow_no": "receipt_id",
            "oper_date": "saletime",
            "item_id": "foreign_item_id",
            "item_subno_sale": "barcode",
            "item_name": "item_name",
            "unit_no": "item_unit",
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

    cost = data_frames["ic_t_jxc_daysum_amt"]
    item = data_frames["bi_t_item_info"]

    if len(cost) == 0:
        frames = pd.DataFrame(columns=columns)
    else:
        frames = (
            cost
            .merge(item, how="left", on="item_id")
        )
        frames = frames[frames["item_id"].notna()]
        frames = frames.groupby(["branch_id", "item_id", "oper_date", "item_clsno"], as_index=False)\
            .agg({"ls_qty": sum, "ls_amt_s": sum, "ls_amt": sum})

        frames["source_id"] = source_id
        frames["cost_type"] = ""
        frames["foreign_category_lv1"] = frames.item_clsno.apply(lambda x: x if len(x) == 2 else x[:2])
        frames["foreign_category_lv2"] = frames.item_clsno.apply(lambda x: "" if len(x) < 4 else x[:4])
        frames["foreign_category_lv3"] = frames.item_clsno.apply(lambda x: "" if len(x) < 6 else x[:6])
        frames["foreign_category_lv4"] = frames.item_clsno.apply(lambda x: "" if len(x) < 8 else x[:8])
        frames["foreign_category_lv5"] = ""
        frames["cmid"] = cmid
        frames = frames.rename(columns={
            "branch_id": "foreign_store_id",
            "item_id": "foreign_item_id",
            "oper_date": "date",
            "ls_qty": "total_quantity",
            "ls_amt_s": "total_sale",
            "ls_amt": "total_cost"
        })
        frames = frames[columns]

    return upload_to_s3(frames, source_id, date, target_table)


def clean_goods(source_id, date, target_table, data_frames):
    """
    清洗商品
    """

    cmid = source_id.split("Y")[0]

    item = data_frames["bi_t_item_info"]
    supcust = data_frames["bi_t_supcust_info"]

    frames = (
        item
        .merge(supcust, how="left", left_on="sup_id", right_on="supcust_id")
    )

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
            return "新品"
        elif x == "5":
            return "预淘汰"
        elif x == "6":
            return "淘汰"
        elif x == "7":
            return "停用"
        elif x == "8":
            return "已删除"

    frames["item_status"] = frames.lifecycle_flag.apply(generate_item_status)
    frames["foreign_category_lv1"] = frames.item_clsno.apply(lambda x: "" if len(x) == 2 else x[:2])
    frames["foreign_category_lv2"] = frames.item_clsno.apply(lambda x: "" if len(x) == 2 else x[:4])
    frames["foreign_category_lv3"] = frames.item_clsno.apply(lambda x: "" if len(x) == 2 else x[:6])
    frames["foreign_category_lv4"] = frames.item_clsno.apply(lambda x: "" if len(x) == 2 else x[:8])
    frames["foreign_category_lv5"] = ""
    frames["storage_time"] = datetime.now(_TZINFO)
    frames["last_updated"] = datetime.now(_TZINFO)
    frames["isvalid"] = 1
    frames["warranty"] = ""
    frames["allot_method"] = ""
    frames["brand_name"] = ""

    frames = frames.rename(columns={
        "item_subno": "barcode",
        "item_id": "foreign_item_id",
        "item_name": "item_name",
        "in_price": "lastin_price",
        "sale_price": "sale_price",
        "unit_no": "item_unit",
        "item_no": "show_code",
        "sup_name": "supplier_name",
        "supcust_no": "supplier_code"
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
    columns = [
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]
    cmid = source_id.split("Y")[0]

    temp = data_frames["bi_t_item_cls"]
    lv4 = temp[(temp.lever_num == 4) & (temp.item_flag == "0")]
    lv3 = temp[(temp.lever_num == 3) & (temp.item_flag == "0")]
    lv2 = temp[(temp.lever_num == 2) & (temp.item_flag == "0")]
    lv1 = temp[(temp.lever_num == 1) & (temp.item_flag == "0")]

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
        "item_clsno": "foreign_category_lv1",
        "item_clsname": "foreign_category_lv1_name"
    })
    category1 = category1[columns]

    category2 = (
        lv2.copy()
        .merge(lv1, how="left", left_on="up_clsno", right_on="item_clsno", suffixes=("_lv2", "_lv1"))
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
    category2 = category2.rename(columns={
        "item_clsno_lv1": "foreign_category_lv1",
        "item_clsname_lv1": "foreign_category_lv1_name",
        "item_clsno_lv2": "foreign_category_lv2",
        "item_clsname_lv2": "foreign_category_lv2_name"
    })
    category2 = category2[columns]

    category3 = (
        lv3.copy()
        .merge(lv2, how="left", left_on="up_clsno", right_on="item_clsno", suffixes=("_lv3", "_lv2"))
        .merge(lv1, how="left", left_on="up_clsno_lv2", right_on="item_clsno")
    )
    category3["cmid"] = cmid
    category3["level"] = 3
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = None
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = None
    category3["last_updated"] = datetime.now(_TZINFO)
    category3 = category3.rename(columns={
        "item_clsno": "foreign_category_lv1",
        "item_clsname": "foreign_category_lv1_name",
        "item_clsno_lv2": "foreign_category_lv2",
        "item_clsname_lv2": "foreign_category_lv2_name",
        "item_clsno_lv3": "foreign_category_lv3",
        "item_clsname_lv3": "foreign_category_lv3_name"
    })
    category3 = category3[columns]

    category4 = (
        lv4.copy()
        .merge(lv3, how="left", left_on="up_clsno", right_on="item_clsno", suffixes=("_lv4", "_lv3"))
        .merge(lv2, how="left", left_on="up_clsno_lv3", right_on="item_clsno")
        .merge(lv1, how="left", left_on="up_clsno", right_on="item_clsno", suffixes=("_lv2", "_lv1"))
    )
    category4["cmid"] = cmid
    category4["level"] = 4
    category4["foreign_category_lv5"] = ""
    category4["foreign_category_lv5_name"] = None
    category4["last_updated"] = datetime.now(_TZINFO)
    category4 = category4.rename(columns={
        "item_clsno_lv1": "foreign_category_lv1",
        "item_clsname_lv1": "foreign_category_lv1_name",
        "item_clsno_lv2": "foreign_category_lv2",
        "item_clsname_lv2": "foreign_category_lv2_name",
        "item_clsno_lv3": "foreign_category_lv3",
        "item_clsname_lv3": "foreign_category_lv3_name",
        "item_clsno_lv4": "foreign_category_lv4",
        "item_clsname_lv4": "foreign_category_lv4_name"
    })
    category4 = category4[columns]

    category = pd.concat([category1, category2, category3, category4])

    return upload_to_s3(category, source_id, date, target_table)


def clean_store(source_id, date, target_table, data_frames):
    """
    门店清洗
    """
    cmid = source_id.split("Y")[0]
    store_frames = data_frames["bi_t_branch_info"]

    store_frames["cmid"] = cmid
    store_frames["address_code"] = None
    store_frames["device_id"] = None
    store_frames["store_status"] = store_frames.display_flag.apply(lambda x: "闭店" if x == "0" else "正常")
    store_frames["create_date"] = datetime.now(_TZINFO)
    store_frames["lat"] = None
    store_frames["lng"] = None
    store_frames["area_code"] = ""
    store_frames["area_name"] = ""
    store_frames["business_area"] = None

    def generate_property(row):
        if row == "0":
            return "总部"
        elif row == "1":
            return "配套中心"
        elif row == "2":
            return "直营店"
        elif row == "8":
            return "仓库"
    store_frames["property_id"] = store_frames["property"]
    store_frames["property"] = store_frames.property.apply(generate_property)
    store_frames["source_id"] = source_id
    store_frames["last_updated"] = datetime.now(_TZINFO)

    frames = store_frames.rename(columns={
        "branch_id": "foreign_store_id",
        "branch_name": "store_name",
        "address": "store_address",
        "branch_no": "show_code",
        "branch_tel": "phone_number",
        "branch_man": "contacts"
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
