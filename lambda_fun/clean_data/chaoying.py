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
        frames["foreign_category_lv3"] = ""
        frames["foreign_category_lv3_name"] = None
        frames["foreign_category_lv4"] = ""
        frames["foreign_category_lv4_name"] = None
        frames["foreign_category_lv5"] = ""
        frames["foreign_category_lv5_name"] = None
        frames["pos_id"] = ""

        frames = frames.rename(columns={
            "bm_store": "foreign_store_id",
            "mc_store": "store_name",
            "dh": "receipt_id",
            "id_hyk": "consumer_id",
            "rq": "saletime",
            "bm_item": "foreign_item_id",
            "barcode": "barcode",
            "mc_item": "item_name",
            "jldw": "item_unit",
            "bm_lv1": "foreign_category_lv1",
            "mc_lv1": "foreign_category_lv1_name",
            "bm_lv2": "foreign_category_lv2",
            "mc_lv2": "foreign_category_lv2_name"
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
        frames["foreign_category_lv3"] = ""
        frames["foreign_category_lv4"] = ""
        frames["foreign_category_lv5"] = ""
        frames["cmid"] = cmid
        frames = frames.rename(columns={
            "store.bm": "foreign_store_id",
            "item.bm": "foreign_item_id",
            "cost.sl_ls": "total_quantity",
            "cost.je_hs_ls": "total_sale",
            "cost.je_cb_hs_ls": "total_cost",
            "lv1.bm": "foreign_category_lv1",
            "lv2.bm": "foreign_category_lv2"
        })
        frames = frames[columns]

    return upload_to_s3(frames, source_id, date, target_table)


def clean_goods(source_id, date, target_table, data_frames):
    """
    清洗商品
    """
    pass


def clean_category(source_id, date, target_table, data_frames):
    """
    分类清洗
    """
    pass


def clean_store(source_id, date, target_table, data_frames):
    """
    门店清洗
    """
    pass


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
