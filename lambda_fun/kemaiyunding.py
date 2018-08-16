"""
科脉云鼎清洗逻辑
销售，成本，库存，商品， 分类等
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


def clean_kemaiyunding(source_id, date, target_table, data_frames):
    if target_table == "goodsflow":
        clen_goodsflow(source_id, date, target_table, data_frames)
    elif target_table == "cost":
        clen_cost(source_id, date, target_table, data_frames)


def clen_goodsflow(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    data_frame1 = frame1(cmid, source_id, data_frames)
    data_frame2 = frame2(cmid, source_id, data_frames)
    data_frame3 = frame3(cmid, source_id, data_frames)
    goodsflow = pd.concat([data_frame1, data_frame2, data_frame3])

    upload_to_s3(goodsflow, source_id, date, target_table)

    return goodsflow


def clen_cost(source_id, date, target_table, data_frames):
    pass


def upload_to_s3(frame, source_id, date, target_table):
    filename = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
    count = len(frame)
    frame.to_csv(filename.name, index=False, compression="gzip")
    filename.seek(0)
    key = CLEANED_PATH.format(
        source_id=source_id,
        target_table=target_table,
        date=date,
        timestamp=now_timestamp(),
        rowcount=count,
    )
    # S3.Object(bucket_name=S3_BUCKET, key=key).put(Body=filename)
    S3.Bucket(S3_BUCKET).upload_file(filename.name, key)
    pass


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp


def frame1(cmid, source_id, frames):
    temp1 = frames["t_sl_master"].merge(frames["t_sl_detail"], how="left", on="fflow_no")

    def gene_quantity_or_sbutotal(x, y):
        if x == 2:
            return -1 * y
        return y

    temp1["quantity"] = temp1.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["fpack_qty"]), axis=1)
    temp1["subtotal"] = temp1.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["famt"]), axis=1)

    temp1 = temp1.merge(frames["t_br_master"], how="left", on="fbrh_no").merge(frames["t_bi_master"], how="inner",
                                                                               on=["fitem_id", "fitem_subno"],
                                                                               suffixes=('_x', '')).merge(
        frames["t_bc_master"], how="left", on="fitem_clsno")

    temp1 = temp1.merge(frames["t_bc_master"], how="left", left_on="fprt_no", right_on="fitem_clsno",
                        suffixes=('_lv3', '_lv2')).merge(frames["t_bc_master"], how="left", left_on="fprt_no_lv2",
                                                         right_on="fitem_clsno")

    temp1 = temp1.rename(columns={"fbrh_no": "foreign_store_id", "fbrh_name": "store_name", "fflow_no": "receipt_id",
                                  "fitem_id": "foreign_item_id", "fitem_subno": "barcode", "fitem_name": "item_name",
                                  "funit_no": "item_unit", "fprice": "saleprice", "fitem_clsno": "foreign_category_lv1",
                                  "fitem_clsname": "foreign_category_lv1_name",
                                  "fitem_clsno_lv2": "foreign_category_lv2",
                                  "fitem_clsname_lv2": "foreign_category_lv2_name",
                                  "fitem_clsno_lv3": "foreign_category_lv3",
                                  "fitem_clsname_lv3": "foreign_category_lv3_name"})

    temp1.insert(0, 'cmid', cmid)
    temp1.insert(0, 'source_id', source_id)
    temp1["consumer_id"] = ''
    temp1["saletime"] = temp1.pop("ftrade_date") + " " + temp1.pop("fcr_time")
    temp1["last_updated"] = datetime.now()
    temp1["foreign_category_lv4"] = ""
    temp1["foreign_category_lv4_name"] = None
    temp1["foreign_category_lv5"] = ""
    temp1["foreign_category_lv5_name"] = None
    temp1["pos_id"] = ""
    # del temp1["fsell_way"]
    # del temp1["fpack_qty"]
    # del temp1["famt"]
    # del temp1["fprt_no_lv3"]
    # del temp1["fprt_no_lv2"]
    # del temp1["fprt_no"]

    temp1 = temp1[
        ["source_id", "cmid", "foreign_store_id", "store_name", "receipt_id", "consumer_id", "saletime", "last_updated",
         "foreign_item_id", "barcode", "item_name", "item_unit", "saleprice", "quantity", "subtotal",
         "foreign_category_lv1", "foreign_category_lv1_name", "foreign_category_lv2", "foreign_category_lv2_name",
         "foreign_category_lv3", "foreign_category_lv3_name", "foreign_category_lv4", "foreign_category_lv4_name",
         "foreign_category_lv5", "foreign_category_lv5_name", "pos_id"]]

    return temp1


def frame2(cmid, source_id, frames):
    temp2 = frames["t_sl_master"].merge(frames["t_sl_detail"], how="left", on="fflow_no") \
        .merge(frames["t_br_master"], how="left", on="fbrh_no") \
        .merge(frames["t_bi_master"], how="inner", on="fitem_id", suffixes=('', '_y')) \
        .merge(frames["t_bi_barcode"], how="inner", on=["fitem_id", "fitem_subno"])

    def gene_quantity(x, y, z):
        if x == 2:
            return -1 * y * z
        return y * z

    def gene_sbutotal(x, y):
        if x == 2:
            return -1 * y
        return y

    temp2["quantity"] = temp2.apply(
        lambda row: gene_quantity(row["fsell_way"], row["fpack_qty"], row["funit_qty"]), axis=1)

    temp2["subtotal"] = temp2.apply(lambda row: gene_sbutotal(row["fsell_way"], row["famt"]), axis=1)

    temp2 = temp2.merge(frames["t_bc_master"], how="left", on="fitem_clsno")

    temp2 = temp2.merge(frames["t_bc_master"], how="left", left_on="fprt_no", right_on="fitem_clsno",
                        suffixes=('_lv3', '_lv2')).merge(frames["t_bc_master"], how="left", left_on="fprt_no_lv2",
                                                         right_on="fitem_clsno")

    temp2 = temp2.rename(columns={"fbrh_no": "foreign_store_id", "fbrh_name": "store_name", "fflow_no": "receipt_id",
                                  "fitem_id": "foreign_item_id", "fitem_subno": "barcode", "fitem_name": "item_name",
                                  "funit_no": "item_unit", "fprice": "saleprice", "fitem_clsno": "foreign_category_lv1",
                                  "fitem_clsname": "foreign_category_lv1_name",
                                  "fitem_clsno_lv2": "foreign_category_lv2",
                                  "fitem_clsname_lv2": "foreign_category_lv2_name",
                                  "fitem_clsno_lv3": "foreign_category_lv3",
                                  "fitem_clsname_lv3": "foreign_category_lv3_name"})

    temp2.insert(0, 'cmid', cmid)
    temp2.insert(0, 'source_id', source_id)
    temp2["consumer_id"] = ''
    temp2["saletime"] = temp2.pop("ftrade_date") + " " + temp2.pop("fcr_time")
    temp2["last_updated"] = datetime.now()
    temp2["foreign_category_lv4"] = ""
    temp2["foreign_category_lv4_name"] = None
    temp2["foreign_category_lv5"] = ""
    temp2["foreign_category_lv5_name"] = None
    temp2["pos_id"] = ""
    # del temp2["fsell_way"]
    # del temp2["fpack_qty"]
    # del temp2["famt"]
    # del temp2["fprt_no_lv3"]
    # del temp2["fprt_no_lv2"]
    # del temp2["fprt_no"]

    temp2 = temp2[
        ["source_id", "cmid", "foreign_store_id", "store_name", "receipt_id", "consumer_id", "saletime", "last_updated",
         "foreign_item_id", "barcode", "item_name", "item_unit", "saleprice", "quantity", "subtotal",
         "foreign_category_lv1", "foreign_category_lv1_name", "foreign_category_lv2", "foreign_category_lv2_name",
         "foreign_category_lv3", "foreign_category_lv3_name", "foreign_category_lv4", "foreign_category_lv4_name",
         "foreign_category_lv5", "foreign_category_lv5_name", "pos_id"]]

    return temp2


def frame3(cmid, source_id, frames):
    temp3 = frames["t_sl_master"].merge(frames["t_sl_detail"], how="left", on="fflow_no") \
        .merge(frames["t_br_master"], how="left", on="fbrh_no").query("fitem_id == 0")

    def gene_quantity_or_sbutotal(x, y):
        if x == 2:
            return -1 * y
        return y

    temp3["quantity"] = temp3.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["fpack_qty"]), axis=1)
    temp3["subtotal"] = temp3.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["famt"]), axis=1)

    temp3 = temp3.rename(columns={"fbrh_no": "foreign_store_id", "fbrh_name": "store_name", "fflow_no": "receipt_id",
                                  "fitem_id": "foreign_item_id", "fitem_subno": "barcode", "fitem_name": "item_name",
                                  "fprice": "saleprice"})

    temp3.insert(0, 'cmid', cmid)
    temp3.insert(0, 'source_id', source_id)
    temp3["consumer_id"] = ''
    temp3["saletime"] = temp3.pop("ftrade_date") + " " + temp3.pop("fcr_time")
    temp3["last_updated"] = datetime.now()
    temp3["foreign_category_lv1"] = ""
    temp3["foreign_category_lv1_name"] = ""
    temp3["foreign_category_lv2"] = ""
    temp3["foreign_category_lv2_name"] = ""
    temp3["foreign_category_lv3"] = ""
    temp3["foreign_category_lv3_name"] = ""
    temp3["foreign_category_lv4"] = ""
    temp3["foreign_category_lv4_name"] = None
    temp3["foreign_category_lv5"] = ""
    temp3["foreign_category_lv5_name"] = None
    temp3["barcode"] = "0000"
    temp3["item_name"] = "万能商品"
    temp3["item_unit"] = "个"
    temp3["pos_id"] = ""
    # del temp3["fsell_way"]
    # del temp3["fpack_qty"]
    # del temp3["famt"]

    temp3 = temp3[
        ["source_id", "cmid", "foreign_store_id", "store_name", "receipt_id", "consumer_id", "saletime", "last_updated",
         "foreign_item_id", "barcode", "item_name", "item_unit", "saleprice", "quantity", "subtotal",
         "foreign_category_lv1", "foreign_category_lv1_name", "foreign_category_lv2", "foreign_category_lv2_name",
         "foreign_category_lv3", "foreign_category_lv3_name", "foreign_category_lv4", "foreign_category_lv4_name",
         "foreign_category_lv5", "foreign_category_lv5_name", "pos_id"]]

    return temp3
