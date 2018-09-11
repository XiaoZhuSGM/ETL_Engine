"""
科脉云鼎清洗逻辑
销售，成本，库存，商品， 分类等

# 销售流水
origin_table_columns = {"t_sl_master": ['fbrh_no', 'fflow_no', 'ftrade_date', 'fcr_time', 'fsell_way'],
                        "t_sl_detail": ['fprice', 'fpack_qty', 'famt', 'fflow_no', 'fitem_subno', 'fitem_id'],
                        "t_br_master": ['fbrh_name', 'fbrh_no'],
                        "t_bi_master": ['fitem_id', 'fitem_subno', 'fitem_name', 'funit_no', 'fitem_clsno'],
                        "t_bc_master": ['fitem_clsno', 'fitem_clsname', 'fprt_no'],
                        "t_bi_barcode": ['funit_qty', 'fitem_id', 'fitem_subno']}

coverts = {"t_sl_master": {"fbrh_no": str}, "t_br_master": {"fbrh_no": str},
           "t_bi_master": {"fitem_clsno": str},
           "t_bc_master": {"fitem_clsno": str, "fprt_no": str}}

# 成本
origin_table_columns = {"t_rpt_sl_detail": ['fitem_id', 'fbrh_no', 'ftrade_date', 'fqty', 'famt', 'fcost_amt'],
                        "t_bi_master": ['fitem_clsno', 'fitem_id']
                        }


coverts = {"t_rpt_sl_detail": {"fbrh_no": str, "fitem_id": "str"},
           "t_bi_master": {"fitem_clsno": str, "fitem_id": "str"}}


# goods
origin_table_columns = {
    "t_bi_master": ['fitem_subno', 'fitem_id', 'fitem_name', 'fstatus', 'fitem_clsno', 'fap_date', 'fexp_date',
                    'fitem_no', 'fitem_brdno', 'funit_no'],
    "t_bs_master": ['fsup_name', 'fsup_no', 'fsale_way'],
    "t_bi_price": ['fin_price', 'fsale_price', 'fitem_id', 'fsup_no'],
    "t_bb_master": ['fitem_brdname', 'fitem_brdno']
}

coverts = {"t_bi_master": {"fitem_id": str, "fitem_clsno": str, "fitem_subno": str},
           "t_bs_master": {"fsup_no": str, "fsale_way": str},
           "t_bi_price": {"fitem_id": str, 'fsup_no': str}}

#category
origin_table_columns = {"t_bc_master": ['fitem_clsno', 'fitem_clsname', 'flvl_num', 'fprt_no']
                        }

coverts = {"t_bc_master": {"fitem_clsno": str, "fprt_no": str}}

store
origin_table_columns = {"t_br_master": ['fbrh_no', 'fbrh_name', 'fstatus', 'fcr_date', 'fbrh_type'],
                        "t_br_ext": ['fbrh_no', 'faddr', 'ftel', 'fman']
                        }

coverts = {"t_br_master": {"fbrh_no": str}, "t_br_ext": {"fbrh_no": str}}

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
        return clean_goodsflow(source_id, date, target_table, data_frames)
    elif target_table == "cost":
        return clean_cost(source_id, date, target_table, data_frames)
    elif target_table == "store":
        return clean_store(source_id, date, target_table, data_frames)
    elif target_table == "goods":
        return clean_goods(source_id, date, target_table, data_frames)
    elif target_table == "sales_target":
        return clean_sales_target(source_id, date, target_table, data_frames)
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
    data_frame1 = frame1(cmid, source_id, data_frames)
    data_frame2 = frame2(cmid, source_id, data_frames)
    data_frame3 = frame3(cmid, source_id, data_frames)
    goodsflow = pd.concat([data_frame1, data_frame2, data_frame3])

    return upload_to_s3(goodsflow, source_id, date, target_table)


def clean_cost(source_id, date, target_table, frames):
    """
    清洗成本
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    cost_frame = frames["t_rpt_sl_detail"].merge(frames["t_bi_master"], how="left", on="fitem_id")
    cost_frame["source_id"] = source_id
    cost_frame["costtype"] = ''
    cost_frame["foreign_category_lv4"] = ''
    cost_frame["foreign_category_lv5"] = ''
    cost_frame["cmid"] = cmid
    cost_frame["foreign_category_lv1"] = cost_frame.fitem_clsno.apply(lambda x: str(x)[:2] if x is not None else '')
    cost_frame["foreign_category_lv2"] = cost_frame.fitem_clsno.apply(lambda x: str(x)[:4] if x is not None else '')
    cost_frame["foreign_category_lv3"] = cost_frame.fitem_clsno.apply(lambda x: str(x) if x is not None else '')
    cost_frame = cost_frame.rename(
        columns={"fbrh_no": "foreign_store_id", "fitem_id": "foreign_item_id", "ftrade_date": "date",
                 "fqty": "total_quantity", "famt": "total_sale", "fcost_amt": "total_cost"})
    cost_frame = cost_frame[
        ["source_id", "foreign_store_id", "foreign_item_id", "date", "costtype", "total_quantity", "total_sale",
         "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
         "foreign_category_lv5", 'cmid']]

    return upload_to_s3(cost_frame, source_id, date, target_table)


def clean_goods(source_id, date, target_table, frames):
    """
    清洗商品
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]

    goods_frame = frames["t_bi_master"].merge(frames["t_bi_price"], how="left", on="fitem_id") \
        .merge(frames["t_bs_master"], how="left", on="fsup_no") \
        .merge(frames["t_bb_master"], how="left", on="fitem_brdno")

    def set_status(x):
        if x == '6':
            y = "正常"
        elif x == '5':
            y = '新品'
        elif x == '7':
            y = '停购'
        elif x == 'B':
            y = '停配'
        elif x == '9':
            y = '淘汰'
        elif x == '6':
            y = '正常'
        else:
            y = '其他'
        return y

    goods_frame['item_status'] = goods_frame.fstatus.apply(lambda x: set_status(x))
    goods_frame['foreign_category_lv1'] = goods_frame.fitem_clsno.apply(lambda x: str(x)[:2])
    goods_frame['foreign_category_lv2'] = goods_frame.fitem_clsno.apply(lambda x: str(x)[:4])
    goods_frame['foreign_category_lv4'] = ''
    goods_frame['foreign_category_lv5'] = ''
    goods_frame["last_updated"] = datetime.now(_TZINFO)
    goods_frame["isvalid"] = 1
    goods_frame["cmid"] = cmid

    def allot_method(x):
        if x == '1':
            y = '统配'
        elif x == '2':
            y = '中转'
        elif x == '3':
            y = "自采"
        else:
            y = ''
        return y

    goods_frame["allot_method"] = goods_frame.fsale_way.apply(lambda x: allot_method(x))

    goods_frame = goods_frame.rename(
        columns={"fitem_subno": "barcode", "fitem_id": "foreign_item_id", "fitem_name": "item_name",
                 "fin_price": "lastin_price", "fsale_price": "sale_price", "fitem_brdname": "brand_name",
                 "funit_no": "item_unit", "fitem_clsno": "foreign_category_lv3", "fap_date": "storage_time",
                 "fexp_date": "warranty", "fitem_no": "show_code", "fsup_name": "supplier_name",
                 "fsup_no": "supplier_code"})

    goods_frame = goods_frame[
        ["cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit",
         "item_status", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
         "storage_time", "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method",
         "supplier_name", "supplier_code", "brand_name"]]

    return upload_to_s3(goods_frame, source_id, date, target_table)


def clean_sales_target(source_id, date, target_table, frames):
    """
    清洗销售目标
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    target_frame = frames["t_sv_sale_manage"].merge(frames["t_br_master"], how="left", on="fbrh_no")
    target_frame["target_date"] = datetime.now(_TZINFO).strftime("%Y-%m-01")
    target_frame["last_updated"] = datetime.now(_TZINFO)
    target_frame["category_level"] = 1
    target_frame['foreign_category_lv1'] = ''
    target_frame['foreign_category_lv2'] = ''
    target_frame['foreign_category_lv3'] = ''
    target_frame['foreign_category_lv4'] = ''
    target_frame['foreign_category_lv5'] = ''
    target_frame["cmid"] = cmid
    target_frame["source_id"] = source_id

    target_frame = target_frame.rename(
        columns={"fbrh_no": "foreign_store_id", "fbrh_name": "store_name", "fsale_amt": "target_sales",
                 "fprofit_amt": "target_gross_profit"})
    target_frame["store_show_code"] = target_frame["foreign_store_id"]

    target_frame = target_frame[
        ["source_id", "cmid", "target_date", "foreign_store_id", "store_show_code", "store_name", "target_sales",
         "target_gross_profit", "category_level", "foreign_category_lv1", "foreign_category_lv2",
         "foreign_category_lv3", "foreign_category_lv4", "foreign_category_lv5", "last_updated"]]

    return upload_to_s3(target_frame, source_id, date, target_table)


def clean_category(source_id, date, target_table, frames):
    """
    分类清洗
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    category1 = frames["t_bc_master"].query('flvl_num == 1')[:]
    category1["cmid"] = cmid
    category1["level"] = 1
    category1['foreign_category_lv2'] = ''
    category1['foreign_category_lv2_name'] = ''
    category1['foreign_category_lv3'] = ''
    category1['foreign_category_lv3_name'] = ''
    category1['foreign_category_lv4'] = ''
    category1['foreign_category_lv4_name'] = ''
    category1['foreign_category_lv5'] = ''
    category1['foreign_category_lv5_name'] = ''
    category1["last_updated"] = datetime.now(_TZINFO)
    category1 = category1.rename(
        columns={"fitem_clsno": "foreign_category_lv1", "fitem_clsname": "foreign_category_lv1_name"})
    category1 = category1[['cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
                           'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
                           'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
                           'foreign_category_lv5_name']]

    category2 = frames["t_bc_master"].merge(frames["t_bc_master"], how="left", left_on="fprt_no",
                                            right_on="fitem_clsno", suffixes=('lv2', 'lv1'))
    category2 = category2.query('flvl_numlv2 == 2')[:]
    category2["level"] = 2
    category2["cmid"] = cmid
    category2['foreign_category_lv3'] = ''
    category2['foreign_category_lv3_name'] = ''
    category2['foreign_category_lv4'] = ''
    category2['foreign_category_lv4_name'] = ''
    category2['foreign_category_lv5'] = ''
    category2['foreign_category_lv5_name'] = ''
    category2["last_updated"] = datetime.now(_TZINFO)

    category2 = category2.rename(
        columns={"fitem_clsnolv1": "foreign_category_lv1", "fitem_clsnamelv1": "foreign_category_lv1_name",
                 "fitem_clsnolv2": "foreign_category_lv2", "fitem_clsnamelv2": "foreign_category_lv2_name"})

    category2 = category2[['cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
                           'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
                           'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
                           'foreign_category_lv5_name']]

    category3 = frames["t_bc_master"].merge(frames["t_bc_master"], how="left", left_on="fprt_no",
                                            right_on="fitem_clsno", suffixes=('lv3', 'lv2')).merge(
        frames["t_bc_master"], how="left", left_on="fprt_nolv2", right_on="fitem_clsno")

    category3 = category3.query('flvl_numlv3 == 3')[:]
    category3["level"] = 3
    category3["cmid"] = cmid
    category3['foreign_category_lv4'] = ''
    category3['foreign_category_lv4_name'] = ''
    category3['foreign_category_lv5'] = ''
    category3['foreign_category_lv5_name'] = ''
    category3["last_updated"] = datetime.now(_TZINFO)

    category3 = category3.rename(
        columns={"fitem_clsno": "foreign_category_lv1", "fitem_clsname": "foreign_category_lv1_name",
                 "fitem_clsnolv2": "foreign_category_lv2", "fitem_clsnamelv2": "foreign_category_lv2_name",
                 "fitem_clsnolv3": "foreign_category_lv3", "fitem_clsnamelv3": "foreign_category_lv3_name"})

    category3 = category3[['cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
                           'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
                           'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
                           'foreign_category_lv5_name']]

    category = pd.concat([category1, category2, category3])

    return upload_to_s3(category, source_id, date, target_table)


def clean_store(source_id, date, target_table, frames):
    """
    门店清洗
    :param source_id:
    :param date:
    :param target_table:
    :param frames:
    :return:
    """
    cmid = source_id.split("Y")[0]
    store_frame = frames["t_br_master"].merge(frames["t_br_ext"], how="left", on="fbrh_no")
    store_frame = store_frame[store_frame['fbrh_type'].isin([5, 6])]
    store_frame["cmid"] = cmid
    store_frame["address_code"] = None
    store_frame["device_id"] = None
    store_frame["lat"] = None
    store_frame["lng"] = None
    store_frame["area_code"] = ''
    store_frame["area_name"] = ''
    store_frame["business_area"] = None
    store_frame["property_id"] = None
    store_frame["source_id"] = source_id
    store_frame["last_updated"] = datetime.now(_TZINFO)
    store_frame["show_code"] = store_frame["fbrh_no"]

    store_frame["store_status"] = store_frame.fstatus.apply(lambda x: '闭店' if x == 9 else '正常')
    store_frame["property"] = store_frame.fbrh_type.apply(lambda x: '直营店' if x == 5 else '加盟店')
    store_frame = store_frame.rename(
        columns={"fbrh_no": "foreign_store_id", "fbrh_name": "store_name", "faddr": "store_address",
                 "fcr_date": "create_date", "ftel": "phone_number", "fman": "contacts"})
    store_frame = store_frame[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    return upload_to_s3(store_frame, source_id, date, target_table)


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


def frame1(cmid, source_id, frames):
    columns = [
        "source_id", "cmid", "foreign_store_id", "store_name", "receipt_id", "consumer_id", "saletime", "last_updated",
        "foreign_item_id", "barcode", "item_name", "item_unit", "saleprice", "quantity", "subtotal",
        "foreign_category_lv1", "foreign_category_lv1_name", "foreign_category_lv2", "foreign_category_lv2_name",
        "foreign_category_lv3", "foreign_category_lv3_name", "foreign_category_lv4", "foreign_category_lv4_name",
        "foreign_category_lv5", "foreign_category_lv5_name", "pos_id"
    ]

    temp1 = frames["t_sl_master"].merge(frames["t_sl_detail"], how="left", on="fflow_no")

    if not len(temp1):
        return pd.DataFrame(columns=columns)

    def gene_quantity_or_sbutotal(x, y):
        if x == 2:
            return -1 * y
        return y

    temp1["quantity"] = temp1.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["fpack_qty"]), axis=1)
    temp1["subtotal"] = temp1.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["famt"]), axis=1)

    temp1 = temp1.merge(frames["t_br_master"], how="left", on="fbrh_no")

    temp1 = temp1.merge(frames["t_bi_master"], how="inner", on=["fitem_id", "fitem_subno"],
                        suffixes=('_x', ''))

    temp1 = temp1.merge(
        frames["t_bc_master"], how="left", on="fitem_clsno")

    temp1 = temp1.merge(frames["t_bc_master"], how="left", left_on="fprt_no", right_on="fitem_clsno",
                        suffixes=('_lv3', '_lv2'))

    temp1 = temp1.merge(frames["t_bc_master"], how="left", left_on="fprt_no_lv2",
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
    temp1["last_updated"] = datetime.now(_TZINFO)
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

    temp1 = temp1[columns]

    return temp1


def frame2(cmid, source_id, frames):
    columns = [
        "source_id", "cmid", "foreign_store_id", "store_name", "receipt_id", "consumer_id", "saletime", "last_updated",
        "foreign_item_id", "barcode", "item_name", "item_unit", "saleprice", "quantity", "subtotal",
        "foreign_category_lv1", "foreign_category_lv1_name", "foreign_category_lv2", "foreign_category_lv2_name",
        "foreign_category_lv3", "foreign_category_lv3_name", "foreign_category_lv4", "foreign_category_lv4_name",
        "foreign_category_lv5", "foreign_category_lv5_name", "pos_id"
    ]

    temp2 = frames["t_sl_master"].merge(frames["t_sl_detail"], how="left", on="fflow_no")

    temp2 = temp2.merge(frames["t_br_master"], how="left", on="fbrh_no")

    temp2 = temp2.merge(frames["t_bi_master"], how="inner", on="fitem_id", suffixes=('', '_y'))

    temp2 = temp2.merge(frames["t_bi_barcode"], how="inner", on=["fitem_id", "fitem_subno"])

    if not len(temp2):
        return pd.DataFrame(columns=columns)

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
    temp2["saleprice"] = temp2.apply(lambda row: row["fprice"] / row["funit_qty"], axis=1)


    temp2 = temp2.merge(frames["t_bc_master"], how="left", on="fitem_clsno")

    temp2 = temp2.merge(frames["t_bc_master"], how="left", left_on="fprt_no", right_on="fitem_clsno",
                        suffixes=('_lv3', '_lv2'))

    temp2 = temp2.merge(frames["t_bc_master"], how="left", left_on="fprt_no_lv2",
                                                         right_on="fitem_clsno")

    temp2 = temp2.rename(columns={"fbrh_no": "foreign_store_id", "fbrh_name": "store_name", "fflow_no": "receipt_id",
                                  "fitem_id": "foreign_item_id", "fitem_subno": "barcode", "fitem_name": "item_name",
                                  "funit_no": "item_unit", "fitem_clsno": "foreign_category_lv1",
                                  "fitem_clsname": "foreign_category_lv1_name",
                                  "fitem_clsno_lv2": "foreign_category_lv2",
                                  "fitem_clsname_lv2": "foreign_category_lv2_name",
                                  "fitem_clsno_lv3": "foreign_category_lv3",
                                  "fitem_clsname_lv3": "foreign_category_lv3_name"})

    temp2.insert(0, 'cmid', cmid)
    temp2.insert(0, 'source_id', source_id)
    temp2["consumer_id"] = ''
    temp2["saletime"] = temp2.pop("ftrade_date") + " " + temp2.pop("fcr_time")
    temp2["last_updated"] = datetime.now(_TZINFO)
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

    temp2 = temp2[columns]

    return temp2


def frame3(cmid, source_id, frames):
    columns = [
        "source_id", "cmid", "foreign_store_id", "store_name", "receipt_id", "consumer_id", "saletime", "last_updated",
        "foreign_item_id", "barcode", "item_name", "item_unit", "saleprice", "quantity", "subtotal",
        "foreign_category_lv1", "foreign_category_lv1_name", "foreign_category_lv2", "foreign_category_lv2_name",
        "foreign_category_lv3", "foreign_category_lv3_name", "foreign_category_lv4", "foreign_category_lv4_name",
        "foreign_category_lv5", "foreign_category_lv5_name", "pos_id"
    ]

    temp3 = frames["t_sl_master"].merge(frames["t_sl_detail"], how="left", on="fflow_no")

    temp3 = temp3.merge(frames["t_br_master"], how="left", on="fbrh_no")
    temp3 = temp3[temp3['fitem_id'] == '0']

    def gene_quantity_or_sbutotal(x, y):
        if x == 2:
            return -1 * y
        return y

    if not len(temp3):
        return pd.DataFrame(columns=columns)
    temp3["quantity"] = temp3.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["fpack_qty"]), axis=1)
    temp3["subtotal"] = temp3.apply(lambda row: gene_quantity_or_sbutotal(row["fsell_way"], row["famt"]), axis=1)

    temp3 = temp3.rename(columns={"fbrh_no": "foreign_store_id", "fbrh_name": "store_name", "fflow_no": "receipt_id",
                                  "fitem_id": "foreign_item_id", "fitem_subno": "barcode", "fitem_name": "item_name",
                                  "fprice": "saleprice"})

    temp3.insert(0, 'cmid', cmid)
    temp3.insert(0, 'source_id', source_id)
    temp3["consumer_id"] = ''
    temp3["saletime"] = temp3.pop("ftrade_date") + " " + temp3.pop("fcr_time")
    temp3["last_updated"] = datetime.now(_TZINFO)
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

    temp3 = temp3[columns]

    return temp3
