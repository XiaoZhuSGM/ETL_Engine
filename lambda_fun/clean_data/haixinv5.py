"""
中山及时清洗逻辑
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
    elif target_table == 'requireorder':
        return clean_requireorder(source_id, date, target_table, data_frames)
    elif target_table == 'delivery':
        return clean_delivery(source_id, date, target_table, data_frames)

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

    head = data_frames["tsalsaleplu"]
    stores = data_frames["torgmanage"]
    item = data_frames["tskuplu"]
    lv3 = data_frames["tcatcategory"].rename(columns=lambda x: f"lv3.{x}")
    lv2 = data_frames["tcatcategory"].rename(columns=lambda x: f"lv2.{x}")
    lv1 = data_frames["tcatcategory"].rename(columns=lambda x: f"lv1.{x}")
    frames = head.merge(stores, on="orgcode", how='left').merge(item, on="pluid", how='left').merge(lv3, how="left",
                                                                                                    left_on="clsid",
                                                                                                    right_on="lv3.clsid")
    if len(frames) == 0:
        frames = pd.DataFrame(columns=[
            'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime',
            'last_updated',
            'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
            'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
            'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
            'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'])
    else:
        frames["lv2.clscode"] = frames["lv3.clscode"].apply(lambda x:'' if pd.isnull(x) else str(x)[:4])
        frames["lv1.clscode"] = frames["lv3.clscode"].apply(lambda x:'' if pd.isnull(x) else str(x)[:2])
        frames = frames.merge(lv2, how="left", on="lv2.clscode").merge(lv1, how="left", on="lv1.clscode")
        frames = frames[frames["trantype"] != "5"]
        frames["source_id"] = source_id
        frames["cmid"] = cmid
        frames["consumer_id"] = ""
        frames["last_updated"] = datetime.now(_TZINFO)
        frames["foreign_category_lv4"] = ""
        frames["foreign_category_lv4_name"] = None
        frames["foreign_category_lv5"] = ""
        frames["foreign_category_lv5_name"] = None
        frames["pos_id"] = ""
        frames = frames.rename(columns={
            "orgcode": "foreign_store_id",
            "orgname": "store_name",
            "saleno": "receipt_id",
            "xsdate": "saletime",
            "pluid": "foreign_item_id",
            "barcode": "barcode",
            "pluname": "item_name",
            "unit": "item_unit",
            "price": "saleprice",
            "xscount": "quantity",
            "sstotal": "subtotal",
            "lv1.clscode": "foreign_category_lv1",
            "lv1.clsname": "foreign_category_lv1_name",
            "lv2.clscode": "foreign_category_lv2",
            "lv2.clsname": "foreign_category_lv2_name",
            "lv3.clscode": "foreign_category_lv3",
            "lv3.clsname": "foreign_category_lv3_name",
        })

        frames = frames[[
            'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime',
            'last_updated', 'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
            'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
            'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
            'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'
        ]]
    return upload_to_s3(frames, source_id, date, target_table)


def clean_cost(source_id, date, target_table, data_frames):
    """
    清洗成本
    """
    cmid = source_id.split("Y")[0]

    cost = data_frames["tsalpludetail"]
    item = data_frames["tskuplu"]
    lv3 = data_frames["tcatcategory"]

    frames = cost.merge(item, on="pluid")

    if len(frames) == 0:
        frames = pd.DataFrame(columns=[
            "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
            "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3",
            "foreign_category_lv4", "foreign_category_lv5", "cmid"])
    else:
        frames = frames.merge(lv3, how="left", on="clsid")
        if source_id =='86YYYYYYYYYYYYY':
            frames = frames[(frames["orgcode"] != "JSZB") & (frames["clscode"].map(len) == 6)]
        else:
            frames = frames[(frames["orgcode"] != "00") & (frames["clscode"].map(len) == 6)]
        frames = frames.groupby(["orgcode", "pluid", "rptdate", "clscode"], as_index=False) \
            .agg({"xscount": sum, "hxtotal": sum, "hjcost": sum})

        frames["source_id"] = source_id
        frames["cost_type"] = ""
        frames["foreign_category_lv1"] = frames["clscode"].apply(lambda x: x[:len(x) - 4])
        frames["foreign_category_lv2"] = frames["clscode"].apply(lambda x: x[:len(x) - 2])
        frames["foreign_category_lv4"] = ""
        frames["foreign_category_lv5"] = ""
        frames["cmid"] = cmid

        frames = frames.rename(columns={
            "orgcode": "foreign_store_id",
            "pluid": "foreign_item_id",
            "rptdate": "date",
            "xscount": "total_quantity",
            "hxtotal": "total_sale",
            "hjcost": "total_cost",
            "clscode": "foreign_category_lv3"
        })
        frames["date"] = frames["date"].apply(lambda row: row.split()[0])
        frames = frames[[
            "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity", "total_sale",
            "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3",
            "foreign_category_lv4", "foreign_category_lv5", "cmid"
        ]]

    return upload_to_s3(frames, source_id, date, target_table)


def clean_goods(source_id, date, target_table, data_frames):
    """
    清洗商品
    """
    cmid = source_id.split("Y")[0]

    item = data_frames["tskuplu"]
    lv = data_frames["tcatcategory"]
    tsk = data_frames["tskumainetp"]
    sup = data_frames["tetpsupplier"]

    frames = item.merge(
        lv, how="left", on="clsid").merge(
        tsk, how='left', on="pluid").merge(
        sup, how='left', on=["orgcode", "etpcode"])

    frames["cmid"] = cmid

    def generate_item_status(x):
        if x == "1":
            return "正常"
        elif x == "2":
            return "预淘汰"
        elif x == "3":
            return "淘汰"

    frames["item_status"] = frames.ywstatus.apply(generate_item_status)
    frames["foreign_category_lv1"] = frames["clscode"].apply(lambda x: x[:2])
    frames["foreign_category_lv2"] = frames["clscode"].apply(lambda x: x[:4])
    frames["foreign_category_lv4"] = ""
    frames["foreign_category_lv5"] = ""
    frames["last_updated"] = datetime.now(_TZINFO)
    frames["isvalid"] = 1
    frames["warranty"] = ""
    frames["allot_method"] = ""
    frames["brand_name"] = ""
    frames["storage_time"] = frames["lrdate"]
    frames = frames.rename(columns={
        "barcode": "barcode",
        "pluid": "foreign_item_id",
        "pluname": "item_name",
        "hjprice": "lastin_price",
        "price": "sale_price",
        "unit": "item_unit",
        "plucode": "show_code",
        "clscode": "foreign_category_lv3",
        "etpcode": "supplier_code",
        "etpname": "supplier_name"
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

    lv1 = data_frames["tcatcategory"].rename(columns=lambda x: f"lv1.{x}")
    lv2 = data_frames["tcatcategory"].rename(columns=lambda x: f"lv2.{x}")
    lv3 = data_frames["tcatcategory"].rename(columns=lambda x: f"lv3.{x}")

    category1 = lv1.copy()
    category1 = category1[category1["lv1.clscode"].map(len) == 2]
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
    category1["last_updated"] = datetime.now(_TZINFO)
    category1 = category1.rename(columns={
        "lv1.clscode": "foreign_category_lv1",
        "lv1.clsname": "foreign_category_lv1_name"
    })
    category1 = category1[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]
    category2 = lv2.copy()
    category2["lv1.clscode"] = category2["lv2.clscode"].apply(lambda x: x[:len(x) - 2])
    category2 = category2.merge(lv1.copy(), how="left", on="lv1.clscode")
    category2 = category2[category2["lv2.clscode"].map(len) == 4]

    category2["cmid"] = cmid
    category2["level"] = 2
    category2["foreign_category_lv3"] = ""
    category2["foreign_category_lv3_name"] = ""
    category2["foreign_category_lv4"] = ""
    category2["foreign_category_lv4_name"] = ""
    category2["foreign_category_lv5"] = ""
    category2["foreign_category_lv5_name"] = ""
    category2["last_updated"] = datetime.now(_TZINFO)
    category2 = category2.rename(columns={
        "lv1.clscode": "foreign_category_lv1",
        "lv1.clsname": "foreign_category_lv1_name",
        "lv2.clscode": "foreign_category_lv2",
        "lv2.clsname": "foreign_category_lv2_name",
    })
    category2 = category2[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category3 = lv3.copy()
    category3["lv2.clscode"] = category3["lv3.clscode"].apply(lambda x: x[:len(x) - 2])
    category3["lv1.clscode"] = category3["lv3.clscode"].apply(lambda x: x[:len(x) - 4])
    category3 = category3.merge(lv2.copy(), how="left", on="lv2.clscode").merge(lv1.copy(), how="left",
                                                                                on="lv1.clscode")

    category3 = category3[category3["lv3.clscode"].map(len) == 6]
    category3["cmid"] = cmid
    category3["level"] = 3
    category3["foreign_category_lv4"] = ""
    category3["foreign_category_lv4_name"] = ""
    category3["foreign_category_lv5"] = ""
    category3["foreign_category_lv5_name"] = ""
    category3["last_updated"] = datetime.now(_TZINFO)
    category3 = category3.rename(columns={
        "lv1.clscode": "foreign_category_lv1",
        "lv1.clsname": "foreign_category_lv1_name",
        "lv2.clscode": "foreign_category_lv2",
        "lv2.clsname": "foreign_category_lv2_name",
        "lv3.clscode": "foreign_category_lv3",
        "lv3.clsname": "foreign_category_lv3_name",
    })
    category3 = category3[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    category = pd.concat([category1, category2, category3])

    return upload_to_s3(category, source_id, date, target_table)


def clean_store(source_id, date, target_table, data_frames):
    """
    门店清洗
    """
    cmid = source_id.split("Y")[0]

    stores = data_frames["torgmanage"].rename(columns=lambda x: f"stores.{x}")
    area = data_frames["torgmanage"]
    area = area[area["orgclass"] == "0"].rename(columns=lambda x: f"area.{x}")
    frames = stores.merge(area, how="left", left_on="stores.preorgcode", right_on="area.orgcode")
    frames = frames[frames["stores.orgclass"] != "0"]

    frames["cmid"] = cmid
    frames["source_id"] = source_id
    frames["store_address"] = ""
    frames["address_code"] = None
    frames["device_id"] = None
    def store_status(value):
        if value == '0':
            return '闭店'
        if value == '1':
            return '正常'
    frames["store_status"] = frames['stores.isenable'].apply(store_status)
    frames["create_date"] = datetime.now(_TZINFO)
    frames["last_updated"] = datetime.now(_TZINFO)
    frames["lat"] = None
    frames["lng"] = None
    frames["show_code"] = frames["stores.orgcode"]
    frames["phone_number"] = ""
    frames["contacts"] = ""
    frames["business_area"] = None
    frames["property_id"] = ""
    frames["property"] = ""
    frames["last_updated"] = datetime.now(_TZINFO)

    frames = frames.rename(columns={
        "stores.orgcode": "foreign_store_id",
        "stores.orgname": "store_name",
        "area.orgcode": "area_code",
        "area.orgname": "area_name"
    })
    frames = frames[
        ['cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
         'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
         'business_area', 'property_id', 'property', 'source_id', 'last_updated']]

    return upload_to_s3(frames, source_id, date, target_table)


def clean_requireorder(source_id, date, target_table, data_frames):
    "门店要货清洗"
    cmid = source_id.split("Y")[0]
    head = data_frames['tordyhhead']
    detail = data_frames['tordyhbody']
    item = data_frames['tskuplu']
    lv = data_frames['tcatcategory']
    tsk = data_frames['tskumainetp']
    sup = data_frames['tetpsupplier']
    frames = head.merge(
        detail, how='inner', on='billno').merge(
        item, how='inner', on='pluid').merge(
        lv, how='inner', on='clsid')
    supplier = tsk.merge(
        sup, how='left', on=['etpcode', 'orgcode'])
    frames = frames.merge(supplier, how='left', on='pluid')
    frames = frames[frames['isactive'] == '1']
    frames['source_id'] = source_id
    frames['cmid'] = cmid
    frames['order_type'] = '门店要货'
    frames['store_show_code'] = frames['etpcode_x']
    frames = frames.rename(columns={
        'billno': 'order_num',
        'jzdate': 'order_date',
        'etpcode_x': 'foreign_store_id',
        'etpname_x': 'store_name',
        'pluid': 'foreign_item_id',
        'plucode': 'item_show_code',
        'pluname': 'item_name',
        'unit': 'item_unit',
        'yhcount': 'order_qty',
        'psprice': 'order_price',
        'pssum': 'order_total',
        'etpcode_y': 'vendor_id',
        'etpname_y': 'vendor_name'
    })
    frames['vendor_show_code'] = frames['vendor_id']
    frames['foreign_category_lv1'] = frames['clscode'].apply(lambda x: x[:2])
    frames["foreign_category_lv2"] = frames['clscode'].apply(lambda x: x[:4])
    frames["foreign_category_lv3"] = frames['clscode']
    frames["foreign_category_lv4"] = ''
    frames['foreign_category_lv5'] = ''
    frames['purchaser'] = ''
    frames = frames[[
        "source_id",
        "cmid",
        "order_num",
        "order_date",
        "order_type",
        "foreign_store_id",
        "store_show_code",
        "store_name",
        "foreign_item_id",
        "item_show_code",
        "barcode",
        "item_name",
        "item_unit",
        "order_qty",
        "order_price",
        "order_total",
        "vendor_id",
        "vendor_show_code",
        "vendor_name",
        "foreign_category_lv1",
        "foreign_category_lv2",
        "foreign_category_lv3",
        "foreign_category_lv4",
        "foreign_category_lv5",
        "purchaser",
    ]]
    return upload_to_s3(frames, source_id, date, target_table)


def clean_delivery(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    head = data_frames['tdstpshead']
    detail = data_frames['tdstpsbody']
    frame = head.merge(detail, how='inner', on='billno')
    frame['cmid'] = cmid
    frame['source_id'] = source_id
    frame['delivery_type'] = '统配出'
    frame['foreign_category_lv1'] = frame['clscode'].apply(lambda x: x[:2])
    frame["foreign_category_lv2"] = frame['clscode'].apply(lambda x: x[:4])
    frame["foreign_category_lv3"] = frame['clscode']
    frame["foreign_category_lv4"] = ''
    frame['foreign_category_lv5'] = ''
    frame['delivery_state'] = ''
    frame['store_show_code'] = frame['shorgcode']
    frame['warehouse_show_code'] = frame['orgcode']
    frame = frame.rename(columns={
        'billno': 'delivery_num',
        'jzdate': 'delivery_date',
        'shorgcode': 'foreign_store_id',
        'shorgname': 'store_name',
        'pluid': 'foreign_item_id',
        "plucode": "item_show_code",
        "barcode": "barcode",
        "pluname": "item_name",
        "unit": "item_unit",
        "pscount": "delivery_qty",
        "psprice": "rtl_price",
        "pstotal": "rtl_amt",
        "orgcode": "warehouse_id",
        "orgname": "warehouse_name",
        "htname": "src_type"
    })
    frame = frame[[
        "delivery_num",
        "delivery_date",
        "delivery_type",
        "foreign_store_id",
        "store_show_code",
        "store_name",
        "foreign_item_id",
        "item_show_code",
        "barcode",
        "item_name",
        "item_unit",
        "delivery_qty",
        "rtl_price",
        "rtl_amt",
        "warehouse_id",
        "warehouse_show_code",
        "warehouse_name",
        "src_type",
        "delivery_state",
        "foreign_category_lv1",
        "foreign_category_lv2",
        "foreign_category_lv3",
        "foreign_category_lv4",
        "foreign_category_lv5",
        "source_id",
        "cmid",
    ]]
    return upload_to_s3(frame, source_id, date, target_table)


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
