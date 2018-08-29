# -*- coding: utf-8 -*-
# @Time    : 2018/8/27 10:38
# @Author  : 范佳楠
import ssl
import tempfile
import time
from datetime import datetime

import boto3
import pandas as pd
import pytz
from typing import Dict

S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"
_TZINFO = pytz.timezone("Asia/Shanghai")


class MeiShiLinCleaner(object):

    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        self.source_id = source_id
        self.date = date
        self.cmid = self.source_id.split('Y', 1)[0]
        self.data = data

    def clean(self, target_table):
        method = getattr(self, target_table, None)
        if method and callable(method):
            df = getattr(self, target_table)()
            return self.up_load_to_s3(df, target_table)
        else:
            raise RuntimeError(f"没有这个表:{target_table}")

    def up_load_to_s3(self, dataframe, target_table):
        file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8')
        count = len(dataframe)
        dataframe.to_csv(file.name, index=False, compression='gzip', float_format='%.4f')
        file.seek(0)
        key = CLEANED_PATH.format(
            source_id=self.source_id,
            target_table=target_table,
            date=self.date,
            timestamp=datetime.fromtimestamp(time.time(), tz=_TZINFO),
            rowcount=count,
        )

        S3.Bucket(S3_BUCKET).upload_file(file.name, key)

        return key

    """
    "origin_table_columns": {
            "dbo.skstoresellingwater": ['sgid', 'gid', 'flowno', 'rtlprc', 'qty', 'realamt', 'fildate'],
            "dbo.skstore": ['gid', 'name'],
            "dbo.skgoods": ['gid', 'code2', 'name', 'munit'],
            "dbo.skgoodssort": ['gid', 'ascode', 'asname', 'bscode', 'bsname', 'cscode', 'csname']
        },

        "converts": {
            "dbo.skstoresellingwater": {"sgid": "str", "gid": "str",
                                        "flowno": "str", "rtlprc": "float", 'qty': 'float', 'realamt': 'float'},
            "dbo.skstore": {'gid': 'str', 'name': 'str'},
            'dbo.skgoods': {'gid': 'str', 'code2': 'str', 'name': 'str', 'munit': 'str'},
            'dbo.skgoodssort': {'gid': 'str', 'ascode': 'str', 'asname': 'str', 'bscode': 'str', 'bsname': 'str',
                                'cscode': 'str', 'csname': 'str'}
        }
    """

    def goodsflow(self):
        flow_frame = self.data['dbo.skstoresellingwater']
        store_frame = self.data['dbo.skstore']
        goods_frame = self.data['dbo.skgoods']
        gsort_frame = self.data['dbo.skgoodssort']
        flow_frame['flowno'] = flow_frame['flowno'].str.strip()
        result_frame = pd.merge(flow_frame,
                                store_frame,
                                left_on='sgid',
                                right_on='gid',
                                how='left',
                                suffixes=('_flow', '_store')).merge(goods_frame,
                                                                    left_on='gid_flow',
                                                                    right_on='gid',
                                                                    how='left',
                                                                    suffixes=('_store', '_goods')).merge(gsort_frame,
                                                                                                         left_on='gid',
                                                                                                         right_on='gid',
                                                                                                         how='left')

        result_frame['source_id'] = self.source_id
        result_frame['cmid'] = self.cmid
        result_frame['last_updated'] = datetime.now()
        result_frame['foreign_category_lv4'] = ''
        result_frame['foreign_category_lv4_name'] = None
        result_frame['foreign_category_lv5'] = ''
        result_frame['foreign_category_lv5_name'] = None
        result_frame['pos_id'] = ''
        result_frame['consumer_id'] = None

        result_frame = result_frame.rename(columns={
            'gid_store': 'foreign_store_id',
            'name_store': 'store_name',
            'flowno': 'receipt_id',
            'fildate': 'saletime',
            'gid': 'foreign_item_id',
            'code2': 'barcode',
            'name_goods': 'item_name',
            'munit': 'item_unit',
            'rtlprc': 'saleprice',
            'qty': 'quantity',
            'realamt': 'subtotal',
            'ascode': 'foreign_category_lv1',
            'asname': 'foreign_category_lv1_name',
            'bscode': 'foreign_category_lv2',
            'bsname': 'foreign_category_lv2_name',
            'cscode': 'foreign_category_lv3',
            'csname': 'foreign_category_lv3_name'

        })

        result_frame = result_frame[[
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

        return result_frame

    """
        "origin_table_columns": {
                "dbo.skcmsale": ['pdkey', 
                                 'orgkey', 
                                 'fildate', 
                                 'saleqty', 
                                 'saleamt', 
                                 'saletax',
                                 'salecamt',
                                 'salectax',
    
                                 ],
                "dbo.skgoodssort":['gid', 'ascode', 'bscode', 'cscode'],
            },

        "converts": {
            "dbo.skcmsale": {'pdkey':'str', 
                             'saleqty':'float',
                             'saleamt':'float',
                             'saletax':'float',
                             'salecamt':'float',
                             'salectax':'float',
                             },
            "dbo.skgoodssort": {"gid":'str'}
        }
        """

    def cost(self):
        cost_frame = self.data['dbo.skcmsale']
        gsort_frame = self.data['dbo.skgoodssort']

        result_frame = pd.merge(cost_frame, gsort_frame, left_on='pdkey', right_on='gid', how='left')
        result_frame['source_id'] = self.source_id
        result_frame['cost_type'] = ''
        result_frame['foreign_category_lv4'] = ''
        result_frame['foreign_category_lv5'] = ''
        result_frame['cmid'] = self.cmid

        result_frame['total_sale'] = result_frame.apply(lambda row: row['saleamt'] + row['saletax'], axis=1)
        result_frame['total_cost'] = result_frame.apply(lambda row: row['salecamt'] + row['salectax'], axis=1)

        result_frame = result_frame.rename(columns={
            'orgkey': 'foreign_store_id',
            'pdkey': 'foreign_item_id',
            'fildate': 'date',
            'saleqty': 'total_quantity',
            'ascode': 'foreign_category_lv1',
            'bscode': 'foreign_category_lv2',
            'cscode': 'foreign_category_lv3',
        })

        result_frame = result_frame[[
            "source_id",
            "foreign_store_id",
            "foreign_item_id",
            "date",
            "cost_type",
            "total_quantity",
            "total_sale",
            "total_cost",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
            "cmid",
        ]]

        return result_frame

    """
    "origin_table_columns": {
        "dbo.skstore": [
            "address",
            "area",
            "code",
            "contactor",
            "gid",
            "name",
            "phone",
            "property",
            "stat",
        ],
        "dbo.skcmarea": ["code", "name"],
    },
    "converts": {
        "dbo.skstore": {
            "gid": "str",
            "name": "str",
            "address": "str",
            "code": "str",
            "area": "str",
            "contactor": "str",
            "phone": "str",
        },
        "dbo.skcmarea": {"code": "str", "name": "str"},
    },
    """

    def store(self):
        store = self.data["dbo.skstore"]
        area = self.data["dbo.skcmarea"]

        store['area'] = store['area'].str.strip()
        area['code'] = area['code'].str.strip()
        area['name'] = area['name'].str.strip()

        columns = [
            "cmid",
            "foreign_store_id",
            "store_name",
            "store_address",
            "address_code",
            "device_id",
            "store_status",
            "create_date",
            "lat",
            "lng",
            "show_code",
            "phone_number",
            "contacts",
            "area_code",
            "area_name",
            "business_area",
            "property_id",
            "property",
            "source_id",
            "last_updated",
        ]

        part = store.merge(
            area,
            how="left",
            left_on=["area"],
            right_on=["code"],
            suffixes=("", ".area"),
        )

        part["cmid"] = self.cmid
        part["source_id"] = self.source_id
        part["address_code"] = ""
        part["device_id"] = ""
        part["lat"] = None
        part["lng"] = None
        part["create_date"] = None
        part["last_updated"] = datetime.now()
        part["store_status"] = part.apply(
            lambda row: "闭店" if row["stat"] == 1 else "正常", axis=1
        )
        part["property_id"] = part["property"]

        def property_map(prop):
            if (prop & 0) > 0:
                return "单店;"
            elif (prop & 2) > 0:
                return "连网连锁店;"
            elif (prop & 4) > 0:
                return "连锁内加盟;"
            elif (prop & 8) > 0:
                return "配货中心;"
            elif (prop & 16) > 0:
                return "总部;"
            elif (prop & 32) > 0:
                return "物流中心;"
            elif (prop & 64) > 0:
                return "连锁外加盟(毛利结算);"
            elif (prop & 128) > 0:
                return "信息中心;"
            elif (prop & 256) > 0:
                return "连锁外加盟(销配结算);"

        part["property"] = part.apply(
            lambda row: property_map(int(row["property_id"])), axis=1
        )
        part['business_area'] = ''

        part = part.rename(
            columns={
                "gid": "foreign_store_id",
                "name": "store_name",
                "address": "store_address",
                "code": "show_code",
                "phone": "phone_number",
                "contactor": "contacts",
                "code.area": "area_code",
                "name.area": "area_name",
            }
        )

        part = part[columns]
        return part

    """
    "origin_table_columns": {
        "dbo.skgoods": [
            "alc",
            "brand",
            "busgate",
            "code",
            "code2",
            "gid",
            "lstinprc",
            "munit",
            "name",
            "rtlprc",
            "sort",
            "validperiod",
            "vdrgid",
            'lifecycle'
        ],
        "dbo.skcmbrand": ["name", "code", 'gid'],
        "dbo.skcmvendor": ["name", "code", "gid"],
    },
    "converts": {
        "dbo.skgoods": {
            "alc": "str",
            "brand": "str",
            "busgate": "str",
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
            "vdrgid": "str",
            'lifecycle':'str'
        },
        "dbo.skcmbrand": {"name": "str", "code": "str", 'gid':'str'},
        "dbo.skcmvendor": {"gid": "str", "code": "str", "name": "str"},
    },
    """

    def goods(self):
        goods = self.data["dbo.skgoods"]
        # goodsbusgate = self.data["dbo.goodsbusgate"]
        brand = self.data["dbo.skcmbrand"]
        vendor = self.data["dbo.skcmvendor"]

        columns = [
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
        ]

        vendor['gid'] = vendor['gid'].str.strip()

        part = goods.merge(
            brand,
            how="left",
            left_on=["brand"],
            right_on=["code"],
            suffixes=("", ".brand"),
        ).merge(
            vendor,
            how="left",
            left_on=["vdrgid"],
            right_on=["gid"],
            suffixes=("", ".vendor"),
        )

        part["foreign_category_lv1"] = part.apply(lambda row: row["sort"][:2], axis=1)
        part["foreign_category_lv2"] = part.apply(lambda row: row["sort"][:4], axis=1)
        part["foreign_category_lv3"] = part.apply(lambda row: row["sort"][:6], axis=1)
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["cmid"] = self.cmid
        part["storage_time"] = datetime.now()
        part["last_updated"] = datetime.now()
        part["isvalid"] = 1
        part['lastin_price'] = None

        part = part.rename(
            columns={
                "code2": "barcode",
                "gid": "foreign_item_id",
                "name": "item_name",
                "rtlprc": "sale_price",
                "munit": "item_unit",
                "lifecycle": "item_status",
                "validperiod": "warranty",
                "code": "show_code",
                "alc": "allot_method",
                "name.vendor": "supplier_name",
                "code.vendor": "supplier_code",
                "name.brand": "brand_name",
            }
        )
        part = part[columns]
        return part

    """
        "origin_table_columns": {"dbo.skcmsort": ["code", "name"]},
        "converts": {"dbo.skcmsort": {"code": "str", "name": "str"}},
    """

    def category(self):

        sort = self.data["dbo.skcmsort"]
        sort['code'] = sort['code'].str.strip()
        sort['name'] = sort['name'].str.strip()

        sort["code1"] = sort.apply(lambda row: row["code"][:2], axis=1)
        sort["code2"] = sort.apply(lambda row: row["code"][:4], axis=1)

        columns = [
            "cmid",
            "level",
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "last_updated",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
            "foreign_category_lv5",
            "foreign_category_lv5_name",
        ]

        part1 = sort[sort["code"].str.len() == 2].copy()
        part1["cmid"] = self.cmid
        part1["level"] = 1
        part1["foreign_category_lv2"] = ""
        part1["foreign_category_lv2_name"] = None
        part1["foreign_category_lv3"] = ""
        part1["foreign_category_lv3_name"] = None
        part1["foreign_category_lv4"] = ""
        part1["foreign_category_lv4_name"] = None
        part1["foreign_category_lv5"] = ""
        part1["foreign_category_lv5_name"] = None
        part1["last_updated"] = datetime.now()

        part1 = part1.rename(
            columns={
                "code": "foreign_category_lv1",
                "name": "foreign_category_lv1_name",
            }
        )
        part1 = part1[columns]

        part2 = sort.merge(
            sort,
            how="left",
            left_on=["code1"],
            right_on=["code"],
            suffixes=("", ".sort1"),
        )
        part2 = part2[part2["code"].str.len() == 4]
        part2["cmid"] = self.cmid
        part2["level"] = 2
        part2["foreign_category_lv3"] = ""
        part2["foreign_category_lv3_name"] = None
        part2["foreign_category_lv4"] = ""
        part2["foreign_category_lv4_name"] = None
        part2["foreign_category_lv5"] = ""
        part2["foreign_category_lv5_name"] = None
        part2["last_updated"] = datetime.now()

        part2 = part2.rename(
            columns={
                "code.sort1": "foreign_category_lv1",
                "name.sort1": "foreign_category_lv1_name",
                "code": "foreign_category_lv2",
                "name": "foreign_category_lv2_name",
            }
        )
        part2 = part2[columns]

        part3 = sort.merge(
            sort,
            how="left",
            left_on=["code2"],
            right_on=["code"],
            suffixes=("", ".sort2"),
        ).merge(
            sort,
            how="left",
            left_on=["code1"],
            right_on=["code"],
            suffixes=("", ".sort3"),
        )

        part3 = part3[part3["code"].str.len() == 6]
        part3["cmid"] = self.cmid
        part3["level"] = 3
        part3["foreign_category_lv4"] = ""
        part3["foreign_category_lv4_name"] = None
        part3["foreign_category_lv5"] = ""
        part3["foreign_category_lv5_name"] = None
        part3["last_updated"] = datetime.now()

        part3 = part3.rename(
            columns={
                "code.sort3": "foreign_category_lv1",
                "name.sort3": "foreign_category_lv1_name",
                "code.sort2": "foreign_category_lv2",
                "name.sort2": "foreign_category_lv2_name",
                "code": "foreign_category_lv3",
                "name": "foreign_category_lv3_name",
            }
        )
        part3 = part3[columns]

        return pd.concat([part1, part2, part3])

    """
    
    "origin_table_columns": {
        "dbo.skcmotrequireorder": [
            "billnumber",
            "billtype",
            "buyercode",
            "checktime",
            "state",
            "uuid",
        ],
        "dbo.skcmotrequireorderline": [
            "munit",
            "checkedqty",
            "price",
            "checkedtotal",
            "bill",
            "product",
        ],
        "dbo.skstore": ["gid", "code", "name"],
        "dbo.skgoods": ["code", "code2", "gid", "name", "psr", "sort", "vdrgid"],
        "dbo.skcmsort": ["code"],
        "dbo.skcmvendor": ["gid", "code", "name"],
        "dbo.skcmemployee": ["name", "gid"],
    },
    "converts": {
        "dbo.skcmotrequireorder": {
            "billnumber": "str",
            "billtype": "str",
            "buyercode": "str",
            "checktime": "str",
            "uuid": "str",
        },
        "dbo.skcmotrequireorderline": {"munit": "str", "bill": "str", "product": "str"},
        "dbo.skstore": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "name": "str",
            "psr": "str",
            "sort": "str",
            "vdrgid": "str",
        },
        "dbo.skcmsort": {"code": "str"},
        "dbo.skcmvendor": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skcmemployee": {"name": "str", "gid": "str"},
    },
    
    """

    def requireorder(self):
        otrequireorder = self.data["dbo.skcmotrequireorder"]
        otrequireorderline = self.data["dbo.skcmotrequireorderline"]
        store = self.data["dbo.skstore"]
        goods = self.data["dbo.skgoods"]
        sort = self.data["dbo.skcmsort"]
        vendor = self.data["dbo.skcmvendor"]
        employee = self.data["dbo.skcmemployee"]

        employee['name'] = employee['name'].str.strip()

        otrequireorder['billnumber'] = otrequireorder['billnumber'].str.strip()
        otrequireorder['uuid'] = otrequireorder['uuid'].str.strip()
        otrequireorder['buyercode'] = otrequireorder['buyercode'].str.strip()
        otrequireorder['billtype'] = otrequireorder['billtype'].str.strip()
        otrequireorderline['bill'] = otrequireorderline['bill'].str.strip()
        otrequireorderline['product'] = otrequireorderline['product'].str.strip()
        goods['gid'] = goods['gid'].str.strip()
        goods['sort'] = goods['sort'].str.strip()
        goods['vdrgid'] = goods['vdrgid'].str.strip()
        goods['psr'] = goods['psr'].str.strip()
        sort['code'] = sort['code'].str.strip()
        vendor['gid'] = vendor['gid'].str.strip()
        employee['gid'] = employee['gid'].str.strip()

        # otrequireorder = otrequireorder.drop_duplicates()
        # otrequireorderline = otrequireorderline.drop_duplicates()
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)
        columns = [
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
        ]
        part = (
            otrequireorder.merge(
                otrequireorderline,
                how="inner",
                left_on=["uuid"],
                right_on=["bill"],
                suffixes=("", ".otrequireorderline"),
            )
                .merge(
                store,
                how="inner",
                left_on=["buyercode"],
                right_on=["code"],
                suffixes=("", ".sotre"),
            )
                .merge(
                goods,
                how="inner",
                left_on=["product"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort1"],
                right_on=["code"],
                suffixes=("", ".sort1"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort2"],
                right_on=["code"],
                suffixes=("", ".sort2"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort3"],
                right_on=["code"],
                suffixes=("", ".sort3"),
            )
                .merge(
                vendor,
                how="left",
                left_on=["vdrgid"],
                right_on=["gid"],
                suffixes=("", ".vendor"),
            )
                .merge(
                employee,
                how="left",
                left_on=["psr"],
                right_on=["gid"],
                suffixes=("", ".employee"),
            )
        )
        part["source_id"] = self.source_id
        part["cmid"] = self.cmid
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part = part[(part["state"] == 8000) & (part["billtype"] == "门店叫货")]
        part = part.rename(
            columns={
                "billnumber": "order_num",
                "checktime": "order_date",
                "billtype": "order_type",
                "gid": "foreign_store_id",
                "code": "store_show_code",
                "name": "store_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "checkedqty": "order_qty",
                "price": "order_price",
                "checkedtotal": "order_total",
                "gid.vendor": "vendor_id",
                "code.vendor": "vendor_show_code",
                "name.vendor": "vendor_name",
                "code.sort1": "foreign_category_lv1",
                "code.sort2": "foreign_category_lv2",
                "code.sort3": "foreign_category_lv3",
                "name.employee": "purchaser",
            }
        )
        part = part[columns]
        return part

    """
    "origin_table_columns": {
            "dbo.skcmstkout": ["billto", "cls", "num", "ocrdate", "stat"],
            "dbo.skcmstkoutdtl": ["alcsrc", "cls", "gdgid", "munit", "num", "qty", "wrh"],
            "dbo.skstore": ["gid", "name", "code"],
            "dbo.skcmwarehouse": ["gid", "name", "code"],
            "dbo.skgoods": ["code", "code2", "gid", "name", "rtlprc", "sort"],
            "dbo.skcmsort": ["code"],
            "dbo.skcmstkoutbck": ["billto", "cls", "num", "ocrdate", "stat"],
            "dbo.skcmstkoutbckdtl": ["bckcls", "cls", "gdgid", "munit", "num", "qty", "wrh"],
        },
        "converts": {
            "dbo.skcmstkout": {
                "billto": "str",
                "cls": "str",
                "num": "str",
                "ocrdate": "str",
                "stat": "str",
            },
            "dbo.skcmstkoutdtl": {
                "cls": "str",
                "gdgid": "str",
                "munit": "str",
                "num": "str",
                "wrh": "str",
                # 'qty':'float'
            },
            "dbo.skstore": {"gid": "str", "name": "str", "code": "str"},
            "dbo.skcmwarehouse": {"gid": "str", "name": "str", "code": "str"},
            "dbo.skgoods": {
                "code": "str",
                "code2": "str",
                "gid": "str",
                "name": "str",
                "sort": "str",
                # 'rtlprc':'float'
            },
            "dbo.skcmsort": {"code": "str"},
            "dbo.skcmstkoutbck": {
                "billto": "str",
                "cls": "str",
                "num": "str",
                "ocrdate": "str",
                "stat": "str",
            },
            "dbo.skcmstkoutbckdtl": {
                "bckcls": "str",
                "cls": "str",
                "gdgid": "str",
                "munit": "str",
                "num": "str",
                "wrh": "str",
            },
        },
    """

    def delivery(self):
        stkout = self.data["dbo.skcmstkout"]
        stkoutdtl = self.data["dbo.skcmstkoutdtl"]
        store = self.data["dbo.skstore"]
        warehouse = self.data["dbo.skcmwarehouse"]
        goods = self.data["dbo.skgoods"]
        sort = self.data["dbo.skcmsort"]
        stkoutbck = self.data["dbo.skcmstkoutbck"]
        stkoutbckdtl = self.data["dbo.skcmstkoutbckdtl"]

        stkoutdtl['munit'] = stkoutdtl['munit'].str.strip()
        stkoutbckdtl['munit'] = stkoutbckdtl['munit'].str.strip()

        sort['code'] = sort['code'].str.strip()

        stkout['num'] = stkout['num'].str.strip()
        stkoutdtl['num'] = stkoutdtl['num'].str.strip()
        stkoutdtl['cls'] = stkoutdtl['cls'].str.strip()
        stkout['cls'] = stkout['cls'].str.strip()
        store['gid'] = store['gid'].str.strip()
        stkout['billto'] = stkout['billto'].str.strip()
        stkoutdtl['wrh'] = stkoutdtl['wrh'].str.strip()
        warehouse['gid'] = warehouse['gid'].str.strip()
        stkoutdtl['gdgid'] = stkoutdtl['gdgid'].str.strip()
        goods['gid'] = goods['gid'].str.strip()

        goods['sort'] = goods['sort'].str.strip()
        stkout['stat'] = stkout['stat'].str.strip()

        stkoutbck['num'] = stkoutbck['num'].str.strip()
        stkoutbckdtl['num'] = stkoutbckdtl['num'].str.strip()
        stkoutbckdtl['cls'] = stkoutbckdtl['cls'].str.strip()
        stkoutbck['cls'] = stkoutbck['cls'].str.strip()
        stkoutbck['stat'] = stkoutbck['stat'].str.strip()
        stkoutbck['billto'] = stkoutbck['billto'].str.strip()
        stkoutbckdtl['wrh'] = stkoutbckdtl['wrh'].str.strip()
        stkoutbckdtl['gdgid'] = stkoutbckdtl['gdgid'].str.strip()

        stkout: pd.DataFrame = stkout[
            (stkout["cls"] == "统配出")
            & (stkout["stat"].isin(("0", "100", "300", "700", "1000")))
            ]
        stkoutbck: pd.DataFrame = stkoutbck[
            (stkoutbck["cls"] == "统配出退")
            & (stkoutbck["stat"].isin(("0", "100", "300", "700", "1000")))
            ]

        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

        columns = [
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
        ]

        src_type = {
            0: "人工录入",
            1: "门店叫货",
            2: "分货预留",
            3: "分货非预留",
            4: "自动配货",
            5: "替代商品",
            6: "特卖会",
            7: "首配",
        }
        delivery_state = {
            "0": "未审核",
            "100": "已审核",
            "300": "已完成",
            "700": "已发货",
            "1000": "已收货",
        }

        part1 = (
            stkout.merge(
                stkoutdtl, how="inner", on=["num", "cls"], suffixes=("", ".stkoutdtl")
            )
                .merge(
                store,
                how="inner",
                left_on=["billto"],
                right_on=["gid"],
                suffixes=("", ".store"),
            )
                .merge(
                warehouse,
                how="inner",
                left_on=["wrh"],
                right_on=["gid"],
                suffixes=("", ".warehouse"),
            )
                .merge(
                goods,
                how="inner",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort1"],
                right_on=["code"],
                suffixes=("", ".sort1"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort2"],
                right_on=["code"],
                suffixes=("", ".sort2"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort3"],
                right_on=["code"],
                suffixes=("", ".sort3"),
            )
        )

        # print(part1)

        part1["foreign_category_lv4"] = ""
        part1["foreign_category_lv5"] = ""
        part1["cmid"] = self.cmid
        part1["source_id"] = self.source_id
        part1["rtl_amt"] = part1.apply(lambda row: row["rtlprc"] * row["qty"], axis=1)
        part1["src_type"] = part1.apply(lambda row: src_type[row["alcsrc"]], axis=1)
        part1["delivery_state"] = part1.apply(
            lambda row: delivery_state[row["stat"]], axis=1
        )
        part1 = part1.rename(
            columns={
                "num": "delivery_num",
                "ocrdate": "delivery_date",
                "cls": "delivery_type",
                "gid": "foreign_store_id",
                "code": "store_show_code",
                "name": "store_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "qty": "delivery_qty",
                "rtlprc": "rtl_price",
                "gid.warehouse": "warehouse_id",
                "code.warehouse": "warehouse_show_code",
                "name.warehouse": "warehouse_name",
                "code.sort1": "foreign_category_lv1",
                "code.sort2": "foreign_category_lv2",
                "code.sort3": "foreign_category_lv3",
            }
        )
        part1 = part1[columns]

        part2 = (
            stkoutbck.merge(
                stkoutbckdtl,
                how="inner",
                on=["num", "cls"],
                suffixes=("", ".stkoutbckdtl"),
            )
                .merge(
                store,
                how="inner",
                left_on=["billto"],
                right_on=["gid"],
                suffixes=("", ".store"),
            )
                .merge(
                warehouse,
                how="inner",
                left_on=["wrh"],
                right_on=["gid"],
                suffixes=("", ".warehouse"),
            )
                .merge(
                goods,
                how="inner",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort1"],
                right_on=["code"],
                suffixes=("", ".sort1"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort2"],
                right_on=["code"],
                suffixes=("", ".sort2"),
            )
                .merge(
                sort,
                how="left",
                left_on=["sort3"],
                right_on=["code"],
                suffixes=("", ".sort3"),
            )
        )
        part2["foreign_category_lv4"] = ""
        part2["foreign_category_lv5"] = ""
        part2["cmid"] = self.cmid
        part2["source_id"] = self.source_id
        part2["delivery_qty"] = part2.apply(lambda row: row["qty"] * -1, axis=1)
        part2["rtl_amt"] = part2.apply(
            lambda row: row["rtlprc"] * row["qty"] * -1, axis=1
        )
        part2["delivery_state"] = part2.apply(
            lambda row: delivery_state[row["stat"]], axis=1
        )
        part2 = part2.rename(
            columns={
                "num": "delivery_num",
                "ocrdate": "delivery_date",
                "cls": "delivery_type",
                "gid": "foreign_store_id",
                "code": "store_show_code",
                "name": "store_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "rtlprc": "rtl_price",
                "gid.warehouse": "warehouse_id",
                "code.warehouse": "warehouse_show_code",
                "name.warehouse": "warehouse_name",
                "bckcls": "src_type",
                "code.sort1": "foreign_category_lv1",
                "code.sort2": "foreign_category_lv2",
                "code.sort3": "foreign_category_lv3",
            }
        )
        part2 = part2[columns]
        return pd.concat([part1, part2])

    """
        "origin_table_columns": {
        "dbo.skcmstkin": ["num", "fildate", "cls", "vendor", "stat"],
        "dbo.skcmstkindtl": [
            "qty",
            "price",
            "qpc",
            "total",
            "num",
            "cls",
            "gdgid",
            "wrh",
        ],
        "dbo.skcmvendor": ["gid", "code", "name"],
        "dbo.skcmmodulestat": ["statname", "no"],
        "dbo.skgoods": ["brand", "code", "code2", "gid", "munit", "name", "sort"],
        "dbo.skcmbrand": ["code", "name"],
        "dbo.skcmwarehouse": ["code", "name", "gid"],
        "dbo.skcmstkinbck": ["num", "fildate", "cls", "vendor", "stat"],
        "dbo.skcmstkinbckdtl": [
            "qty",
            "price",
            "qpc",
            "total",
            "num",
            "cls",
            "gdgid",
            "wrh",
        ],
    },
    "converts": {
        "dbo.skcmstkin": {
            "num": "str",
            "fildate": "str",
            "cls": "str",
            "vendor": "str",
        },
        "dbo.skcmstkindtl": {"num": "str", "cls": "str", "gdgid": "str", "wrh": "str"},
        "dbo.skcmvendor": {"gid": "str", "code": "str", "name": "str"},
        "dbo.modulestat": {"statname": "str"},
        "dbo.skgoods": {
            "code": "str",
            "brand": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "sort": "str",
            "name": "str",
        },
        "dbo.skcmbrand": {"code": "str", "name": "str"},
        "dbo.skcmwarehouse": {"code": "str", "name": "str", "gid": "str"},
        "dbo.skcmstkinbck": {
            "num": "str",
            "fildate": "str",
            "cls": "str",
            "vendor": "str",
        },
        "dbo.skcmstkinbckdtl": {
            "num": "str",
            "cls": "str",
            "gdgid": "str",
            "wrh": "str",
        },
    },
    
    """

    def purchase_warehouse(self):
        stkin = self.data["dbo.skcmstkin"]
        stkindtl = self.data["dbo.skcmstkindtl"]
        vendorh = self.data["dbo.skcmvendor"]
        modulestat = self.data["dbo.skcmmodulestat"]
        goods = self.data["dbo.skgoods"]
        brand = self.data["dbo.skcmbrand"]
        warehouseh = self.data["dbo.skcmwarehouse"]
        stkinbck = self.data["dbo.skcmstkinbck"]
        stkinbckdtl = self.data["dbo.skcmstkinbckdtl"]

        brand['name'] = brand['name'].str.strip()

        stkin['num'] = stkin['num'].str.strip()
        stkin['cls'] = stkin['cls'].str.strip()
        stkin['vendor'] = stkin['vendor'].str.strip()
        stkin['cls'] = stkin['cls'].str.strip()
        # stkin['stat'] = stkin['stat'].str.strip()

        modulestat['statname'] = modulestat['statname'].str.strip()

        vendorh['gid'] = vendorh['gid'].str.strip()

        stkindtl['num'] = stkindtl['num'].str.strip()
        stkindtl['cls'] = stkindtl['cls'].str.strip()
        stkindtl['gdgid'] = stkindtl['gdgid'].str.strip()
        stkindtl['wrh'] = stkindtl['wrh'].str.strip()

        goods['gid'] = goods['gid'].str.strip()
        goods['brand'] = goods['brand'].str.strip()
        warehouseh['gid'] = warehouseh['gid'].str.strip()

        brand['code'] = brand['code'].str.strip()

        stkinbck['num'] = stkinbck['num'].str.strip()
        stkinbck['num'] = stkinbck['num'].str.strip()
        stkinbck['cls'] = stkinbck['cls'].str.strip()
        stkinbck['vendor'] = stkinbck['vendor'].str.strip()
        stkinbck['cls'] = stkinbck['cls'].str.strip()
        # stkinbck['stat'] = stkinbck['stat'].str.strip()

        stkinbckdtl['num'] = stkinbckdtl['num'].str.strip()
        stkinbckdtl['cls'] = stkinbckdtl['cls'].str.strip()
        stkinbckdtl['gdgid'] = stkinbckdtl['gdgid'].str.strip()
        stkinbckdtl['wrh'] = stkinbckdtl['wrh'].str.strip()

        columns = [
            "source_id",
            "cmid",
            "purchase_num",
            "purchase_date",
            "purchase_type",
            "foreign_item_id",
            "item_show_code",
            "barcode",
            "item_name",
            "item_unit",
            "purchase_qty",
            "purchase_price",
            "purchase_total",
            "vendor_id",
            "vendor_show_code",
            "vendor_name",
            "brand_code",
            "brand_name",
            "warehouse_code",
            "warehouse_name",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
            "bill_status",
        ]

        part1 = (
            stkin.merge(
                stkindtl, how="left", on=["num", "cls"], suffixes=("", ".stkindtl")
            )
                .merge(
                vendorh,
                how="left",
                left_on=["vendor"],
                right_on=["gid"],
                suffixes=("", ".vendorh"),
            )
                .merge(
                modulestat,
                how="left",
                left_on=["stat"],
                right_on=["no"],
                suffixes=("", ".modulestat"),
            )
                .merge(
                goods,
                how="left",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                brand,
                how="left",
                left_on=["brand"],
                right_on=["code"],
                suffixes=("", ".brand"),
            )
                .merge(
                warehouseh,
                how="left",
                left_on=["wrh"],
                right_on=["gid"],
                suffixes=("", ".warehouseh"),
            )
        )
        part1["purchase_price"] = part1.apply(
            lambda row: row["price"] / row["qpc"], axis=1
        )
        part1["foreign_category_lv1"] = part1.apply(lambda row: row["sort"][:2], axis=1)
        part1["foreign_category_lv2"] = part1.apply(lambda row: row["sort"][:4], axis=1)
        part1["foreign_category_lv3"] = part1.apply(lambda row: row["sort"][:6], axis=1)
        part1["foreign_category_lv4"] = ""
        part1["foreign_category_lv5"] = ""
        part1["cmid"] = self.cmid
        part1["source_id"] = self.source_id
        part1 = part1.rename(
            columns={
                "num": "purchase_num",
                "fildate": "purchase_date",
                "cls": "purchase_type",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "qty": "purchase_qty",
                "total": "purchase_total",
                "gid": "vendor_id",
                "code": "vendor_show_code",
                "name": "vendor_name",
                "code.brand": "brand_code",
                "name.brand": "brand_name",
                "code.warehouseh": "warehouse_code",
                "name.warehouseh": "warehouse_name",
                "statname": "bill_status",
            }
        )
        part1 = part1[columns]

        part2 = (
            stkinbck.merge(
                stkinbckdtl,
                how="left",
                on=["num", "cls"],
                suffixes=("", ".stkinbckdtl"),
            )
                .merge(
                vendorh,
                how="left",
                left_on=["vendor"],
                right_on=["gid"],
                suffixes=("", ".vendorh"),
            )
                .merge(
                modulestat,
                how="left",
                left_on=["stat"],
                right_on=["no"],
                suffixes=("", ".modulestat"),
            )
                .merge(
                goods,
                how="left",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                brand,
                how="left",
                left_on=["brand"],
                right_on=["code"],
                suffixes=("", ".brand"),
            )
                .merge(
                warehouseh,
                how="left",
                left_on=["wrh"],
                right_on=["gid"],
                suffixes=("", ".warehouseh"),
            )
        )
        part2["purchase_qty"] = part2.apply(lambda row: -1 * row["qty"], axis=1)
        part2["purchase_total"] = part2.apply(lambda row: -1 * row["total"], axis=1)
        part2["purchase_price"] = part2.apply(
            lambda row: row["price"] / row["qpc"], axis=1
        )
        part2["foreign_category_lv1"] = part2.apply(lambda row: row["sort"][:2], axis=1)
        part2["foreign_category_lv2"] = part2.apply(lambda row: row["sort"][:4], axis=1)
        part2["foreign_category_lv3"] = part2.apply(lambda row: row["sort"][:6], axis=1)
        part2["foreign_category_lv4"] = ""
        part2["foreign_category_lv5"] = ""
        part2["cmid"] = self.cmid
        part2["source_id"] = self.source_id

        part2 = part2.rename(
            columns={
                "num": "purchase_num",
                "fildate": "purchase_date",
                "cls": "purchase_type",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "gid": "vendor_id",
                "code": "vendor_show_code",
                "name": "vendor_name",
                "code.brand": "brand_code",
                "name.brand": "brand_name",
                "code.warehouseh": "warehouse_code",
                "name.warehouseh": "warehouse_name",
                "statname": "bill_status",
            }
        )
        part2 = part2[columns]
        return pd.concat([part1, part2])

    """
    "origin_table_columns": {
        "dbo.skcmdiralc": ["cls", "fildate", "num", "receiver", "stat", "vendor"],
        "dbo.skcmdiralcdtl": ["cls", "gdgid", "num", "price", "qpc", "qty", "total"],
        "dbo.skcmvendor": ["gid", "code", "name"],
        "dbo.skstore": ["gid", "code", "name"],
        "dbo.skcmmodulestat": ["statname", "no"],
        "dbo.skgoods": ["brand", "code", "code2", "gid", "munit", "name", "sort"],
        "dbo.skcmbrand": ["code", "name"],
    },
    "converts": {
        "dbo.skcmdiralc": {
            "cls": "str",
            "num": "str",
            "fildate": "str",
            "receiver": "str",
            "vendor": "str",
        },
        "dbo.skcmdiralcdtl": {"cls": "str", "num": "str", "gdgid": "str"},
        "dbo.skcmvendor": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skstore": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skcmmodulestat": {"statname": "str"},
        "dbo.skgoods": {
            "code": "str",
            "brand": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "sort": "str",
            "name": "str",
        },
        "dbo.skcmbrand": {"code": "str", "name": "str"},
    },
    """

    def purchase_store(self):
        diralc = self.data["dbo.skcmdiralc"]
        diralcdtl = self.data["dbo.skcmdiralcdtl"]
        vendor = self.data["dbo.skcmvendor"]
        store = self.data["dbo.skstore"]
        modulestat = self.data["dbo.skcmmodulestat"]
        goods = self.data["dbo.skgoods"]
        brand = self.data["dbo.skcmbrand"]

        diralc['num'] = diralc['num'].str.strip()
        diralc['cls'] = diralc['cls'].str.strip()
        diralc['vendor'] = diralc['vendor'].str.strip()
        diralc['receiver'] = diralc['receiver'].str.strip()

        diralcdtl['num'] = diralcdtl['num'].str.strip()
        diralcdtl['cls'] = diralcdtl['cls'].str.strip()
        diralcdtl['gdgid'] = diralcdtl['gdgid'].str.strip()

        vendor['gid'] = vendor['gid'].str.strip()
        store['gid'] = store['gid'].str.strip()
        goods['gid'] = goods['gid'].str.strip()
        goods['brand'] = goods['brand'].str.strip()

        brand['code'] = brand['code'].str.strip()

        brand['name'] = brand['name'].str.strip()

        modulestat['statname'] = modulestat['statname'].str.strip()

        columns = [
            "source_id",
            "cmid",
            "purchase_num",
            "purchase_date",
            "purchase_type",
            "foreign_store_id",
            "store_show_code",
            "store_name",
            "foreign_item_id",
            "item_show_code",
            "barcode",
            "item_name",
            "item_unit",
            "purchase_qty",
            "purchase_price",
            "purchase_total",
            "vendor_id",
            "vendor_show_code",
            "vendor_name",
            "brand_code",
            "brand_name",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
            "bill_status",
        ]

        part = (
            diralc.merge(
                diralcdtl, how="left", on=["num", "cls"], suffixes=("", ".diralcdtl")
            )
                .merge(
                vendor,
                how="left",
                left_on=["vendor"],
                right_on=["gid"],
                suffixes=("", ".vendor"),
            )
                .merge(
                store,
                how="left",
                left_on=["receiver"],
                right_on=["gid"],
                suffixes=("", ".store"),
            )
                .merge(
                modulestat,
                how="left",
                left_on=["stat"],
                right_on=["no"],
                suffixes=("", ".modulestat"),
            )
                .merge(
                goods,
                how="left",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                brand,
                how="left",
                left_on=["brand"],
                right_on=["code"],
                suffixes=("", ".brand"),
            )
        )
        part["foreign_category_lv1"] = part.apply(lambda row: row["sort"][:2], axis=1)
        part["foreign_category_lv2"] = part.apply(lambda row: row["sort"][:4], axis=1)
        part["foreign_category_lv3"] = part.apply(lambda row: row["sort"][:6], axis=1)
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["purchase_qty"] = part.apply(
            lambda row: row["qty"] if row["cls"] == "直配出" else row["qty"] * -1, axis=1
        )
        part["purchase_price"] = part.apply(
            lambda row: row["price"] / row["qpc"], axis=1
        )
        part["purchase_total"] = part.apply(
            lambda row: row["total"] if row["cls"] == "直配出" else row["total"] * -1,
            axis=1,
        )
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id

        part = part.rename(
            columns={
                "num": "purchase_num",
                "fildate": "purchase_date",
                "cls": "purchase_type",
                "gid.store": "foreign_store_id",
                "code.store": "store_show_code",
                "name.store": "store_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "gid": "vendor_id",
                "code": "vendor_show_code",
                "name": "vendor_name",
                "code.brand": "brand_code",
                "name.brand": "brand_name",
                "statname": "bill_status",
            }
        )
        part = part[columns]
        return part

    """
    "origin_table_columns": {
        "dbo.skcminvxf": ["cls", "fildate", "fromstore", "num", "stat", "tostore"],
        "dbo.skcminvxfdtl": ["qty", "price", "total", "num", "cls", "gdgid"],
        "dbo.skstore": ["gid", "code", "name"],
        "dbo.skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "dbo.skcmmodulestat": ["statname", "no"],
    },
    "converts": {
        "dbo.skcminvxf": {
            "cls": "str",
            "fildate": "str",
            "fromstore": "str",
            "num": "str",
            "tostore": "str",
        },
        "dbo.skcminvxfdtl": {"num": "str", "cls": "str", "gdgid": "str"},
        "dbo.skstore": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "dbo.skcmmodulestat": {"statname": "str"},
    },
    """

    def move_store(self):
        invxf = self.data["dbo.skcminvxf"]
        invxfdtl = self.data["dbo.skcminvxfdtl"]
        store = self.data["dbo.skstore"]
        goods = self.data["dbo.skgoods"]
        modulestat = self.data["dbo.skcmmodulestat"]

        modulestat['statname'] = modulestat['statname'].str.strip()

        invxf['num'] = invxf['num'].str.strip()
        invxf['cls'] = invxf['cls'].str.strip()
        invxf['fromstore'] = invxf['fromstore'].str.strip()
        invxf['tostore'] = invxf['tostore'].str.strip()

        store['gid'] = store['gid'].str.strip()

        invxfdtl['num'] = invxfdtl['num'].str.strip()
        invxfdtl['cls'] = invxfdtl['cls'].str.strip()
        invxfdtl['gdgid'] = invxfdtl['gdgid'].str.strip()

        goods['gid'] = goods['gid'].str.strip()
        goods['sort'] = goods['sort'].str.strip()

        columns = [
            "source_id",
            "cmid",
            "move_num",
            "move_date",
            "move_type",
            "from_store_id",
            "from_store_show_code",
            "from_store_name",
            "to_store_id",
            "to_store_show_code",
            "to_store_name",
            "foreign_item_id",
            "item_show_code",
            "item_name",
            "move_qty",
            "price",
            "move_amount",
            "status",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
            "barcode",
            "item_unit",
        ]

        part = (
            invxf.merge(
                invxfdtl, how="inner", on=["num", "cls"], suffixes=("", ".invxfdtl")
            )
                .merge(
                store,
                how="inner",
                left_on=["fromstore"],
                right_on=["gid"],
                suffixes=("", ".from_store"),
            )
                .merge(
                store,
                how="inner",
                left_on=["tostore"],
                right_on=["gid"],
                suffixes=("", ".to_store"),
            )
                .merge(
                goods,
                how="inner",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                modulestat,
                how="left",
                left_on=["stat"],
                right_on=["no"],
                suffixes=("", ".modulestat"),
            )
        )
        part = part[part["cls"].isin(("门店调拨",))]
        part["foreign_category_lv1"] = part.apply(lambda row: row["sort"][:2], axis=1)
        part["foreign_category_lv2"] = part.apply(lambda row: row["sort"][:4], axis=1)
        part["foreign_category_lv3"] = part.apply(lambda row: row["sort"][:6], axis=1)
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id

        part = part.rename(
            columns={
                "num": "move_num",
                "fildate": "move_date",
                "cls": "move_type",
                "gid": "from_store_id",
                "code": "from_store_show_code",
                "name": "from_store_name",
                "gid.to_store": "to_store_id",
                "code.to_store": "to_store_show_code",
                "name.to_store": "to_store_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "name.goods": "item_name",
                "qty": "move_qty",
                "price": "price",
                "total": "move_amount",
                "statname": "status",
                "code2": "barcode",
                "munit": "item_unit",
            }
        )
        part = part[columns]
        return part

    """
    "origin_table_columns": {
        "dbo.skcminvxf": ["cls", "fildate", "fromwrh", "num", "stat", "towrh"],
        "dbo.skcminvxfdtl": ["qty", "price", "total", "num", "cls", "gdgid"],
        "dbo.skcmwarehouse": ["gid", "code", "name"],
        "dbo.skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "dbo.skcmmodulestat": ["statname", "no"],
    },
    "converts": {
        "dbo.skcminvxf": {
            "cls": "str",
            "fildate": "str",
            "fromwrh": "str",
            "num": "str",
            "towrh": "str",
        },
        "dbo.skcminvxfdtl": {"num": "str", "cls": "str", "gdgid": "str"},
        "dbo.skcmwarehouse": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "dbo.skcmmodulestat": {"statname": "str"},
    },
    """

    def move_warehouse(self):
        invxf = self.data["dbo.skcminvxf"]
        invxfdtl = self.data["dbo.skcminvxfdtl"]
        warehouse = self.data["dbo.skcmwarehouse"]
        goods = self.data["dbo.skgoods"]
        modulestat = self.data["dbo.skcmmodulestat"]

        modulestat['statname'] = modulestat['statname'].str.strip()

        invxf['num'] = invxf['num'].str.strip()
        invxf['cls'] = invxf['cls'].str.strip()
        invxf['fromwrh'] = invxf['fromwrh'].str.strip()
        invxf['towrh'] = invxf['towrh'].str.strip()

        warehouse['gid'] = warehouse['gid'].str.strip()

        invxfdtl['num'] = invxfdtl['num'].str.strip()
        invxfdtl['cls'] = invxfdtl['cls'].str.strip()
        invxfdtl['gdgid'] = invxfdtl['gdgid'].str.strip()

        goods['gid'] = goods['gid'].str.strip()

        columns = [
            "source_id",
            "cmid",
            "move_num",
            "move_date",
            "move_type",
            "from_warehouse_id",
            "from_warehouse_show_code",
            "from_warehouse_name",
            "to_warehouse_id",
            "to_warehouse_show_code",
            "to_warehouse_name",
            "foreign_item_id",
            "item_show_code",
            "item_name",
            "move_qty",
            "price",
            "move_amount",
            "status",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
            "barcode",
            "item_unit",
        ]

        part = (
            invxf.merge(
                invxfdtl, how="inner", on=["num", "cls"], suffixes=("", ".invxfdtl")
            )
                .merge(
                warehouse,
                how="inner",
                left_on=["fromwrh"],
                right_on=["gid"],
                suffixes=("", ".from_warehouse"),
            )
                .merge(
                warehouse,
                how="inner",
                left_on=["towrh"],
                right_on=["gid"],
                suffixes=("", ".to_warehouse"),
            )
                .merge(
                goods,
                how="inner",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
                .merge(
                modulestat,
                how="left",
                left_on=["stat"],
                right_on=["no"],
                suffixes=("", ".modulestat"),
            )
        )
        part = part[part["cls"].isin(("仓库调拨",))]
        part["foreign_category_lv1"] = part.apply(lambda row: row["sort"][:2], axis=1)
        part["foreign_category_lv2"] = part.apply(lambda row: row["sort"][:4], axis=1)
        part["foreign_category_lv3"] = part.apply(lambda row: row["sort"][:6], axis=1)
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id
        part = part.rename(
            columns={
                "num": "move_num",
                "fildate": "move_date",
                "cls": "move_type",
                "gid": "from_warehouse_id",
                "code": "from_warehouse_show_code",
                "name": "from_warehouse_name",
                "gid.to_warehouse": "to_warehouse_id",
                "code.to_warehouse": "to_warehouse_show_code",
                "name.to_warehouse": "to_warehouse_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "name.goods": "item_name",
                "qty": "move_qty",
                "price": "price",
                "total": "move_amount",
                "statname": "status",
                "code2": "barcode",
                "munit": "item_unit",
            }
        )
        part = part[columns]
        return part

    """
    "origin_table_columns": {
        "dbo.skcmckdatas": [
            "rtlbal",
            "acntqty",
            "cktime",
            "gdgid",
            "num",
            "qty",
            "stat",
            "store",
        ],
        "dbo.skstore": ["gid", "code", "name"],
        "dbo.skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "dbo.skcmsort": ["code"],
    },
    "converts": {
        "dbo.skcmckdatas": {"cktime": "str", "gdgid": "str", "num": "str", "store": "str"},
        "dbo.skstore": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "dbo.skcmsort": {"code": "str"},
    },
    """

    def goods_loss(self):
        ckdatas = self.data["dbo.skcmckdatas"]
        store = self.data["dbo.skstore"]
        goods = self.data["dbo.skgoods"]
        sort = self.data["dbo.skcmsort"]

        ckdatas['store'] = ckdatas['store'].str.strip()
        ckdatas['gdgid'] = ckdatas['gdgid'].str.strip()
        ckdatas['num'] = ckdatas['num'].str.strip()

        store['gid'] = store['gid'].str.strip()
        goods['gid'] = goods['gid'].str.strip()
        goods['sort'] = goods['sort'].str.strip()
        goods['munit'] = goods['munit'].str.strip()

        sort['code'] = sort['code'].str.strip()

        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

        columns = [
            "cmid",
            "source_id",
            "lossnum",
            "lossdate",
            "foreign_store_id",
            "store_show_code",
            "store_name",
            "foreign_item_id",
            "item_showcode",
            "barcode",
            "item_name",
            "item_unit",
            "quantity",
            "subtotal",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
        ]

        part = (
            ckdatas.merge(
                store,
                how="left",
                left_on=["store"],
                right_on=["gid"],
                suffixes=("", ".store"),
            )
            .merge(
                goods,
                how="left",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
            .merge(
                sort,
                how="left",
                left_on=["sort1"],
                right_on=["code"],
                suffixes=("", ".sort1"),
            )
            .merge(
                sort,
                how="left",
                left_on=["sort2"],
                right_on=["code"],
                suffixes=("", ".sort2"),
            )
            .merge(
                sort,
                how="left",
                left_on=["sort3"],
                right_on=["code"],
                suffixes=("", ".sort3"),
            )
        )
        part = part[(part["qty"] < part["acntqty"]) & (part["stat"] == 3)]
        part["quantity"] = part.apply(lambda row: row["qty"] - row["acntqty"], axis=1)
        part["source_id"] = self.source_id
        part["cmid"] = self.cmid
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["lossdate"] = part.apply(lambda row: row["cktime"].split()[0], axis=1)

        part = part.rename(
            columns={
                "num": "lossnum",
                "gid": "foreign_store_id",
                "code": "store_show_code",
                "name": "store_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_showcode",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "rtlbal": "subtotal",
                "code.sort1": "foreign_category_lv1",
                "code.sort2": "foreign_category_lv2",
                "code.sort3": "foreign_category_lv3",
            }
        )

        part = part[columns]
        return part

    """
    "origin_table_columns": {
        "dbo.skcmckdatas": [
            "rtlbal",
            "acntqty",
            "cktime",
            "gdgid",
            "num",
            "qty",
            "stat",
            "wrh",
        ],
        "dbo.skcmwarehouse": ["gid", "code", "name"],
        "dbo.skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "dbo.skcmsort": ["code"],
    },
    "converts": {
        "dbo.skcmckdatas": {"cktime": "str", "gdgid": "str", "num": "str", "wrh": "str"},
        "dbo.skcmwarehouse": {"gid": "str", "code": "str", "name": "str"},
        "dbo.skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "dbo.skcmsort": {"code": "str"},
    },
    """
    
    def check_warehouse(self):
        ckdatas = self.data["dbo.skcmckdatas"]
        warehouse = self.data["dbo.skcmwarehouse"]
        goods = self.data["dbo.skgoods"]
        sort = self.data["dbo.skcmsort"]

        goods['sort'] = goods['sort'].str.strip()

        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

        ckdatas['num'] = ckdatas['num'].str.strip()
        ckdatas['wrh'] = ckdatas['wrh'].str.strip()
        ckdatas['gdgid'] = ckdatas['gdgid'].str.strip()

        goods['gid'] = goods['gid'].str.strip()
        goods['sort'] = goods['sort'].str.strip()
        goods['munit'] = goods['munit'].str.strip()
        warehouse['gid'] = warehouse['gid'].str.strip()

        sort['code'] = sort['code'].str.strip()

        columns = [
            "cmid",
            "source_id",
            "check_num",
            "check_date",
            "foreign_warehouse_id",
            "warehouse_show_code",
            "warehouse_name",
            "foreign_item_id",
            "item_show_code",
            "barcode",
            "item_name",
            "item_unit",
            "quantity",
            "subtotal",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
        ]

        part = (
            ckdatas.merge(
                warehouse,
                how="left",
                left_on=["wrh"],
                right_on=["gid"],
                suffixes=("", ".warehouse"),
            )
            .merge(
                goods,
                how="left",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            )
            .merge(
                sort,
                how="left",
                left_on=["sort1"],
                right_on=["code"],
                suffixes=("", ".sort1"),
            )
            .merge(
                sort,
                how="left",
                left_on=["sort2"],
                right_on=["code"],
                suffixes=("", ".sort2"),
            )
            .merge(
                sort,
                how="left",
                left_on=["sort3"],
                right_on=["code"],
                suffixes=("", ".sort3"),
            )
        )

        part = part[part["stat"] == 3]
        part["quantity"] = part.apply(lambda row: row["qty"] - row["acntqty"], axis=1)
        part["source_id"] = self.source_id
        part["cmid"] = self.cmid
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["check_date"] = part.apply(lambda row: row["cktime"].split()[0], axis=1)

        part = part.rename(
            columns={
                "num": "check_num",
                "gid": "foreign_warehouse_id",
                "code": "warehouse_show_code",
                "name": "warehouse_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_show_code",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "rtlbal": "subtotal",
                "code.sort1": "foreign_category_lv1",
                "code.sort2": "foreign_category_lv2",
                "code.sort3": "foreign_category_lv3",
            }
        )
        part = part[columns]
        return part

