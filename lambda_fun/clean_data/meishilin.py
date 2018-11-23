# -*- coding: utf-8 -*-
# @Time    : 2018/8/27 10:38
# @Author  : 范佳楠
from datetime import datetime

import pandas as pd

from typing import Dict
from base import Base
import pytz

_TZINFO = pytz.timezone("Asia/Shanghai")


class MeiShiLinCleaner(Base):
    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        Base.__init__(self, source_id, date, data)

    """
    "origin_table_columns": {
            "skstoresellingwater": ["sgid", "gid", "flowno", "rtlprc", "qty", "realamt", "fildate"],
            "skstore": ["gid", "name"],
            "skgoods": ["gid", "code2", "name", "munit"],
            "skgoodssort": ["gid", "ascode", "asname", "bscode", "bsname", "cscode", "csname"]
        },

        "converts": {
            "skstoresellingwater": {"sgid": "str", "gid": "str",
                                        "flowno": "str", "rtlprc": "float",
                                        "qty": "float", "realamt": "float"},
            "skstore": {"gid": "str", "name": "str"},
            "skgoods": {"gid": "str", "code2": "str", "name": "str", "munit": "str"},
            "skgoodssort": {"gid": "str", "ascode": "str", "asname": "str",
            "bscode": "str", "bsname": "str",
                                "cscode": "str", "csname": "str"}
        }
    """

    def goodsflow(self):
        columns = [
            "source_id",
            "cmid",
            "foreign_store_id",
            "store_name",
            "receipt_id",
            "consumer_id",
            "saletime",
            "last_updated",
            "foreign_item_id",
            "barcode",
            "item_name",
            "item_unit",
            "saleprice",
            "quantity",
            "subtotal",
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
            "foreign_category_lv5",
            "foreign_category_lv5_name",
            "pos_id",
        ]
        flow_frame = self.data["skstoresellingwater"]

        if not len(flow_frame):
            return pd.DataFrame(columns=columns)

        store_frame = self.data["skstore"]
        goods_frame = self.data["skgoods"]
        gsort_frame = self.data["skgoodssort"]
        result_frame = (
            pd.merge(
                flow_frame,
                store_frame,
                left_on="sgid",
                right_on="gid",
                how="left",
                suffixes=("_flow", "_store"),
            )
            .merge(
                goods_frame,
                left_on="gid_flow",
                right_on="gid",
                how="left",
                suffixes=("_store", "_goods"),
            )
            .merge(gsort_frame, left_on="gid", right_on="gid", how="left")
        )

        result_frame["source_id"] = self.source_id
        result_frame["cmid"] = self.cmid
        result_frame["last_updated"] = datetime.now(_TZINFO)
        result_frame["foreign_category_lv4"] = ""
        result_frame["foreign_category_lv4_name"] = None
        result_frame["foreign_category_lv5"] = ""
        result_frame["foreign_category_lv5_name"] = None
        result_frame["pos_id"] = ""
        result_frame["consumer_id"] = None

        result_frame = result_frame.rename(
            columns={
                "gid_store": "foreign_store_id",
                "name_store": "store_name",
                "flowno": "receipt_id",
                "fildate": "saletime",
                "gid": "foreign_item_id",
                "code2": "barcode",
                "name_goods": "item_name",
                "munit": "item_unit",
                "rtlprc": "saleprice",
                "qty": "quantity",
                "realamt": "subtotal",
                "ascode": "foreign_category_lv1",
                "asname": "foreign_category_lv1_name",
                "bscode": "foreign_category_lv2",
                "bsname": "foreign_category_lv2_name",
                "cscode": "foreign_category_lv3",
                "csname": "foreign_category_lv3_name",
            }
        )

        result_frame = result_frame[columns]

        return result_frame

    """
        "origin_table_columns": {
                "skcmsale": ["pdkey",
                                 "orgkey",
                                 "fildate",
                                 "saleqty",
                                 "saleamt",
                                 "saletax",
                                 "salecamt",
                                 "salectax",

                                 ],
                "skgoodssort":["gid", "ascode", "bscode", "cscode"],
            },

        "converts": {
            "skcmsale": {"pdkey":"str",
                             "saleqty":"float",
                             "saleamt":"float",
                             "saletax":"float",
                             "salecamt":"float",
                             "salectax":"float",
                             },
            "skgoodssort": {"gid":"str"}
        }
        """

    def cost(self):
        columns = [
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
        ]
        cost_frame = self.data["skcmsale"]

        if not len(cost_frame):
            return pd.DataFrame(columns=columns)

        gsort_frame = self.data["skgoodssort"]

        result_frame = pd.merge(
            cost_frame, gsort_frame, left_on="pdkey", right_on="gid", how="left"
        )
        result_frame["source_id"] = self.source_id
        result_frame["cost_type"] = ""
        result_frame["foreign_category_lv4"] = ""
        result_frame["foreign_category_lv5"] = ""
        result_frame["cmid"] = self.cmid

        result_frame["total_sale"] = result_frame.apply(
            lambda row: row["saleamt"] + row["saletax"], axis=1
        )
        result_frame["total_cost"] = result_frame.apply(
            lambda row: row["salecamt"] + row["salectax"], axis=1
        )

        result_frame = result_frame.rename(
            columns={
                "orgkey": "foreign_store_id",
                "pdkey": "foreign_item_id",
                "fildate": "date",
                "saleqty": "total_quantity",
                "ascode": "foreign_category_lv1",
                "bscode": "foreign_category_lv2",
                "cscode": "foreign_category_lv3",
            }
        )

        result_frame = result_frame[columns]

        return result_frame

    """
    "origin_table_columns": {
        "skstore": [
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
        "skcmarea": ["code", "name"],
    },
    "converts": {
        "skstore": {
            "gid": "str",
            "name": "str",
            "address": "str",
            "code": "str",
            "area": "str",
            "contactor": "str",
            "phone": "str",
        },
        "skcmarea": {"code": "str", "name": "str"},
    },
    """

    def store(self):
        store = self.data["skstore"]
        area = self.data["skcmarea"]

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
        part["last_updated"] = datetime.now(_TZINFO)
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
        part["business_area"] = ""

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
        "skgoods": [
            "alc",
            "brand",
            "code",
            "code2",
            "gid",
            "munit",
            "name",
            "rtlprc",
            "sort",
            "validperiod",
            "vdrgid",
            "lifecycle"
        ],
        "skcmbrand": ["name", "code", "gid"],
        "skcmvendor": ["name", "code", "gid"],
    },
    "converts": {
        "skgoods": {
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
            "lifecycle":"str"
        },
        "skcmbrand": {"name": "str", "code": "str", "gid":"str"},
        "skcmvendor": {"gid": "str", "code": "str", "name": "str"},
    },
    """

    def goods(self):
        goods = self.data["skgoods"]
        # goodsbusgate = self.data["goodsbusgate"]
        brand = self.data["skcmbrand"]
        vendor = self.data["skcmvendor"]

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
        part["storage_time"] = datetime.now(_TZINFO)
        part["last_updated"] = datetime.now(_TZINFO)
        # part["isvalid"] = 1
        part["lastin_price"] = None

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
                "alword": "isvalid",
            }
        )
        part = part[columns]

        part["supplier_name"] = part["supplier_name"].str.strip()
        part["supplier_code"] = part["supplier_code"].str.strip()
        part["brand_name"] = part["brand_name"].str.strip()

        return part

    """
        "origin_table_columns": {"skcmsort": ["code", "name"]},
        "converts": {"skcmsort": {"code": "str", "name": "str"}},
    """

    def category(self):

        sort = self.data["skcmsort"]

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
        part1["last_updated"] = datetime.now(_TZINFO)

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
        part2["last_updated"] = datetime.now(_TZINFO)

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
        part3["last_updated"] = datetime.now(_TZINFO)

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
        "skcmotrequireorder": [
            "billnumber",
            "billtype",
            "buyercode",
            "checktime",
            "state",
            "uuid",
        ],
        "skcmotrequireorderline": [
            "munit",
            "checkedqty",
            "price",
            "checkedtotal",
            "bill",
            "product",
        ],
        "skstore": ["gid", "code", "name"],
        "skgoods": ["code", "code2", "gid", "name", "psr", "sort", "vdrgid"],
        "skcmsort": ["code"],
        "skcmvendor": ["gid", "code", "name"],
        "skcmemployee": ["name", "gid"],
    },
    "converts": {
        "skcmotrequireorder": {
            "billnumber": "str",
            "billtype": "str",
            "buyercode": "str",
            "checktime": "str",
            "uuid": "str",
        },
        "skcmotrequireorderline": {"munit": "str", "bill": "str", "product": "str"},
        "skstore": {"gid": "str", "code": "str", "name": "str"},
        "skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "name": "str",
            "psr": "str",
            "sort": "str",
            "vdrgid": "str",
        },
        "skcmsort": {"code": "str"},
        "skcmvendor": {"gid": "str", "code": "str", "name": "str"},
        "skcmemployee": {"name": "str", "gid": "str"},
    },

    """

    def requireorder(self):
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
        otrequireorder = self.data["skcmotrequireorder"]

        if not len(otrequireorder):
            return pd.DataFrame(columns=columns)

        otrequireorderline = self.data["skcmotrequireorderline"]
        store = self.data["skstore"]
        goods = self.data["skgoods"]
        sort = self.data["skcmsort"]
        vendor = self.data["skcmvendor"]
        employee = self.data["skcmemployee"]

        # otrequireorder = otrequireorder.drop_duplicates()
        # otrequireorderline = otrequireorderline.drop_duplicates()
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

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
            "skcmstkout": ["billto", "cls", "num", "ocrdate", "stat"],
            "skcmstkoutdtl": ["alcsrc", "cls", "gdgid", "munit", "num", "qty", "wrh"],
            "skstore": ["gid", "name", "code"],
            "skcmwarehouse": ["gid", "name", "code"],
            "skgoods": ["code", "code2", "gid", "name", "rtlprc", "sort"],
            "skcmsort": ["code"],
            "skcmstkoutbck": ["billto", "cls", "num", "ocrdate", "stat"],
            "skcmstkoutbckdtl": ["bckcls", "cls", "gdgid", "munit", "num", "qty", "wrh"],
        },
        "converts": {
            "skcmstkout": {
                "billto": "str",
                "cls": "str",
                "num": "str",
                "ocrdate": "str",
                "stat": "str",
            },
            "skcmstkoutdtl": {
                "cls": "str",
                "gdgid": "str",
                "munit": "str",
                "num": "str",
                "wrh": "str",
                # "qty":"float"
            },
            "skstore": {"gid": "str", "name": "str", "code": "str"},
            "skcmwarehouse": {"gid": "str", "name": "str", "code": "str"},
            "skgoods": {
                "code": "str",
                "code2": "str",
                "gid": "str",
                "name": "str",
                "sort": "str",
                # "rtlprc":"float"
            },
            "skcmsort": {"code": "str"},
            "skcmstkoutbck": {
                "billto": "str",
                "cls": "str",
                "num": "str",
                "ocrdate": "str",
                "stat": "str",
            },
            "skcmstkoutbckdtl": {
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
        stkout = self.data["skcmstkout"]

        if not len(stkout):
            return pd.DataFrame(columns=columns)

        stkoutdtl = self.data["skcmstkoutdtl"]
        store = self.data["skstore"]
        warehouse = self.data["skcmwarehouse"]
        goods = self.data["skgoods"]
        sort = self.data["skcmsort"]
        stkoutbck = self.data["skcmstkoutbck"]
        stkoutbckdtl = self.data["skcmstkoutbckdtl"]

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

        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["foreign_category_lv4"] = ""
            part1["foreign_category_lv5"] = ""
            part1["cmid"] = self.cmid
            part1["source_id"] = self.source_id
            part1["rtl_amt"] = part1.apply(
                lambda row: row["rtlprc"] * row["qty"], axis=1
            )
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

        stkoutbck: pd.DataFrame = stkoutbck[
            (stkoutbck["cls"] == "统配出退")
            & (stkoutbck["stat"].isin(("0", "100", "300", "700", "1000")))
        ]
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
        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
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
        "skcmstkin": ["num", "fildate", "cls", "vendor", "stat"],
        "skcmstkindtl": [
            "qty",
            "price",
            "qpc",
            "total",
            "num",
            "cls",
            "gdgid",
            "wrh",
        ],
        "skcmvendor": ["gid", "code", "name"],
        "skcmmodulestat": ["statname", "no"],
        "skgoods": ["brand", "code", "code2", "gid", "munit", "name", "sort"],
        "skcmbrand": ["code", "name"],
        "skcmwarehouse": ["code", "name", "gid"],
        "skcmstkinbck": ["num", "fildate", "cls", "vendor", "stat"],
        "skcmstkinbckdtl": [
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
        "skcmstkin": {
            "num": "str",
            "fildate": "str",
            "cls": "str",
            "vendor": "str",
        },
        "skcmstkindtl": {"num": "str", "cls": "str", "gdgid": "str", "wrh": "str"},
        "skcmvendor": {"gid": "str", "code": "str", "name": "str"},
        "modulestat": {"statname": "str"},
        "skgoods": {
            "code": "str",
            "brand": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "sort": "str",
            "name": "str",
        },
        "skcmbrand": {"code": "str", "name": "str"},
        "skcmwarehouse": {"code": "str", "name": "str", "gid": "str"},
        "skcmstkinbck": {
            "num": "str",
            "fildate": "str",
            "cls": "str",
            "vendor": "str",
        },
        "skcmstkinbckdtl": {
            "num": "str",
            "cls": "str",
            "gdgid": "str",
            "wrh": "str",
        },
    },

    """

    def purchase_warehouse(self):
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
        stkin = self.data["skcmstkin"]

        if not len(stkin):
            return pd.DataFrame(columns=columns)

        stkindtl = self.data["skcmstkindtl"]
        vendorh = self.data["skcmvendor"]
        modulestat = self.data["skcmmodulestat"]
        goods = self.data["skgoods"]
        brand = self.data["skcmbrand"]
        warehouseh = self.data["skcmwarehouse"]
        stkinbck = self.data["skcmstkinbck"]
        stkinbckdtl = self.data["skcmstkinbckdtl"]

        part1 = stkin.merge(
            stkindtl, how="left", on=["num", "cls"], suffixes=("", ".stkindtl")
        )

        part1 = part1.merge(
            vendorh,
            how="left",
            left_on=["vendor"],
            right_on=["gid"],
            suffixes=("", ".vendorh"),
        )

        part1 = part1.merge(
            modulestat,
            how="left",
            left_on=["stat"],
            right_on=["no"],
            suffixes=("", ".modulestat"),
        )

        part1 = part1.merge(
            goods,
            how="left",
            left_on=["gdgid"],
            right_on=["gid"],
            suffixes=("", ".goods"),
        )

        part1 = part1.merge(
            brand,
            how="left",
            left_on=["brand"],
            right_on=["code"],
            suffixes=("", ".brand"),
        )

        part1 = part1.merge(
            warehouseh,
            how="left",
            left_on=["wrh"],
            right_on=["gid"],
            suffixes=("", ".warehouseh"),
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
        "skcmdiralc": ["cls", "fildate", "num", "receiver", "stat", "vendor"],
        "skcmdiralcdtl": ["cls", "gdgid", "num", "price", "qpc", "qty", "total"],
        "skcmvendor": ["gid", "code", "name"],
        "skstore": ["gid", "code", "name"],
        "skcmmodulestat": ["statname", "no"],
        "skgoods": ["brand", "code", "code2", "gid", "munit", "name", "sort"],
        "skcmbrand": ["code", "name"],
    },
    "converts": {
        "skcmdiralc": {
            "cls": "str",
            "num": "str",
            "fildate": "str",
            "receiver": "str",
            "vendor": "str",
        },
        "skcmdiralcdtl": {"cls": "str", "num": "str", "gdgid": "str"},
        "skcmvendor": {"gid": "str", "code": "str", "name": "str"},
        "skstore": {"gid": "str", "code": "str", "name": "str"},
        "skcmmodulestat": {"statname": "str"},
        "skgoods": {
            "code": "str",
            "brand": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "sort": "str",
            "name": "str",
        },
        "skcmbrand": {"code": "str", "name": "str"},
    },
    """

    def purchase_store(self):
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

        diralc = self.data["skcmdiralc"]

        if not len(diralc):
            return pd.DataFrame(columns=columns)

        diralcdtl = self.data["skcmdiralcdtl"]
        vendor = self.data["skcmvendor"]
        store = self.data["skstore"]
        modulestat = self.data["skcmmodulestat"]
        goods = self.data["skgoods"]
        brand = self.data["skcmbrand"]

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
        "skcminvxf": ["cls", "fildate", "fromstore", "num", "stat", "tostore"],
        "skcminvxfdtl": ["qty", "price", "total", "num", "cls", "gdgid"],
        "skstore": ["gid", "code", "name"],
        "skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "skcmmodulestat": ["statname", "no"],
    },
    "converts": {
        "skcminvxf": {
            "cls": "str",
            "fildate": "str",
            "fromstore": "str",
            "num": "str",
            "tostore": "str",
        },
        "skcminvxfdtl": {"num": "str", "cls": "str", "gdgid": "str"},
        "skstore": {"gid": "str", "code": "str", "name": "str"},
        "skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "skcmmodulestat": {"statname": "str"},
    },
    """

    def move_store(self):
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
        invxf = self.data["skcminvxf"]

        if not len(invxf):
            return pd.DataFrame(columns=columns)

        invxfdtl = self.data["skcminvxfdtl"]
        store = self.data["skstore"]
        goods = self.data["skgoods"]
        modulestat = self.data["skcmmodulestat"]

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
        "skcminvxf": ["cls", "fildate", "fromwrh", "num", "stat", "towrh"],
        "skcminvxfdtl": ["qty", "price", "total", "num", "cls", "gdgid"],
        "skcmwarehouse": ["gid", "code", "name"],
        "skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "skcmmodulestat": ["statname", "no"],
    },
    "converts": {
        "skcminvxf": {
            "cls": "str",
            "fildate": "str",
            "fromwrh": "str",
            "num": "str",
            "towrh": "str",
        },
        "skcminvxfdtl": {"num": "str", "cls": "str", "gdgid": "str"},
        "skcmwarehouse": {"gid": "str", "code": "str", "name": "str"},
        "skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "skcmmodulestat": {"statname": "str"},
    },
    """

    def move_warehouse(self):
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

        invxf = self.data["skcminvxf"]

        if not len(invxf):
            return pd.DataFrame(columns=columns)

        invxfdtl = self.data["skcminvxfdtl"]
        warehouse = self.data["skcmwarehouse"]
        goods = self.data["skgoods"]
        modulestat = self.data["skcmmodulestat"]

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
        if not len(part):
            return pd.DataFrame(columns=columns)
        part["foreign_category_lv1"] = part.apply(lambda row: row["sort"][:2], axis=1)
        part["foreign_category_lv2"] = part.apply(lambda row: row["sort"][:4], axis=1)
        part["foreign_category_lv3"] = part.apply(lambda row: row["sort"][:6], axis=1)
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id
        part = part[part["cls"].isin(("仓库调拨",))]

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
        "skcmckdatas": [
            "rtlbal",
            "acntqty",
            "cktime",
            "gdgid",
            "num",
            "qty",
            "stat",
            "store",
        ],
        "skstore": ["gid", "code", "name"],
        "skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "skcmsort": ["code"],
    },
    "converts": {
        "skcmckdatas": {"cktime": "str", "gdgid": "str", "num": "str", "store": "str"},
        "skstore": {"gid": "str", "code": "str", "name": "str"},
        "skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "skcmsort": {"code": "str"},
    },
    """

    def goods_loss(self):
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
        ckdatas = self.data["skcmckdatas"]

        if not len(ckdatas):
            return pd.DataFrame(columns=columns)

        store = self.data["skstore"]
        goods = self.data["skgoods"]
        sort = self.data["skcmsort"]

        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

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

        if not len(part):
            return pd.DataFrame(columns=columns)

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
        "skcmckdatas": [
            "rtlbal",
            "acntqty",
            "cktime",
            "gdgid",
            "num",
            "qty",
            "stat",
            "wrh",
        ],
        "skcmwarehouse": ["gid", "code", "name"],
        "skgoods": ["code", "code2", "gid", "munit", "name", "sort"],
        "skcmsort": ["code"],
    },
    "converts": {
        "skcmckdatas": {"cktime": "str", "gdgid": "str", "num": "str", "wrh": "str"},
        "skcmwarehouse": {"gid": "str", "code": "str", "name": "str"},
        "skgoods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "skcmsort": {"code": "str"},
    },
    """

    def check_warehouse(self):
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
        ckdatas = self.data["skcmckdatas"]

        if not len(ckdatas):
            return pd.DataFrame(columns=columns)

        warehouse = self.data["skcmwarehouse"]
        goods = self.data["skgoods"]
        sort = self.data["skcmsort"]

        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

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
