# -*- coding: utf-8 -*-
# @Time    : 2018/8/29 16:06
# @Author  : 范佳楠

from datetime import datetime


import pandas as pd
import pytz
from typing import Dict
from base import Base

_TZINFO = pytz.timezone("Asia/Shanghai")


class ZhiBaiWeiCleaner(Base):
    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        Base.__init__(self, source_id, date, data)

    """
        "origin_table_columns": {
            "pos_t_saleflow": ['branch_no',
                                   'item_no',
                                   'flow_no',
                                   'oper_date',
                                   'sell_way',
                                   'sale_price',
                                   'sale_qnty',
                                   'sale_money',
                                   ],
            "bi_t_branch_info": ['branch_no', 'branch_name'],
            "bi_t_item_info": ['item_no', 'item_clsno', 'barcode', 'item_name', 'unit_no'],
            "bi_t_item_cls": ['item_clsno', 'item_clsname', 'item_flag']
        },

        "converts": {
            "pos_t_saleflow": {'branch_no': 'str',
                                   'item_no': 'str',
                                   'sale_price': 'float',
                                   'sale_qnty': 'float',
                                   'sell_way': 'str',
                                   'sale_money': 'float',
                                   'flow_no': 'str'},
            "bi_t_branch_info": {'branch_no': 'str'},
            "bi_t_item_info": {'item_no': 'str', 'item_clsno': 'str'},
            "bi_t_item_cls": {'item_clsno': 'str', 'item_flag': 'str'}
        }
    """

    def goodsflow(self):
        filter_columns = [
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
        rename_columns_dict = {
            "branch_no": "foreign_store_id",
            "branch_name": "store_name",
            "flow_no": "receipt_id",
            "oper_date": "saletime",
            "item_no": "foreign_item_id",
            "item_name": "item_name",
            "unit_no": "item_unit",
            "item_clsno_lv1": "foreign_category_lv1",
            "item_clsname": "foreign_category_lv1_name",
            "item_clsno_lv2": "foreign_category_lv2",
            "item_clsname_lv2": "foreign_category_lv2_name",
            "item_clsno_lv3": "foreign_category_lv3",
            "item_clsname_lv3": "foreign_category_lv3_name",
        }

        flow = self.data["pos_t_saleflow"]
        store = self.data["bi_t_branch_info"]
        item = self.data["bi_t_item_info"]
        item_cls = self.data["bi_t_item_cls"]

        flow["branch_no"] = flow.apply(lambda row: row["branch_no"][:2], axis=1)

        item["item_clsno_1"] = item.apply(lambda row: row["item_clsno"][:2], axis=1)
        item["item_clsno_2"] = item.apply(lambda row: row["item_clsno"][:4], axis=1)
        item["item_clsno_3"] = item.apply(lambda row: row["item_clsno"][:6], axis=1)

        part1 = (
            (
                pd.merge(
                    flow, store, left_on="branch_no", right_on="branch_no", how="left"
                )
            )
            .merge(item, left_on="item_no", right_on="item_no", how="left")
            .merge(
                item_cls,
                left_on="item_clsno_1",
                right_on="item_clsno",
                how="left",
                suffixes=("", "_lv1"),
            )
            .merge(
                item_cls,
                left_on="item_clsno_2",
                right_on="item_clsno",
                how="left",
                suffixes=("", "_lv2"),
            )
            .merge(
                item_cls,
                left_on="item_clsno_3",
                right_on="item_clsno",
                how="left",
                suffixes=("", "_lv3"),
            )
        )

        part1 = part1[part1["item_clsno"] != "00"]

        def saleprice_convert(row):
            if row["sell_way"] == "C":
                return 0
            else:
                return row["sale_price"]

        def quantity_convert(row):
            if row["sell_way"] == "A" or row["sell_way"] == "C":
                return row["sale_qnty"]
            else:
                return -1 * row["sale_qnty"]

        def subtotal(row):
            if row["sell_way"] == "A":
                return row["sale_money"]
            elif row["sell_way"] == "B":
                return -1 * row["sale_money"]
            elif row["sell_way"] == "C":
                return 0
            else:
                pass

        if len(part1) == 0:
            part1 = pd.DataFrame(columns=filter_columns)
        else:
            part1["saleprice"] = part1.apply(saleprice_convert, axis=1)
            part1["quantity"] = part1.apply(quantity_convert, axis=1)
            part1["subtotal"] = part1.apply(subtotal, axis=1)
            part1["source_id"] = self.source_id
            part1["cmid"] = self.cmid
            part1["consumer_id"] = ""
            part1["last_updated"] = datetime.now(_TZINFO)
            part1["foreign_category_lv4"] = ""
            part1["foreign_category_lv4_name"] = None
            part1["foreign_category_lv5"] = ""
            part1["foreign_category_lv5_name"] = None
            part1["pos_id"] = ""

            part1 = part1.rename(columns=rename_columns_dict)

            part1 = part1[filter_columns]

        part2 = (
            (
                pd.merge(
                    flow, store, left_on="branch_no", right_on="branch_no", how="left"
                )
            )
            .merge(item, left_on="item_no", right_on="item_no", how="left")
            .merge(
                item_cls,
                left_on="item_clsno_1",
                right_on="item_clsno",
                how="left",
                suffixes=("", "_lv1"),
            )
        )

        part2 = part2[part2["item_clsno"] == "00"]
        part2 = part2[part2["item_flag"] == "0"]
        if len(part2) == 0:
            part2 = pd.DataFrame(columns=filter_columns)
        else:
            part2["saleprice"] = part2.apply(saleprice_convert, axis=1)
            part2["quantity"] = part2.apply(quantity_convert, axis=1)
            part2["subtotal"] = part2.apply(subtotal, axis=1)
            part2["source_id"] = self.source_id
            part2["cmid"] = self.cmid
            part2["consumer_id"] = ""
            part2["last_updated"] = datetime.now(_TZINFO)

            part2["foreign_category_lv2"] = ""
            part2["foreign_category_lv2_name"] = None
            part2["foreign_category_lv3"] = ""
            part2["foreign_category_lv3_name"] = None
            part2["pos_id"] = ""

            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv4_name"] = None
            part2["foreign_category_lv5"] = ""
            part2["foreign_category_lv5_name"] = None
            part2["pos_id"] = ""

            part2 = part2.rename(columns=rename_columns_dict)

            part2 = part2[filter_columns]

        return pd.concat([part1, part2])

    def cost(self):
        if self.source_id == "71YYYYYYYYYYYYY":
            return self.cost_71()
        else:
            return self.cost_other()

    """
        'origin_table_columns': {
            "ml_dayflow": ['item_no', 'oper_date', 'qty', 'amt', 'cost_amt','branch_no','trans_key'],
            'bi_t_item_info': ['item_no', 'item_clsno',],
            'bi_t_item_cls': ['item_clsno', 'item_flag']
        },

        'converts': {
            "ml_dayflow": {'branch_no': 'str', 'item_no':'str', 'trans_key':'str', 'oper_date':'str'},
            'bi_t_item_info': {'item_no': 'str', 'item_clsno': 'str'},
            'bi_t_item_cls': {'item_clsno':'str', 'item_flag':'str'}
        }
    """

    def cost_71(self):
        pass

    """
        'origin_table_columns': {
            "ml_dayflow": ['item_no', 'oper_date', 'qty', 'amt', 'cost_amt','branch_no','trans_key'],
            'bi_t_item_info': ['item_no', 'item_clsno',],
            'bi_t_item_cls': ['item_clsno', 'item_flag']
        },

        'converts': {
            "ml_dayflow": {'branch_no': 'str', 'item_no':'str', 'trans_key':'str', 'oper_date':'str'},
            'bi_t_item_info': {'item_no': 'str', 'item_clsno': 'str'},
            'bi_t_item_cls': {'item_clsno':'str', 'item_flag':'str'}
        }
    """

    def cost_other(self):
        cost = self.data["ml_dayflow"]
        item = self.data["bi_t_item_info"]
        item_cls = self.data["bi_t_item_cls"]

        cost["branch_no"] = cost.apply(lambda row: row["branch_no"][:2], axis=1)

        item["item_clsno_1"] = item.apply(lambda row: row["item_clsno"][:2], axis=1)
        item["item_clsno_2"] = item.apply(lambda row: row["item_clsno"][:4], axis=1)
        item["item_clsno_3"] = item.apply(lambda row: row["item_clsno"][:6], axis=1)

        part1 = (
            pd.merge(cost, item, on="item_no", how="left")
            .merge(
                item_cls,
                left_on="item_clsno_1",
                right_on="item_clsno",
                how="left",
                suffixes=("", "_lv1"),
            )
            .merge(
                item_cls,
                left_on="item_clsno_2",
                right_on="item_clsno",
                how="left",
                suffixes=("", "_lv2"),
            )
            .merge(
                item_cls,
                left_on="item_clsno_3",
                right_on="item_clsno",
                how="left",
                suffixes=("", "_lv3"),
            )
        )

        filter_columns = [
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

        rename_columns_dict = {
            "branch_no": "foreign_store_id",
            "item_no": "foreign_item_id",
            "oper_date": "date",
            "qty": "total_quantity",
            "amt": "total_sale",
            "cost_amt": "total_cost",
            "item_clsno_lv1": "foreign_category_lv1",
            "item_clsno_lv2": "foreign_category_lv2",
            "item_clsno_lv3": "foreign_category_lv3",
        }

        part1["cost_type"] = ""
        part1["source_id"] = self.source_id
        part1["foreign_category_lv4"] = ""
        part1["foreign_category_lv5"] = ""
        part1["cmid"] = self.cmid

        part1 = part1[
            (part1["item_clsno"] != "00") & (part1["trans_key"].isin(("1", "2")))
        ]

        part1 = part1.rename(columns=rename_columns_dict)
        part1 = part1[filter_columns]

        part2 = pd.merge(cost, item, on="item_no", how="left").merge(
            item_cls,
            left_on="item_clsno_1",
            right_on="item_clsno",
            how="left",
            suffixes=("", "_lv1"),
        )

        part2["cost_type"] = ""
        part2["source_id"] = self.source_id
        part2["foreign_category_lv2"] = ""
        part2["foreign_category_lv3"] = ""
        part2["foreign_category_lv4"] = ""
        part2["foreign_category_lv5"] = ""
        part2["cmid"] = self.cmid

        part2 = part2[
            (part2["item_clsno"] == "00")
            & (part2["trans_key"].isin(("1", "2")))
            & (part2["item_flag"] == "0")
        ]

        part2 = part2.rename(columns=rename_columns_dict)
        part2 = part2[filter_columns]

        result = pd.concat([part1, part2])
        result["date"] = result.apply(lambda row: row["date"].split()[0], axis=1)
        return result

    """
    'origin_table_columns': {
            "bi_t_branch_info": [
                'area_no',
                'branch_no',
                'branch_name',
                'address',
                'update_time',
                'branch_tel',
                'branch_man',
                'is_jmd',
                'custjs_type'
            ],
            "bi_t_area_info": ['area_no', 'area_name']
        },

        'converts': {"bi_t_branch_info": {'branch_no': 'str',
                                              'area_no': 'str',
                                              'is_jmd': 'str',
                                              'custjs_type': 'str'
                                              },
                     "bi_t_area_info": {'area_no': 'str'}
                     }
    """

    def store(self):
        store = self.data["bi_t_branch_info"]
        area = self.data["bi_t_area_info"]

        result = pd.merge(store, area, on="area_no", how="left")

        result = result[result["custjs_type"] == "0"]
        result["source_id"] = self.source_id
        result["cmid"] = self.cmid
        result["store_status"] = ""
        result["lat"] = None
        result["lng"] = None
        result["address_code"] = None
        result["device_id"] = None
        result["business_area"] = None
        result["last_updated"] = datetime.now(_TZINFO)
        result["show_code"] = result["branch_no"]

        result["property"] = result.apply(
            lambda row: "加盟店" if row["is_jmd"] == "1" else "直营店", axis=1
        )

        result = result.rename(
            columns={
                "branch_no": "foreign_store_id",
                "branch_name": "store_name",
                "address": "store_address",
                "update_time": "create_date",
                "branch_tel": "phone_number",
                "branch_man": "contacts",
                "area_no": "area_code",
                "area_name": "area_name",
                "is_jmd": "property_id",
            }
        )
        result = result[
            [
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
        ]
        return result

    """
        'origin_table_columns': {
            "bi_t_item_info": [
                'item_clsno',
                'sup_no',
                'barcode',
                'item_no',
                'price',
                'sale_price',
                'unit_no',
                'display_flag',
                'create_date',
                'item_subno',
                'item_brandname',
                'item_name'
            ],
            "bi_t_item_cls": ['item_clsno', 'item_flag'],
            "bi_t_supcust_info":['supcust_no', 'supcust_flag', 'sup_name']
        },

        'converts': {
            "bi_t_item_info": {
                'item_clsno':'str',
                'sup_no':'str',
                'display_flag':'str'
            },
            "bi_t_item_cls": {'item_clsno':'str', 'item_flag':'str'},
            "bi_t_supcust_info":{'supcust_no':'str', 'supcust_flag':'str'}
            }
    """

    def goods(self):
        item = self.data["bi_t_item_info"]
        item_cls = self.data["bi_t_item_cls"]
        supcust = self.data["bi_t_supcust_info"]

        item["item_clsno_1"] = item.apply(lambda row: row["item_clsno"][:2], axis=1)
        item["item_clsno_2"] = item.apply(lambda row: row["item_clsno"][:4], axis=1)
        item["item_clsno_3"] = item.apply(lambda row: row["item_clsno"][:6], axis=1)

        part1 = pd.merge(
            item,
            item_cls,
            left_on="item_clsno_1",
            right_on="item_clsno",
            suffixes=("", "_lv1"),
            how="left",
        )
        part1 = part1.merge(
            item_cls,
            left_on="item_clsno_2",
            right_on="item_clsno",
            suffixes=("", "_lv2"),
            how="left",
        )
        part1 = part1.merge(
            item_cls,
            left_on="item_clsno_3",
            right_on="item_clsno",
            suffixes=("", "_lv3"),
            how="left",
        )
        part1 = part1.merge(
            supcust, left_on="sup_no", right_on="supcust_no", how="left"
        )
        part1 = part1[(part1["item_clsno"] != "00") & (part1["supcust_flag"] == "S")]

        display_flag_to_item_status = {
            "0": "停用",
            "1": "进销",
            "2": "新品",
            "3": "只销",
            "4": "停销",
        }

        def item_status_convert(row):
            return display_flag_to_item_status[row["display_flag"]]

        part1["item_status"] = part1.apply(item_status_convert, axis=1)
        part1["cmid"] = self.cmid
        part1["foreign_category_lv4"] = ""
        part1["foreign_category_lv5"] = ""
        part1["last_updated"] = datetime.now(_TZINFO)
        part1["isvalid"] = 1
        part1["warranty"] = ""
        part1["allot_method"] = ""

        rename_dict = {
            "barcode": "barcode",
            "item_no": "foreign_item_id",
            "item_name": "item_name",
            "price": "lastin_price",
            "sale_price": "sale_price",
            "unit_no": "item_unit",
            "item_clsno_lv1": "foreign_category_lv1",
            "item_clsno_lv2": "foreign_category_lv2",
            "item_clsno_lv3": "foreign_category_lv3",
            "create_date": "storage_time",
            "item_subno": "show_code",
            "sup_name": "supplier_name",
            "supcust_no": "supplier_code",
            "item_brandname": "brand_name",
        }

        filter_columns = [
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

        part1 = part1.rename(columns=rename_dict)
        part1 = part1[filter_columns]

        part2 = pd.merge(
            item,
            item_cls,
            left_on="item_clsno_1",
            right_on="item_clsno",
            suffixes=("", "_lv1"),
            how="left",
        ).merge(supcust, left_on="sup_no", right_on="supcust_no", how="left")

        part2 = part2[
            (part2["item_clsno"] == "00")
            & (part2["supcust_flag"] == "S")
            & (part2["item_flag"] == "0")
        ]

        part2["item_status"] = part2.apply(item_status_convert, axis=1)
        part2["cmid"] = self.cmid
        part2["foreign_category_lv2"] = ""
        part2["foreign_category_lv3"] = ""
        part2["foreign_category_lv4"] = ""
        part2["foreign_category_lv5"] = ""
        part2["last_updated"] = datetime.now(_TZINFO)
        part2["isvalid"] = 1
        part2["warranty"] = ""
        part2["allot_method"] = ""

        part2 = part2.rename(columns=rename_dict)
        part2 = part2[filter_columns]

        return pd.concat([part1, part2])

    """
    'origin_table_columns': {
            "bi_t_item_cls": ['item_clsno',
                              'item_clsname',
                              'item_flag',
                              ]
        },

    'converts': {"bi_t_item_cls": {'item_clsno': 'str',
                                        'item_clsname': 'str',
                                        'item_flag': 'str'}}
    """

    def category(self):
        item_cls = self.data["bi_t_item_cls"]

        item_cls["item_clsno_1"] = item_cls.apply(
            lambda row: row["item_clsno"][:2], axis=1
        )
        item_cls["item_clsno_2"] = item_cls.apply(
            lambda row: row["item_clsno"][:4], axis=1
        )

        part1 = item_cls[
            (item_cls["item_flag"] == "0") & (item_cls["item_clsno"].str.len() == 2)
        ]
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
                "item_clsno": "foreign_category_lv1",
                "item_clsname": "foreign_category_lv1_name",
            }
        )

        part2 = item_cls.merge(
            item_cls,
            left_on="item_clsno_1",
            right_on="item_clsno",
            how="left",
            suffixes=("", "_lv1"),
        )

        part2 = part2[
            (part2["item_clsno"].str.len() == 4) & (part2["item_flag"] == "0")
        ]
        part2 = part2.rename(
            columns={
                "item_clsno_lv1": "foreign_category_lv1",
                "item_clsname_lv1": "foreign_category_lv1_name",
                "item_clsno": "foreign_category_lv2",
                "item_clsname": "foreign_category_lv2_name",
            }
        )

        part2["foreign_category_lv3"] = ""
        part2["foreign_category_lv3_name"] = None
        part2["foreign_category_lv4"] = ""
        part2["foreign_category_lv4_name"] = None
        part2["foreign_category_lv5"] = ""
        part2["foreign_category_lv5_name"] = None
        part2["last_updated"] = datetime.now(_TZINFO)
        part2["cmid"] = self.cmid
        part2["level"] = 2

        part3 = item_cls.merge(
            item_cls,
            left_on="item_clsno_2",
            right_on="item_clsno",
            how="left",
            suffixes=("", "_lv2"),
        ).merge(
            item_cls,
            left_on="item_clsno_1",
            right_on="item_clsno",
            how="left",
            suffixes=("", "_lv1"),
        )
        part3 = part3[
            (part3["item_clsno"].str.len() == 6) & (part3["item_flag"] == "0")
        ]

        part3 = part3.rename(
            columns={
                "item_clsno_lv1": "foreign_category_lv1",
                "item_clsname_lv1": "foreign_category_lv1_name",
                "item_clsno_lv2": "foreign_category_lv2",
                "item_clsname_lv2": "foreign_category_lv2_name",
                "item_clsno": "foreign_category_lv3",
                "item_clsname": "foreign_category_lv3_name",
            }
        )

        part3["foreign_category_lv4"] = ""
        part3["foreign_category_lv4_name"] = None
        part3["foreign_category_lv5"] = ""
        part3["foreign_category_lv5_name"] = None
        part3["last_updated"] = datetime.now(_TZINFO)
        part3["cmid"] = self.cmid
        part3["level"] = 3

        filter_columns = [
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

        part1 = part1[filter_columns]
        part2 = part2[filter_columns]
        part3 = part3[filter_columns]

        return pd.concat([part1, part2, part3])

    """
    'origin_table_columns': {
            "ic_t_check_master": ['sheet_no',
                                      'branch_no',
                                      'approve_flag',
                                      'del_flag',
                                      'check_no',
                                      'oper_date'
                                      ],
            "ic_t_check_detail": ['sheet_no', 'item_no', 'balance_qty'],
            "bi_t_branch_info": ['branch_no', 'branch_name'],
            "bi_t_item_info": ['item_no', 'item_clsno', 'base_price', 'item_subno', 'barcode', 'unit_no', 'item_name'],
            "bi_t_item_cls": ['item_clsno', 'item_flag']
        },

        'converts': {
            "ic_t_check_master": {
                'sheet_no': 'str',
                'branch_no': 'str',
                'approve_flag': 'str',
                'del_flag': 'str',
            },
            "ic_t_check_detail": {
                'sheet_no': 'str',
                'item_no': 'str',
                'balance_qty': 'str',

            },
            "bi_t_branch_info": {
                'branch_no': 'str'
            },
            "bi_t_item_info": {
                'item_no': 'str',
                'item_clsno': 'str',
                'base_price': 'str'
            },
            "bi_t_item_cls": {
                'item_clsno': 'str',
                'item_flag':'str'
            }
        }
    """

    def goods_loss(self):
        filter_columns = [
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
        loss = self.data["ic_t_check_master"]

        if not len(loss):
            return pd.DataFrame(columns=filter_columns)

        detail = self.data["ic_t_check_detail"]
        
        if not len(detail):
            return pd.DataFrame(columns=filter_columns)

        detail["balance_qty"] = detail.apply(
            lambda row: float(row["balance_qty"]), axis=1
        )

        store = self.data["bi_t_branch_info"]
        item = self.data["bi_t_item_info"]
        item_cls = self.data["bi_t_item_cls"]

        def test(row):
            if row["base_price"]:
                return float(row["base_price"])
            else:
                print(row["item_no"])
                return 0

        if not len(loss):
            return pd.DataFrame(columns=filter_columns)
        # item['base_price'] = item.apply(lambda row: float(row['base_price']), axis=1)
        loss["branch_no"] = loss.apply(lambda row: row["branch_no"][:2], axis=1)

        item["base_price"] = item.apply(test, axis=1)
        item["item_clsno_1"] = item.apply(lambda row: row["item_clsno"][:2], axis=1)
        item["item_clsno_2"] = item.apply(lambda row: row["item_clsno"][:4], axis=1)
        item["item_clsno_3"] = item.apply(lambda row: row["item_clsno"][:6], axis=1)

        part1 = (
            loss.merge(detail, on="sheet_no", how="left")
            .merge(store, on="branch_no", how="left")
            .merge(item, on="item_no", how="left")
            .merge(
                item_cls,
                how="left",
                left_on="item_clsno_1",
                right_on="item_clsno",
                suffixes=("", "_lv1"),
            )
            .merge(
                item_cls,
                how="left",
                left_on="item_clsno_2",
                right_on="item_clsno",
                suffixes=("", "_lv2"),
            )
            .merge(
                item_cls,
                how="left",
                left_on="item_clsno_3",
                right_on="item_clsno",
                suffixes=("", "_lv3"),
            )
        )

        part1 = part1[
            (part1["balance_qty"] < 0)
            & (part1["approve_flag"] == "1")
            & (part1["del_flag"] == "0")
            & (part1["item_clsno"] != "00")
            & (part1["branch_no"] != "00")
        ]

        if not len(part1):
            part1 = pd.DataFrame(columns=filter_columns)
        else:

            part1["cmid"] = self.cmid
            part1["source_id"] = self.cmid
            part1["foreign_category_lv4"] = ""
            part1["foreign_category_lv5"] = ""
            part1["subtotal"] = part1.apply(
                lambda row: float(row["balance_qty"]) * float(row["base_price"]), axis=1
            )
            part1["store_show_code"] = part1["branch_no"]

            part1 = part1.rename(
                columns={
                    "check_no": "lossnum",
                    "oper_date": "lossdate",
                    "branch_no": "foreign_store_id",
                    "branch_name": "store_name",
                    "item_no": "foreign_item_id",
                    "item_subno": "item_showcode",
                    "barcode": "barcode",
                    "item_name": "item_name",
                    "unit_no": "item_unit",
                    "balance_qty": "quantity",
                    "item_clsno_lv1": "foreign_category_lv1",
                    "item_clsno_lv2": "foreign_category_lv2",
                    "item_clsno_lv3": "foreign_category_lv3",
                }
            )

            part1["lossdate"] = part1.apply(
                lambda row: row["lossdate"].split()[0], axis=1
            )

        part2 = (
            loss.merge(detail, on="sheet_no", how="left")
            .merge(store, on="branch_no", how="left")
            .merge(item, on="item_no", how="left")
            .merge(
                item_cls,
                how="left",
                left_on="item_clsno_1",
                right_on="item_clsno",
                suffixes=("", "_lv1"),
            )
        )

        part2 = part2[
            (part2["balance_qty"] < 0)
            & (part2["approve_flag"] == "1")
            & (part2["del_flag"] == "0")
            & (part2["item_clsno"] == "00")
            & (part2["branch_no"] != "00")
            & (part2["item_flag"] == "0")
        ]

        if not len(part2):
            part2 = pd.DataFrame(columns=filter_columns)
        else:

            part2["cmid"] = self.cmid
            part2["source_id"] = self.cmid
            part2["foreign_category_lv2"] = ""
            part2["foreign_category_lv3"] = ""
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""
            part2["subtotal"] = part2.apply(
                lambda row: float(row["balance_qty"]) * float(row["base_price"]), axis=1
            )
            part2["store_show_code"] = part2["branch_no"]

            part2 = part2.rename(
                columns={
                    "check_no": "lossnum",
                    "oper_date": "lossdate",
                    "branch_no": "foreign_store_id",
                    "branch_name": "store_name",
                    "item_no": "foreign_item_id",
                    "item_subno": "item_showcode",
                    "barcode": "barcode",
                    "item_name": "item_name",
                    "unit_no": "item_unit",
                    "balance_qty": "quantity",
                    "item_clsno_lv1": "foreign_category_lv1",
                }
            )
            part2["lossdate"] = part2.apply(
                lambda row: row["lossdate"].split()[0], axis=1
            )

        part1 = part1[filter_columns]
        part2 = part2[filter_columns]

        return pd.concat([part1, part2])
