"""
友琪清洗逻辑
销售，成本，库存，商品， 分类， 商损
"""

import pandas as pd
import boto3
from datetime import datetime
import tempfile
import time
import pytz
from typing import Dict
import numpy as np
S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")
_TZINFO = pytz.timezone("Asia/Shanghai")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


class YouQiCleaner:
    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        self.source_id = source_id
        self.date = date
        self.cmid = self.source_id.split("Y", 1)[0]
        self.data = data

    def clean(self, target_table):
        method = getattr(self, target_table, None)
        if method and callable(method):
            df = getattr(self, target_table)()
            return self.up_load_to_s3(df, target_table)
        else:
            raise RuntimeError(f"没有这个表: {target_table}")

    def up_load_to_s3(self, dataframe, target_table):
        filename = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
        count = len(dataframe)
        dataframe.to_csv(
            filename.name, index=False, compression="gzip", float_format="%.4f"
        )
        filename.seek(0)
        key = CLEANED_PATH.format(
            source_id=self.source_id,
            target_table=target_table,
            date=self.date,
            timestamp=datetime.fromtimestamp(time.time(), tz=_TZINFO),
            rowcount=count,
        )
        S3.Bucket(S3_BUCKET).upload_file(filename.name, key)

        return key

    def goodsflow(self):
        columns = [
            'source_id', 'cmid', 'foreign_store_id', 'store_name', 'receipt_id', 'consumer_id', 'saletime',
            'last_updated', 'foreign_item_id', 'barcode', 'item_name', 'item_unit', 'saleprice', 'quantity', 'subtotal',
            'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
            'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
            'foreign_category_lv5', 'foreign_category_lv5_name', 'pos_id'
        ]
        goodsflow = self.data["goodsflow"]
        store = self.data["tshopitem"]
        goods = self.data["product"]
        lv3 = self.data["tdepset"].rename(columns=lambda x: f"lv3.{x}")
        lv2 = self.data["tdepset"].rename(columns=lambda x: f"lv2.{x}")
        lv1 = self.data["tdepset"].rename(columns=lambda x: f"lv1.{x}")

        goods["lv2_depcode"] = goods.depcode.apply(lambda x: x[:4])
        goods["lv1_depcode"] = goods.depcode.apply(lambda x: x[:2])

        frames = (
            goodsflow
            .merge(store, how="left", left_on="store_id", right_on="uniquecode")
            .merge(goods, how="left", on="pid")
            .merge(lv3, how="left", left_on="depcode", right_on="lv3.depcode")
            .merge(lv2, how="left", left_on="lv2_depcode", right_on="lv2.depcode")
            .merge(lv1, how="left", left_on="lv1_depcode", right_on="lv1.depcode")
        )
        frames = frames[frames["tradeflag"].isin(("T", "R"))]

        if len(frames) == 0:
            frames = pd.DataFrame(columns=columns)
        else:
            frames["source_id"] = self.source_id
            frames["cmid"] = self.cmid
            frames["consumer_id"] = ""
            frames["last_updated"] = datetime.now(_TZINFO)
            frames["foreign_category_lv4"] = ""
            frames["foreign_category_lv4_name"] = None
            frames["foreign_category_lv5"] = ""
            frames["foreign_category_lv5_name"] = None
            frames["pos_id"] = frames.lsno.apply(lambda x: x[:4])
            frames = frames.rename(columns={
                "uniquecode": "foreign_store_id",
                "shopname": "store_name",
                "lsno": "receipt_id",
                "sdatetime": "saletime",
                "pid": "foreign_item_id",
                "prodname": "item_name",
                "unit": "item_unit",
                "price": "saleprice",
                "amount": "quantity",
                "total": "subtotal",
                "lv1.depcode": "foreign_category_lv1",
                "lv1.depname": "foreign_category_lv1_name",
                "lv2.depcode": "foreign_category_lv2",
                "lv2.depname": "foreign_category_lv2_name",
                "lv3.depcode": "foreign_category_lv3",
                "lv3.depname": "foreign_category_lv3_name",
            })
            frames["barcode"] = frames.barcode.apply(lambda x: None if pd.isnull(x) else str(int(x)))
            frames["foreign_store_id"] = frames["foreign_store_id"].apply(lambda x: str(x).zfill(5))
        return frames[columns]

    def cost(self):
        columns = [
            "source_id", "foreign_store_id", "foreign_item_id", "date", "cost_type", "total_quantity",
            "total_sale", "total_cost", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3",
            "foreign_category_lv4", "foreign_category_lv5", "cmid"
        ]

        cost = self.data["cost"]
        goods = self.data["product"]
        frames = cost.merge(goods, how="left", on="pid")
        frames = frames[frames["pid"].notna()]
        frames = (
            frames
            .groupby(["pid", "ywdate", "depcode", "store_id"], as_index=False)
            .agg({"countn": np.sum, "saleamt": np.sum, "cost": np.sum})
        )

        if len(frames) == 0:
            frames = pd.DataFrame(columns=columns)
        else:
            frames["source_id"] = self.source_id
            frames["cost_type"] = ""
            frames["foreign_category_lv1"] = frames.depcode.apply(lambda x: x[:2])
            frames["foreign_category_lv2"] = frames.depcode.apply(lambda x: x[:4])
            frames["foreign_category_lv4"] = ""
            frames["foreign_category_lv5"] = ""
            frames["cmid"] = self.cmid

            frames = frames.rename(columns={
                "store_id": "foreign_store_id",
                "pid": "foreign_item_id",
                "ywdate": "date",
                "countn": "total_quantity",
                "saleamt": "total_sale",
                "cost": "total_cost",
                "depcode": "foreign_category_lv3"
            })
            frames["foreign_store_id"] = frames["foreign_store_id"].apply(lambda x: str(x).zfill(5))

        return frames[columns]

    def goods(self):

        goods = self.data["product"]
        cls = self.data["tdepset"]
        vendor = self.data["tsuppset"]

        frames = (
            goods
            .merge(cls, how="left", on="depcode")
            .merge(vendor, how="left", on="suppcode")
        )

        frames["cmid"] = self.cmid
        frames["item_status"] = (
            frames.apply(
                lambda row:
                ("禁采" if row["fnocg"] == 1 else "") +
                ("禁销" if row["fnosale"] == 1 else "") +
                ("禁要" if row["fnoyh"] == 1 else "") +
                ("禁促" if row["fnopromotion"] == 1 else "") +
                ("禁退" if row["fnoth"] == 1 else "") +
                ("禁厨打" if row["fnocd"] == 1 else ""),
                axis=1
            )
        )
        frames["foreign_category_lv1"] = frames.depcode.apply(lambda x: None if pd.isnull(x) else x[:2])
        frames["foreign_category_lv2"] = frames.depcode.apply(lambda x: None if pd.isnull(x) else x[:4])
        frames["foreign_category_lv4"] = None
        frames["foreign_category_lv5"] = None
        frames["last_updated"] = datetime.now(_TZINFO)
        frames["isvalid"] = frames.isdel.apply(lambda x: "未删除" if x == 0 else "已删除")
        frames["allot_method"] = None
        frames["brand_name"] = None
        frames["warranty"] = frames["validdays"]
        frames["storage_time"] = datetime.now(_TZINFO)
        frames = frames.rename(columns={
            "pid": "foreign_item_id",
            "prodname": "item_name",
            "coprice": "lastin_price",
            "stdprice": "sale_price",
            "unit": "item_unit",
            "depcode": "foreign_category_lv3",
            "prodcode": "show_code",
            "suppname": "supplier_name",
            "suppcode": "supplier_code",
        })
        frames = frames[[
            "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
            "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
            "storage_time",
            "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method", "supplier_name",
            "supplier_code", "brand_name"
        ]]

        return frames

    def category(self):
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

        lv = self.data["tdepset"]

        frame1 = lv[lv["deplevel"] == 1]
        frame1["cmid"] = self.cmid
        frame1["foreign_category_lv2"] = ""
        frame1["foreign_category_lv2_name"] = None
        frame1["foreign_category_lv3"] = ""
        frame1["foreign_category_lv3_name"] = None
        frame1["foreign_category_lv4"] = ""
        frame1["foreign_category_lv4_name"] = None
        frame1["foreign_category_lv5"] = ""
        frame1["foreign_category_lv5_name"] = None
        frame1["last_updated"] = datetime.now(_TZINFO)

        frame1 = frame1.rename(columns={
            "deplevel": "level",
            "depcode": "foreign_category_lv1",
            "depname": "foreign_category_lv1_name",
        })
        frame1 = frame1[columns]
        
        lv["lv2_depcode"] = lv["depcode"].apply(lambda x: x[:2])
        frame2 = (
            lv
            .merge(lv, how="left", left_on="lv2_depcode", right_on="depcode", suffixes=("_lv2", "_lv1"))
        )
        frame2 = frame2[frame2["deplevel_lv2"] == 2]
        frame2["cmid"] = self.cmid
        frame2["foreign_category_lv3"] = ""
        frame2["foreign_category_lv3_name"] = None
        frame2["foreign_category_lv4"] = ""
        frame2["foreign_category_lv4_name"] = None
        frame2["foreign_category_lv5"] = ""
        frame2["foreign_category_lv5_name"] = None
        frame2["last_updated"] = datetime.now(_TZINFO)
        frame2 = frame2.rename(columns={
            "deplevel_lv2": "level",
            "depcode_lv1": "foreign_category_lv1",
            "depname_lv1": "foreign_category_lv1_name",
            "depcode_lv2": "foreign_category_lv2",
            "depname_lv2": "foreign_category_lv2_name",
        })        
        frame2 = frame2[columns]
        
        lv["lv3_depcode"] = lv["depcode"].apply(lambda x: x[:4])
        frame3 = (
            lv
            .merge(lv, how="left", left_on="lv3_depcode", right_on="depcode", suffixes=("_lv3", "_lv2"))
            .merge(lv, how="left", left_on="lv2_depcode_lv3", right_on="depcode")
        )
        frame3 = frame3[frame3["deplevel_lv3"] == 3]
        frame3["cmid"] = self.cmid
        frame3["foreign_category_lv4"] = ""
        frame3["foreign_category_lv4_name"] = None
        frame3["foreign_category_lv5"] = ""
        frame3["foreign_category_lv5_name"] = None
        frame3["last_updated"] = datetime.now(_TZINFO)
        frame3 = frame3.rename(columns={
            "deplevel_lv3": "level",
            "depcode": "foreign_category_lv1",
            "depname": "foreign_category_lv1_name",
            "depcode_lv2": "foreign_category_lv2",
            "depname_lv2": "foreign_category_lv2_name",
            "depcode_lv3": "foreign_category_lv3",
            "depname_lv3": "foreign_category_lv3_name",
        })
        frame3 = frame3[columns]
        
        return pd.concat([frame1, frame2, frame3])

    def store(self):
        columns = [
            'cmid', 'foreign_store_id', 'store_name', 'store_address', 'address_code', 'device_id', 'store_status',
            'create_date', 'lat', 'lng', 'show_code', 'phone_number', 'contacts', 'area_code', 'area_name',
            'business_area', 'property_id', 'property', 'source_id', 'last_updated'
        ]

        store = self.data["tshopitem"]
        area = self.data["tshoparea"]
        frames = (
            store
            .merge(area, how="left", left_on="shoparea", right_on="shopareacode", suffixes=("_store", "_area"))
        )
        frames["cmid"] = self.cmid
        frames["address_code"] = ""
        frames["device_id"] = ""
        frames["lat"] = None
        frames["lng"] = None
        frames["business_area"] = ""
        frames["property"] = (
            frames["shoptype"]
            .apply(
                lambda x:
                "配送中心" if x == 0 else
                "办事处" if x == 1 else
                "自营店" if x == 1 else
                "加盟店" if x == 1 else None
            )
        )
        frames["source_id"] = self.source_id
        frames["last_updated"] = datetime.now(_TZINFO)
        frames = frames.rename(columns={
            "uniquecode": "foreign_store_id",
            "shopname": "store_name",
            "address": "store_address",
            "shopmemo": "store_status",
            "sdatetime": "create_date",
            "shopcode": "show_code",
            "phoneno": "phone_number",
            "linkman": "contacts",
            "shopareacode": "area_code",
            "shopareaname": "area_name",
            "shoptype": "property_id",
        })
        frames = frames[columns]

        return frames

    def goods_loss(self):
        columns = [
            "cmid", "source_id", "lossnum", "lossdate", "foreign_store_id", "store_show_code", "store_name",
            "foreign_item_id", "item_showcode", "barcode", "item_name", "item_unit", "quantity", "subtotal",
            "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
            "foreign_category_lv5",
        ]

        goodslossh = self.data["goodslossh"]
        goodsloss = self.data["goodsloss"]
        store = self.data["tshopitem"]
        goods = self.data["product"]

        goodslossh["uniquecode"] = goodslossh["formno"].apply(lambda x: x[:5])
        frames = (
            goodslossh
            .merge(goodsloss, on="formno")
            .merge(store, how="left", on="uniquecode")
            .merge(goods, how="left", on="pid")
        )
        frames = frames[frames["checktype"] == 1]

        if len(frames) == 0:
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = self.cmid
            frames["source_id"] = self.source_id
            frames["foreign_category_lv1"] = frames["depcode"].apply(lambda x: x[:2])
            frames["foreign_category_lv2"] = frames["depcode"].apply(lambda x: x[:4])
            frames["foreign_category_lv4"] = ""
            frames["foreign_category_lv5"] = ""

            frames = frames.rename(columns={
                "formno": "lossnum",
                "formdate": "lossdate",
                "uniquecode": "foreign_store_id",
                "shopcode": "store_show_code",
                "shopname": "store_name",
                "pid": "foreign_item_id",
                "prodcode": "item_showcode",
                "prodname": "item_name",
                "unit": "item_unit",
                "countn": "quantity",
                "total": "subtotal",
                "depcode": "foreign_category_lv3",
            })
            frames = frames[columns]

        return frames