"""
# goodsflow:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "goodsflow",
    "origin_table_columns": {
        "HD40.buy2s": ["gdcode", "qty", "gid", "realamt", "flowno", "posno"],
        "HD40.buy1s": ["cardno", "flowno", "posno", "fildate"],
        "HD40.workstation": ["no", "storegid"],
        "HD40.store": ["gid", "name"],
        "HD40.goods": ["gid", "munit", "name", "sort"],
        "HD40.sort": ["name", "code"],
    },
    "converts": {
        "HD40.buy2s": {
            "gdcode": "str",
            "flowno": "str",
            "posno": "str",
            "gid": "str",
        },
        "HD40.buy1s": {
            "cardno": "str",
            "flowno": "str",
            "posno": "str",
            "fildate": "str",
        },
        "HD40.workstation": {"no": "str", "storegid": "str"},
        "HD40.store": {"gid": "str", "name": "str"},
        "HD40.goods": {"sort": "str", "gid": "str", "munit": "str", "name": "str"},
        "HD40.sort": {"code": "str", "name": "str"},
    },
}
# cost:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "cost",
    "origin_table_columns": {
        "hd_report.rpt_storesaldrpt": [
            "cls",
            "fildate",
            "orgkey",
            "pdkey",
            "saleamt",
            "salecamt",
            "salectax",
            "saleqty",
            "saletax",
        ],
        "HD40.goods": ["sort", "gid"],
        "HD40.sort": ["code"],
        "HD40.sdrpts": [
            "amt",
            "cls",
            "fildate",
            "gdgid",
            "iamt",
            "itax",
            "ocrdate",
            "qty",
            "snd",
            "tax",
        ],
    },
    "converts": {
        "hd_report.rpt_storesaldrpt": {
            "cls": "str",
            "fildate": "str",
            "orgkey": "str",
            "pdkey": "str",
        },
        "HD40.goods": {"sort": "str", "gid": "str"},
        "HD40.sort": {"code": "str"},
        "HD40.sdrpts": {
            "cls": "str",
            "fildate": "str",
            "ocrdate": "str",
            "gdgid": "str",
            "snd": "str",
        },
    },
}
# purchase_warehouse
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "purchase_warehouse",
    "origin_table_columns": {
        "HD40.stkin": ["num", "fildate", "cls", "vendor", "stat"],
        "HD40.stkindtl": [
            "qty",
            "price",
            "qpc",
            "total",
            "num",
            "cls",
            "gdgid",
            "wrh",
        ],
        "HD40.vendorh": ["gid", "code", "name"],
        "HD40.modulestat": ["statname", "no"],
        "HD40.goods": ["brand", "code", "code2", "gid", "munit", "name", "sort"],
        "HD40.brand": ["code", "name"],
        "HD40.warehouseh": ["code", "name", "gid"],
        "HD40.stkinbck": ["num", "fildate", "cls", "vendor", "stat"],
        "HD40.stkinbckdtl": [
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
        "HD40.stkin": {
            "num": "str",
            "fildate": "str",
            "cls": "str",
            "vendor": "str",
        },
        "HD40.stkindtl": {"num": "str", "cls": "str", "gdgid": "str", "wrh": "str"},
        "HD40.vendorh": {"gid": "str", "code": "str", "name": "str"},
        "HD40.modulestat": {"statname": "str"},
        "HD40.goods": {
            "code": "str",
            "brand": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "sort": "str",
            "name": "str",
        },
        "HD40.brand": {"code": "str", "name": "str"},
        "HD40.warehouseh": {"code": "str", "name": "str", "gid": "str"},
        "HD40.stkinbck": {
            "num": "str",
            "fildate": "str",
            "cls": "str",
            "vendor": "str",
        },
        "HD40.stkinbckdtl": {
            "num": "str",
            "cls": "str",
            "gdgid": "str",
            "wrh": "str",
        },
    },
}
"""

import pandas as pd
import boto3
from datetime import datetime
import tempfile
import time
import pytz


S3_BUCKET = "ext-etl-data"
S3 = boto3.client("s3")
_TZINFO = pytz.timezone("Asia/Shanghai")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


class HaiDingCleaner:
    def __init__(self, source_id: str, date, data: dict) -> None:
        self.source_id = source_id
        self.date = date
        self.cmid = self.source_id.split("Y", 1)[0]
        self.data = data

    def clean(self, target_table):
        method = getattr(self, target_table, None)
        if method and callable(method):
            df = getattr(self, target_table)()
            self.up_load_to_s3(df, target_table)
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

    def goodsflow(self):
        buy2s = self.data["HD40.buy2s"]
        buy1s = self.data["HD40.buy1s"]
        workstation = self.data["HD40.workstation"]
        store = self.data["HD40.store"]
        goods = self.data["HD40.goods"]
        sort = self.data["HD40.sort"]
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

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
        part = (
            buy2s.merge(
                buy1s, how="left", on=["flowno", "posno"], suffixes=("", ".buy1s")
            )
            .merge(
                workstation,
                how="left",
                left_on=["posno"],
                right_on=["no"],
                suffixes=("", ".workstation"),
            )
            .merge(
                store,
                how="left",
                left_on=["storegid"],
                right_on=["gid"],
                suffixes=("", ".store"),
            )
            .merge(
                goods,
                how="left",
                left_on=["gid"],
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
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id
        part["saleprice"] = part.apply(
            lambda row: 0 if row["qty"] == 0 else row["realamt"] / row["qty"], axis=1
        )
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv4_name"] = None
        part["foreign_category_lv5"] = ""
        part["foreign_category_lv5_name"] = None
        part["last_updated"] = str(datetime.now())
        part = part[part["gid.store"].notnull() & part["gid"].notnull()]
        part = part.rename(
            columns={
                "gid.store": "foreign_store_id",
                "name": "store_name",
                "flowno": "receipt_id",
                "cardno": "consumer_id",
                "fildate": "saletime",
                "gid": "foreign_item_id",
                "gdcode": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "qty": "quantity",
                "realamt": "subtotal",
                "sort1": "foreign_category_lv1",
                "name.sort1": "foreign_category_lv1_name",
                "sort2": "foreign_category_lv2",
                "name.sort2": "foreign_category_lv2_name",
                "sort3": "foreign_category_lv3",
                "name.sort3": "foreign_category_lv3_name",
                "posno": "pos_id",
            }
        )
        return part[columns]

    def cost(self):
        rpt_storesaldrpt = self.data["hd_report.rpt_storesaldrpt"]
        goods = self.data["HD40.goods"]
        sort = self.data["HD40.sort"]
        sdrpts = self.data["HD40.sdrpts"]
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

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

        part1 = (
            rpt_storesaldrpt.merge(
                goods,
                how="left",
                left_on=["pdkey"],
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
        part1["source_id"] = self.source_id
        part1["cmid"] = self.cmid
        part1["total_sale"] = part1.apply(
            lambda row: row["saleamt"] + row["saletax"], axis=1
        )
        part1["total_cost"] = part1.apply(
            lambda row: row["salecamt"] + row["salectax"], axis=1
        )
        part1["foreign_category_lv1"] = part1.apply(
            lambda row: "" if row["code"] is None else row["code"], axis=1
        )
        part1["foreign_category_lv2"] = part1.apply(
            lambda row: "" if row["code.sort2"] is None else row["code.sort2"], axis=1
        )
        part1["foreign_category_lv3"] = part1.apply(
            lambda row: "" if row["code.sort3"] is None else row["code.sort3"], axis=1
        )
        part1["foreign_category_lv4"] = ""
        part1["foreign_category_lv5"] = ""
        part1 = part1.rename(
            columns={
                "orgkey": "foreign_store_id",
                "pdkey": "foreign_item_id",
                "fildate": "date",
                "cls": "cost_type",
                "saleqty": "total_quantity",
            }
        )
        part1 = part1[columns]
        part2 = (
            sdrpts.merge(
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

        part2["source_id"] = self.source_id
        part2["cmid"] = self.cmid
        part2["snd"] = part2["snd"].astype(str)
        part2["gdgid"] = part2["gdgid"].astype(str)
        part2["total_quantity"] = part2.apply(
            lambda row: row["qty"]
            if row["cls"] in ("零售", "批发")
            else 0
            if row["cls"] in ("成本差异", "成本调整")
            else -1 * row["qty"],
            axis=1,
        )
        part2["total_sale"] = part2.apply(
            lambda row: row["amt"] + row["tax"]
            if row["cls"] in ("零售", "批发")
            else 0
            if row["cls"] in ("成本差异", "成本调整")
            else -1 * (row["amt"] + row["tax"]),
            axis=1,
        )
        part2["total_cost"] = part2.apply(
            lambda row: row["iamt"] + row["itax"]
            if row["cls"] in ("零售", "批发")
            else 0
            if row["cls"] in ("成本差异", "成本调整")
            else -1 * (row["iamt"] + row["itax"]),
            axis=1,
        )
        part2["foreign_category_lv1"] = part2.apply(
            lambda row: "" if row["code"] is None else row["code"], axis=1
        )
        part2["foreign_category_lv2"] = part2.apply(
            lambda row: "" if row["code.sort2"] is None else row["code.sort2"], axis=1
        )
        part2["foreign_category_lv3"] = part2.apply(
            lambda row: "" if row["code.sort3"] is None else row["code.sort3"], axis=1
        )
        part2["foreign_category_lv4"] = ""
        part2["foreign_category_lv5"] = ""
        now = datetime.now().strftime("%Y-%m-%d")
        part2 = part2[
            (part2["ocrdate"] >= now)
            & (part2["cls"].isin(("零售", "零售退", "批发", "批发退", "成本差异", "成本调整")))
        ]
        part2 = part2.rename(
            columns={
                "snd": "foreign_store_id",
                "gdgid": "foreign_item_id",
                "fildate": "date",
                "cls": "cost_type",
            }
        )
        part2 = part2[columns]
        return pd.concat([part1, part2])

    def requireorder(self):
        # otrequireorder = self.data["HD40.otrequireorder"]
        # otrequireorderline = self.data["HD40.otrequireorderline"]
        # store = self.data["HD40.store"]
        # goods = self.data["HD40.goods"]
        # sort = self.data["HD40.sort"]
        # vendor = self.data["HD40.vendor"]
        # employee = self.data["HD40.employee"]
        pass

    def purchase_warehouse(self):
        stkin = self.data["HD40.stkin"]
        stkindtl = self.data["HD40.stkindtl"]
        vendorh = self.data["HD40.vendorh"]
        modulestat = self.data["HD40.modulestat"]
        goods = self.data["HD40.goods"]
        brand = self.data["HD40.brand"]
        warehouseh = self.data["HD40.warehouseh"]
        stkinbck = self.data["HD40.stkinbck"]
        stkinbckdtl = self.data["HD40.stkinbckdtl"]

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
