"""
将event配置的ETL-engine中，做成自动化
"""

import pandas as pd
import boto3
from datetime import datetime
import tempfile
import time
import pytz
from typing import Dict

S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")
_TZINFO = pytz.timezone("Asia/Shanghai")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


def from_excel_get_position_43():
    key = "datapipeline/source_id=43YYYYYYYYYYYYY/43_store_positon.csv"
    key = f"s3://{S3_BUCKET}/{key}"
    store_position_frame = pd.read_csv(
        key, encoding="utf-8", usecols=["id", "store_name", "lat", "lng"]
    )
    return store_position_frame


class HaiDingCleaner:
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
        buy2s = self.data["buy2s"]
        buy1s = self.data["buy1s"]
        workstation = self.data["workstation"]
        store = self.data["store"]
        goods = self.data["goods"]
        sort = self.data["sort"]
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
                how="inner",
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
        part["last_updated"] = str(datetime.now(_TZINFO))
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
        rpt_storesaldrpt = self.data["rpt_storesaldrpt"]
        goods = self.data["goods"]
        sort = self.data["sort"]
        sdrpts = self.data["sdrpts"]
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

        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
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
                lambda row: "" if row["code.sort2"] is None else row["code.sort2"],
                axis=1,
            )
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: "" if row["code.sort3"] is None else row["code.sort3"],
                axis=1,
            )
            part1["foreign_category_lv4"] = ""
            part1["foreign_category_lv5"] = ""

            if self.source_id in ("79YYYYYYYYYYYYY", "80YYYYYYYYYYYYY"):
                part1 = part1[part1["cls"] == "零售"]
            elif self.source_id in ("82YYYYYYYYYYYYY","43YYYYYYYYYYYYY"):
                part1 = part1[part1["cls"] != "批发"]

            part1["fildate"] = part1.apply(
                lambda row: row["fildate"].split()[0], axis=1
            )

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

        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
            part2["source_id"] = self.source_id
            part2["cmid"] = self.cmid
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
                lambda row: "" if row["code.sort2"] is None else row["code.sort2"],
                axis=1,
            )
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: "" if row["code.sort3"] is None else row["code.sort3"],
                axis=1,
            )
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""
            part2["fildate"] = part2.apply(
                lambda row: row["fildate"].split()[0], axis=1
            )
            now = datetime.now(_TZINFO).strftime("%Y-%m-%d")
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
        result = pd.concat([part1, part2])
        result = result[
            (result["total_quantity"] != 0)
            | (result["total_sale"] != 0)
            | (result["total_cost"] != 0)
        ]

        return result

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

        otrequireorder = self.data["otrequireorder"]
        otrequireorderline = self.data["otrequireorderline"]
        store = self.data["store"]
        sort = self.data["sort"]
        vendor = self.data["vendor"]
        employee = self.data["employee"]
        goods = self.data["goods"]
        otrequireorder = otrequireorder.drop_duplicates()
        otrequireorderline = otrequireorderline.drop_duplicates()
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
        if not len(part):
            return pd.DataFrame(columns=columns)
        part["source_id"] = self.source_id
        part["cmid"] = self.cmid
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part = part[(part["state"] == 8000) & (part["billtype"] == "门店叫货")]
        part = part.rename(
            columns={
                "billnumber": "order_num",
                "finishtime": "order_date",
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

        stkoutlog = self.data["stkoutlog"]
        stkout = self.data["stkout"]
        stkoutdtl = self.data["stkoutdtl"]
        store = self.data["store"]
        warehouse = self.data["warehouse"]
        sort = self.data["sort"]
        modulestat = self.data["modulestat"]
        stkoutbcklog = self.data["stkoutbcklog"]
        stkoutbck = self.data["stkoutbck"]
        stkoutbckdtl = self.data["stkoutbckdtl"]
        goods = self.data["goods"]
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

        if self.cmid == '43':
            stkoutlog: pd.DataFrame = stkoutlog[
                (stkoutlog["cls"] == "统配出")
                & (stkoutlog["stat"].isin(("700", "720", "740", "320", "340")))
            ]
        else:
            stkoutlog: pd.DataFrame = stkoutlog[
                (stkoutlog["cls"] == "统配出")
                & (stkoutlog["stat"].isin(("1000","320","340","720","740")))
                ]

        stkout: pd.DataFrame = stkout[(stkout["cls"] == "统配出")]
        part1 = (
            stkoutlog.merge(
                stkout,
                how="inner",
                on=["num", "cls"],
                suffixes=("", ".stkout")
            )
            .merge(
                stkoutdtl,
                how="inner",
                on=["num", "cls"],
                suffixes=("", ".stkoutdtl")
            )
            .merge(
                store,
                how="inner",
                left_on=["billto"],
                right_on=["gid"],
                suffixes=("", ".store"),
            )
            .merge(
                modulestat,
                how="inner",
                left_on=["stat.stkout"],
                right_on=["no"],
                suffixes=("", ".modulestat"),
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

            part1 = part1.rename(
                columns={
                    "num": "delivery_num",
                    "time": "delivery_date",
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
                    "statname": "delivery_state",
                    "gid.warehouse": "warehouse_id",
                    "code.warehouse": "warehouse_show_code",
                    "name.warehouse": "warehouse_name",
                    "code.sort1": "foreign_category_lv1",
                    "code.sort2": "foreign_category_lv2",
                    "code.sort3": "foreign_category_lv3",
                }
            )
            part1 = part1[columns]

        if self.cmid == '43':
            stkoutbcklog: pd.DataFrame = stkoutbcklog[
                (stkoutbcklog["cls"] == "统配出退")
                & (stkoutbcklog["stat"].isin(("1000", "1020", "1040", "320", "340")))
            ]
        else:
            stkoutbcklog: pd.DataFrame = stkoutbcklog[
                (stkoutbcklog["cls"] == "统配出退")
                & (stkoutbcklog["stat"].isin(("1000", "1020", "1040")))
                ]

        stkoutbck: pd.DataFrame = stkoutbck[(stkoutbck["cls"] == "统配出退")]

        part2 = (
            stkoutbcklog.merge(
                stkoutbck,
                how="inner",
                on=["num", "cls"],
                suffixes=("", ".stkoutbck"),
            )
            .merge(
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
                modulestat,
                how="inner",
                left_on=["stat.stkoutbck"],
                right_on=["no"],
                suffixes=("", ".modulestat"),
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

            part2 = part2.rename(
                columns={
                    "num": "delivery_num",
                    "time": "delivery_date",
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
                    "statname": "delivery_state",
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
            "origin_order_num",
        ]
        stkin = self.data["stkin"]
        stkindtl = self.data["stkindtl"]
        vendorh = self.data["vendorh"]
        modulestat = self.data["modulestat"]
        goods = self.data["goods"]
        brand = self.data["brand"]
        warehouseh = self.data["warehouseh"]
        stkinbck = self.data["stkinbck"]
        stkinbckdtl = self.data["stkinbckdtl"]

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
        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["purchase_price"] = part1.apply(
                lambda row: row["price"] / row["qpc"], axis=1
            )
            part1["foreign_category_lv1"] = part1.apply(
                lambda row: row["sort"][:2], axis=1
            )
            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["sort"][:4], axis=1
            )
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["sort"][:6], axis=1
            )
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
                    "ordnum": "origin_order_num",
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
        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
            part2["purchase_qty"] = part2.apply(lambda row: -1 * row["qty"], axis=1)
            part2["purchase_total"] = part2.apply(lambda row: -1 * row["total"], axis=1)
            part2["purchase_price"] = part2.apply(
                lambda row: row["price"] / row["qpc"], axis=1
            )
            part2["foreign_category_lv1"] = part2.apply(
                lambda row: row["sort"][:2], axis=1
            )
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["sort"][:4], axis=1
            )
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["sort"][:6], axis=1
            )
            part2["origin_order_num"] = ""
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

    def purchase_order(self):
        columns = [
            "cmid",
            "source_id",
            "order_num",
            "order_date",
            "order_type",
            "dead_date",
            "vendor_id",
            "vendor_show_code",
            "vendor_name",
            "warehouse_code",
            "warehouse_name",
            "foreign_item_id",
            "item_show_code",
            "barcode",
            "item_name",
            "item_unit",
            "order_qty",
            "order_price",
            "order_amount",
            "origin_inv",
            "on_order_qty",
            "stat",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
        ]
        ord = self.data["ord"]
        orddtl = self.data["orddtl"]
        vendorh = self.data["vendorh"]
        modulestat = self.data["modulestat"]
        goods = self.data["goods"]
        warehouseh = self.data["warehouseh"]

        part = (
            ord.merge(
                orddtl, how="inner", on="num", suffixes=("", ".orddtl")
            )
            .merge(
                vendorh,
                how="inner",
                left_on=["vendor"],
                right_on=["gid"],
                suffixes=("", ".vendor"),
            )
            .merge(
                warehouseh,
                how="inner",
                left_on=["raiserwrh"],
                right_on=["gid"],
                suffixes=("", ".warehouse"),
            )
            .merge(
                goods,
                how="inner",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".item"),
            )
            .merge(
                modulestat,
                how="inner",
                left_on=["stat"],
                right_on=["no"],
                suffixes=("", ".stat"),
            )
        )
        if not len(part):
            part = pd.DataFrame(columns=columns)
        else:
            part["dead_date"] = part.apply(
                lambda row: datetime.strptime(row["deaddate"], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'), axis=1
            )
            part["order_price"] = part.apply(
                lambda row: 0 if row["qty"] == 0 else row["total"] / row["qty"], axis=1
            )
            part["on_order_qty"] = part.apply(
                lambda row: 0 if pd.isna(row["ordqty"]) else row["ordqty"], axis=1
            )
            part["foreign_category_lv1"] = part.apply(
                lambda row: row["sort"][:2], axis=1
            )
            part["foreign_category_lv2"] = part.apply(
                lambda row: row["sort"][:4], axis=1
            )
            part["foreign_category_lv3"] = part.apply(
                lambda row: row["sort"][:6], axis=1
            )
            part["foreign_category_lv4"] = ""
            part["foreign_category_lv5"] = ""
            part["cmid"] = self.cmid
            part["source_id"] = self.source_id
            part = part[part["cls"] == "自营进"]
            part['stat'] = part['statname']
            part = part.rename(
                columns={
                    "num": "order_num",
                    "fildate": "order_date",
                    "cls": "order_type",
                    "gid": "vendor_id",
                    "code": "vendor_show_code",
                    "name": "vendor_name",
                    "code.warehouse": "warehouse_code",
                    "name.warehouse": "warehouse_name",
                    "gid.item": "foreign_item_id",
                    "code.item": "item_show_code",
                    "code2": "barcode",
                    "name.item": "item_name",
                    "munit": "item_unit",
                    "qty": "order_qty",
                    "total": "order_amount",
                    "invqty": "origin_inv",
                }
            )
            part = part[columns]
        return part

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
        diralc = self.data["diralc"]
        diralcdtl = self.data["diralcdtl"]
        vendor = self.data["vendor"]
        store = self.data["store"]
        modulestat = self.data["modulestat"]
        goods = self.data["goods"]
        brand = self.data["brand"]

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
                how="inner",
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
        if not len(part):
            return pd.DataFrame(columns=columns)
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

    def sales_promotion(self):
        columns = [
            "source_id",
            "cmid",
            "prom_createtime",
            "prom_begintime",
            "prom_endtime",
            "prom_num",
            "prom_cls",
            "foreign_store_id",
            "store_show_code",
            "store_name",
            "foreign_item_id",
            "item_show_code",
            "item_name",
            "item_unit",
            "origin_rtl_price",
            "prom_price",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
            "remark",
            "audittime",
        ]
        v_prom_gd = self.data["v_prom_gd"]
        goods = self.data["goods"]

        part = v_prom_gd.merge(
            goods,
            how="left",
            left_on=["gdgid"],
            right_on=["gid"],
            suffixes=("", ".goods"),
        )
        if not len(part):
            return pd.DataFrame(columns=columns)
        part["foreign_category_lv1"] = part.apply(lambda row: row["sort"][:2], axis=1)
        part["foreign_category_lv2"] = part.apply(lambda row: row["sort"][:4], axis=1)
        part["foreign_category_lv3"] = part.apply(lambda row: row["sort"][:6], axis=1)
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["remark"] = ""
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id

        part = part.rename(
            columns={
                "createtime": "prom_createtime",
                "begintime": "prom_begintime",
                "endtime": "prom_endtime",
                "num": "prom_num",
                "cls": "prom_cls",
                "storegid": "foreign_store_id",
                "storecode": "store_show_code",
                "storename": "store_name",
                "gdgid": "foreign_item_id",
                "gdcode": "item_show_code",
                "gdname": "item_name",
                "munit": "item_unit",
                "rtlprc": "origin_rtl_price",
                "promprc": "prom_price",
            }
        )

        part = part[columns]
        return part

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
        invxf = self.data["invxf"]
        invxfdtl = self.data["invxfdtl"]
        store = self.data["store"]
        goods = self.data["goods"]
        modulestat = self.data["modulestat"]

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
        if not len(part):
            return pd.DataFrame(columns=columns)
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
        invxf = self.data["invxf"]
        invxfdtl = self.data["invxfdtl"]
        warehouse = self.data["warehouse"]
        goods = self.data["goods"]
        modulestat = self.data["modulestat"]

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



    def store(self):
        store = self.data["store"]
        area = self.data["area"]
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
        if self.source_id == "43YYYYYYYYYYYYY":
            store_position_frame = from_excel_get_position_43()
            position = store.merge(
                store_position_frame, how="left", left_on="code", right_on="id"
            )
            part["lat"] = position["lat"]
            part["lng"] = position["lng"]
        else:
            part["lat"] = None
            part["lng"] = None
        part["create_date"] = datetime.now(_TZINFO)
        part["last_updated"] = datetime.now(_TZINFO)
        part["store_status"] = part["stat"].map({0: '正常', 1: '关店', 2: '暂停营业', 3: '未开业', 4: '正常'})
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
                "circle": "business_area",
            }
        )

        part = part[columns]
        return part

    def goods(self):
        goods = self.data["goods"]
        vendor = self.data["vendor"]
        goodsbusgate = self.data["goodsbusgate"]
        brand = self.data["brand"]

        def num2_convert(row):
            try:
                return float(row["validperiod"])
            except Exception:
                return 0

        goods["validperiod"] = goods.apply(num2_convert, axis=1)

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

        part = (
            goods.merge(
                goodsbusgate,
                how="left",
                left_on=["busgate"],
                right_on=["gid"],
                suffixes=("", ".goodsbusgate"),
            )
            .merge(
                brand,
                how="left",
                left_on=["brand"],
                right_on=["code"],
                suffixes=("", ".brand"),
            )
            .merge(
                vendor,
                how="left",
                left_on=["vdrgid"],
                right_on=["gid"],
                suffixes=("", ".vendor"),
            )
        )
        part["foreign_category_lv1"] = part.apply(lambda row: row["sort"][:2], axis=1)
        part["foreign_category_lv2"] = part.apply(lambda row: row["sort"][:4], axis=1)
        part["foreign_category_lv3"] = part.apply(lambda row: row["sort"][:6], axis=1)
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["cmid"] = self.cmid
        part["last_updated"] = datetime.now(_TZINFO)
        part["isvalid"] = 1

        part = part.rename(
            columns={
                "code2": "barcode",
                "gid": "foreign_item_id",
                "name": "item_name",
                "lstinprc": "lastin_price",
                "rtlprc": "sale_price",
                "munit": "item_unit",
                "name.goodsbusgate": "item_status",
                "validperiod": "warranty",
                "code": "show_code",
                "alc": "allot_method",
                "name.vendor": "supplier_name",
                "code.vendor": "supplier_code",
                "name.brand": "brand_name",
                "createdate": "storage_time",
            }
        )
        part = part[columns]

        part["warranty"] = part.apply(lambda row: int(row["warranty"]), axis=1)
        return part

    def category(self):
        sort = self.data["sort"]
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

    def goods_loss(self):
        if self.source_id == "82YYYYYYYYYYYYY":
            return self.goods_loss_82()
        else:
            return self.goods_loss_43_67_79_80()

    def goods_loss_82(self):
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
        ckdatas = self.data["ckdatas"]

        store = self.data["store"]
        goods = self.data["goods"]
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

        part = ckdatas.merge(
            store,
            how="inner",
            left_on=["store"],
            right_on=["gid"],
            suffixes=("", ".store"),
        ).merge(
            goods,
            how="left",
            left_on=["gdgid"],
            right_on=["gid"],
            suffixes=("", ".goods"),
        )
        if not len(part):
            return pd.DataFrame(columns=columns)
        part["quantity"] = part["qty"]
        part["source_id"] = self.source_id
        part["cmid"] = self.cmid
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""
        part["lossdate"] = part.apply(lambda row: row["fildate"].split()[0], axis=1)

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
                "total": "subtotal",
                "sort1": "foreign_category_lv1",
                "sort2": "foreign_category_lv2",
                "sort3": "foreign_category_lv3",
            }
        )

        part = part[columns]
        return part

    def goods_loss_43_67_79_80(self):
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
        ckdatas = self.data["ckdatas"]

        store = self.data["store"]
        sort = self.data["sort"]
        goods = self.data["goods"]
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

        part = (
            ckdatas.merge(
                store,
                how="inner",
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

    def check_store(self):
        columns = [
            "cmid",
            "source_id",
            "checknum",
            "checkdate",
            "foreign_store_id",
            "store_show_code",
            "store_name",
            "foreign_item_id",
            "item_showcode",
            "barcode",
            "item_name",
            "item_unit",
            "origin_qty",
            "origin_amt",
            "actual_qty",
            "actual_amt",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
        ]
        ckdatas = self.data["ckdatas"]

        store = self.data["store"]
        goods = self.data["goods"]
        sort = self.data["sort"]
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

        part = (
            ckdatas.merge(
                store,
                how="inner",
                left_on=["store"],
                right_on=["gid"],
                suffixes=("", ".store"),
            ).merge(
                goods,
                how="left",
                left_on=["gdgid"],
                right_on=["gid"],
                suffixes=("", ".goods"),
            ).merge(
                sort,
                how="left",
                left_on=["sort1"],
                right_on=["code"],
                suffixes=("", ".sort1"),
            ).merge(
                sort,
                how="left",
                left_on=["sort2"],
                right_on=["code"],
                suffixes=("", ".sort2"),
            ).merge(
                sort,
                how="left",
                left_on=["sort3"],
                right_on=["code"],
                suffixes=("", ".sort3"),
            )
        )
        part = part[(part["stat"] == 3)]
        if not len(part):
            return pd.DataFrame(columns=columns)
        part["source_id"] = self.source_id
        part["cmid"] = self.cmid
        part["foreign_category_lv4"] = ""
        part["foreign_category_lv5"] = ""

        part = part.rename(
            columns={
                "num": "checknum",
                "cktime": "checkdate",
                "gid": "foreign_store_id",
                "code": "store_show_code",
                "name": "store_name",
                "gid.goods": "foreign_item_id",
                "code.goods": "item_showcode",
                "code2": "barcode",
                "name.goods": "item_name",
                "munit": "item_unit",
                "acntqty": "origin_qty",
                "acntrtotal": "origin_amt",
                "qty": "actual_qty",
                "rtotal": "actual_amt",
                "code.sort1": "foreign_category_lv1",
                "code.sort2": "foreign_category_lv2",
                "code.sort3": "foreign_category_lv3",
            }
        )
        part = part[columns]
        return part

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

        ckdatas = self.data["ckdatas"]
        warehouse = self.data["warehouse"]
        goods = self.data["goods"]
        sort = self.data["sort"]

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
        if not len(part):
            return pd.DataFrame(columns=columns)
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

    def vendor(self):
        if self.source_id == "43YYYYYYYYYYYYY":
            return self.vendor_43()
        else:
            return self.vendor_67_79_80()

    def vendor_67_79_80(self):
        columns = [
            "cmid",
            "vendor_id",
            "vendor_show_code",
            "vendor_name",
            "vendor_address",
            "contacts",
            "phone_number",
            "vendor_status",
            "vendor_type",
            "source_id",
            "last_updated",
        ]
        vendor = self.data.get("vendor")
        if len(vendor) == 0:
            return pd.DataFrame(columns=columns)

        vendor["cmid"] = self.cmid
        vendor["source_id"] = self.source_id
        vendor["last_updated"] = str(datetime.now(_TZINFO))

        vendor = vendor.rename(
            columns={
                "gid": "vendor_id",
                "code": "vendor_show_code",
                "name": "vendor_name",
                "address": "vendor_address",
                "contactor": "contacts",
                "ctrphone": "phone_number",
                "vdrstat": "vendor_status",
                "vdrtype": "vendor_type",
            }
        )
        part = vendor[columns]
        return part

    def vendor_43(self):
        columns = [
            "cmid",
            "vendor_id",
            "vendor_show_code",
            "vendor_name",
            "vendor_address",
            "contacts",
            "phone_number",
            "vendor_status",
            "vendor_type",
            "source_id",
            "last_updated",
        ]
        vendor = self.data.get("vendor")
        if len(vendor) == 0:
            return pd.DataFrame(columns=columns)

        vendor["cmid"] = self.cmid
        vendor["source_id"] = self.source_id
        vendor["last_updated"] = str(datetime.now(_TZINFO))

        vendor = vendor.rename(
            columns={
                "gid": "vendor_id",
                "code": "vendor_show_code",
                "name": "vendor_name",
                "address": "vendor_address",
                "contactor": "contacts",
                "tele": "phone_number",
                "vendorstat": "vendor_status",
                "vdrtype": "vendor_type",
            }
        )
        part = vendor[columns]
        return part
