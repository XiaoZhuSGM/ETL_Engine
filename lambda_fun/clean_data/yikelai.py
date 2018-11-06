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


class HongYeCleaner:
    store_id_len_map = {"89": 3}

    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        self.source_id = source_id
        self.cmid = self.source_id.split("Y", 1)[0]
        self.store_id_len = self.store_id_len_map[self.cmid]
        self.date = date
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

    def _goodsclass_subquery_1(self):
        inf_goodsclass = self.data["inf_goodsclass"]

        subquery = (
            inf_goodsclass.merge(
                inf_goodsclass,
                how="left",
                left_on=["fatherclass"],
                right_on=["classcode"],
                suffixes=("", ".lv3"),
            )
            .merge(
                inf_goodsclass,
                how="left",
                left_on=["fatherclass.lv3"],
                right_on=["classcode"],
                suffixes=("", ".lv2"),
            )
            .merge(
                inf_goodsclass,
                how="left",
                left_on=["fatherclass.lv2"],
                right_on=["classcode"],
                suffixes=("", ".lv1"),
            )
        )
        subquery = subquery[subquery["classgrade"] == 4]
        subquery_columns = [
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
            "foreign_category_lv4",
            "foreign_category_lv4_name",
        ]
        subquery = subquery.rename(
            columns={
                "classcode.lv1": "foreign_category_lv1",
                "classname.lv1": "foreign_category_lv1_name",
                "classcode.lv2": "foreign_category_lv2",
                "classname.lv2": "foreign_category_lv2_name",
                "classcode.lv3": "foreign_category_lv3",
                "classname.lv3": "foreign_category_lv3_name",
                "classcode": "foreign_category_lv4",
                "classname": "foreign_category_lv4_name",
            }
        )
        subquery = subquery[subquery_columns]
        return subquery

    def _goodsclass_subquery_2(self):
        inf_goodsclass = self.data["inf_goodsclass"]
        subquery = inf_goodsclass.merge(
            inf_goodsclass,
            how="left",
            left_on=["fatherclass"],
            right_on=["classcode"],
            suffixes=("", ".lv2"),
        ).merge(
            inf_goodsclass,
            how="left",
            left_on=["fatherclass.lv2"],
            right_on=["classcode"],
            suffixes=("", ".lv1"),
        )
        subquery = subquery[subquery["classgrade"] == 3]
        subquery_columns = [
            "foreign_category_lv1",
            "foreign_category_lv1_name",
            "foreign_category_lv2",
            "foreign_category_lv2_name",
            "foreign_category_lv3",
            "foreign_category_lv3_name",
        ]
        subquery = subquery.rename(
            columns={
                "classcode.lv1": "foreign_category_lv1",
                "classname.lv1": "foreign_category_lv1_name",
                "classcode.lv2": "foreign_category_lv2",
                "classname.lv2": "foreign_category_lv2_name",
                "classcode": "foreign_category_lv3",
                "classname": "foreign_category_lv3_name",
            }
        )
        subquery = subquery[subquery_columns]
        return subquery

    def goodsflow(self):
        bil_goodsflow = self.data["bil_goodsflow"]
        inf_shop_message = self.data["inf_shop_message"]
        inf_goods = self.data["inf_goods"]

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
            "pos_id"
        ]
        subquery1 = self._goodsclass_subquery_1()
        bil_goodsflow["deptcode"] = bil_goodsflow.deptcode.apply(lambda x: x[:3])
        part1 = (
            bil_goodsflow.merge(
                inf_shop_message,
                how="left",
                left_on=["deptcode"],
                right_on=["deptcode"],
                suffixes=("", ".inf_shop_message"),
            )
            .merge(
                inf_goods,
                how="left",
                left_on=["gdsincode"],
                right_on=["gdsincode"],
                suffixes=("", ".inf_goods"),
            )
            .merge(
                subquery1,
                how="inner",
                left_on=["classcode"],
                right_on=["foreign_category_lv4"],
                suffixes=("", ".lv"),
            )
        )
        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
            part1 = part1[
                (~part1["flowno"].str.contains("NNN", regex=False))
                & (part1["deptcode"].notnull())
                & (part1["gdsincode"].notnull())
            ]
            part1["cmid"] = self.cmid
            part1["source_id"] = self.source_id
            part1["consumer_id"] = ""
            part1["last_updated"] = datetime.now(_TZINFO)
            part1["saleprice"] = part1.apply(
                lambda row: 0
                if row["amount"] == 0
                else row["actualpay"] / row["amount"],
                axis=1,
            )
            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part1["foreign_category_lv4"] = part1.apply(
                lambda row: row["foreign_category_lv3"] + row["foreign_category_lv4"],
                axis=1,
            )
            part1["foreign_category_lv5"] = ""
            part1["foreign_category_lv5_name"] = None
            part1["pos_id"] = ""

            part1 = part1.rename(
                columns={
                    "deptcode": "foreign_store_id",
                    "shotname": "store_name",
                    "flowno": "receipt_id",
                    "recorddate": "saletime",
                    "gdsincode": "foreign_item_id",
                    "stripecode": "barcode",
                    "gdsname": "item_name",
                    "baseunit": "item_unit",
                    "amount": "quantity",
                    "actualpay": "subtotal",
                }
            )
            part1 = part1[columns]

        subquery2 = self._goodsclass_subquery_2()
        bil_goodsflow["deptcode"] = bil_goodsflow.deptcode.apply(lambda x: x[:3])

        part2 = (
            bil_goodsflow.merge(
                inf_shop_message,
                how="left",
                left_on=["deptcode"],
                right_on=["deptcode"],
                suffixes=("", ".inf_shop_message"),
            )
            .merge(
                inf_goods,
                how="left",
                left_on=["gdsincode"],
                right_on=["gdsincode"],
                suffixes=("", ".inf_goods"),
            )
            .merge(
                subquery2,
                how="inner",
                left_on=["classcode"],
                right_on=["foreign_category_lv3"],
                suffixes=("", ".lv"),
            )
        )

        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
            part2 = part2[
                (~part2["flowno"].str.contains("NNN", regex=False))
                & (part2["deptcode"].notnull())
                & (part2["gdsincode"].notnull())
            ]
            part2["cmid"] = self.cmid
            part2["source_id"] = self.source_id
            part2["consumer_id"] = ""
            part2["last_updated"] = datetime.now(_TZINFO)
            part2["saleprice"] = part2.apply(
                lambda row: 0
                if row["amount"] == 0
                else row["actualpay"] / row["amount"],
                axis=1,
            )
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv4_name"] = None
            part2["foreign_category_lv5"] = ""
            part2["foreign_category_lv5_name"] = None
            part2["pos_id"] = ""

            part2 = part2.rename(
                columns={
                    "deptcode": "foreign_store_id",
                    "shotname": "store_name",
                    "flowno": "receipt_id",
                    "recorddate": "saletime",
                    "gdsincode": "foreign_item_id",
                    "stripecode": "barcode",
                    "gdsname": "item_name",
                    "baseunit": "item_unit",
                    "amount": "quantity",
                    "actualpay": "subtotal",
                }
            )
            part2 = part2[columns]

        return pd.concat([part1, part2])

    def cost(self):
        rep_goods_sale = self.data["rep_goods_sale"]
        inf_goods = self.data["inf_goods"]
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

        subquery1 = self._goodsclass_subquery_1()

        part1 = rep_goods_sale.merge(
            inf_goods, how="left", on=["gdsincode"], suffixes=("", ".inf_goods")
        ).merge(
            subquery1,
            how="inner",
            left_on=["classcode"],
            right_on=["foreign_category_lv4"],
            suffixes=("", ".lv"),
        )
        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
            part1 = part1.groupby(
                [
                    "deptcode",
                    "gdsincode",
                    "recorddate",
                    "foreign_category_lv1",
                    "foreign_category_lv2",
                    "foreign_category_lv3",
                    "foreign_category_lv4",
                ],
                as_index=False,
            ).agg(
                {
                    "totalamount": np.sum,
                    "totalsalemoney": np.sum,
                    "totalinmoney": np.sum,
                }
            )

            part1["cmid"] = self.cmid
            part1["source_id"] = self.source_id
            part1["foreign_store_id"] = part1.apply(
                lambda row: row["deptcode"][: self.store_id_len], axis=1
            )
            part1["cost_type"] = ""
            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part1["foreign_category_lv4"] = part1.apply(
                lambda row: row["foreign_category_lv3"] + row["foreign_category_lv4"],
                axis=1,
            )
            part1["foreign_category_lv5"] = ""

            part1 = part1.rename(
                columns={
                    "gdsincode": "foreign_item_id",
                    "recorddate": "date",
                    "totalamount": "total_quantity",
                    "totalsalemoney": "total_sale",
                    "totalinmoney": "total_cost",
                }
            )
            part1 = part1[columns]

        subquery2 = self._goodsclass_subquery_2()
        part2 = rep_goods_sale.merge(
            inf_goods, how="left", on=["gdsincode"], suffixes=("", ".inf_goods")
        ).merge(
            subquery2,
            how="inner",
            left_on=["classcode"],
            right_on=["foreign_category_lv3"],
            suffixes=("", ".lv"),
        )
        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
            part2 = part2.groupby(
                [
                    "deptcode",
                    "gdsincode",
                    "recorddate",
                    "foreign_category_lv1",
                    "foreign_category_lv2",
                    "foreign_category_lv3",
                ],
                as_index=False,
            ).agg(
                {
                    "totalamount": np.sum,
                    "totalsalemoney": np.sum,
                    "totalinmoney": np.sum,
                }
            )

            part2["cmid"] = self.cmid
            part2["source_id"] = self.source_id
            part2["foreign_store_id"] = part2.apply(
                lambda row: row["deptcode"][: self.store_id_len], axis=1
            )
            part2["cost_type"] = ""
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""
            part2 = part2.rename(
                columns={
                    "gdsincode": "foreign_item_id",
                    "recorddate": "date",
                    "totalamount": "total_quantity",
                    "totalsalemoney": "total_sale",
                    "totalinmoney": "total_cost",
                }
            )
            part2 = part2[columns]
        return pd.concat([part1, part2])

    def goods(self):
        inf_goods = self.data["inf_goods"]
        inf_goods_salecircle = self.data["inf_goods_salecircle"]
        inf_brand = self.data["inf_brand"]
        inf_tradeunit = self.data["inf_tradeunit"]

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
        subquery1 = self._goodsclass_subquery_1()
        part1 = (
            inf_goods.merge(
                inf_goods_salecircle,
                how="left",
                left_on=["salecircle"],
                right_on=["circlevalue"],
                suffixes=("", ".inf_goods_salecircle"),
            )
            .merge(inf_brand, how="left", on=["brandcode"], suffixes=("", ".inf_brand"))
            .merge(
                inf_tradeunit,
                how="left",
                left_on=["lastsupplier"],
                right_on=["unitcode"],
                suffixes=("", ".inf_tradeunit"),
            )
            .merge(
                subquery1,
                how="inner",
                left_on=["classcode"],
                right_on=["foreign_category_lv4"],
                suffixes=("", ".lv"),
            )
        )
        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["cmid"] = self.cmid
            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part1["foreign_category_lv4"] = part1.apply(
                lambda row: row["foreign_category_lv3"] + row["foreign_category_lv4"],
                axis=1,
            )
            part1["foreign_category_lv5"] = ""
            part1["storage_time"] = datetime.now(_TZINFO)
            part1["last_updated"] = datetime.now(_TZINFO)
            part1["isvalid"] = "1"
            part1["show_code"] = part1["gdsincode"]
            part1["allot_method"] = None

            part1 = part1.rename(
                columns={
                    "stripecode": "barcode",
                    "gdsincode": "foreign_item_id",
                    "gdsname": "item_name",
                    "lastinprice": "lastin_price",
                    "saleprice": "sale_price",
                    "baseunit": "item_unit",
                    "circlename": "item_status",
                    "qcdays": "warranty",
                    "unitname": "supplier_name",
                    "unitcode": "supplier_code",
                    "brand": "brand_name",
                }
            )
            part1 = part1[columns]

        subquery2 = self._goodsclass_subquery_2()
        part2 = (
            inf_goods.merge(
                inf_goods_salecircle,
                how="left",
                left_on=["salecircle"],
                right_on=["circlevalue"],
                suffixes=("", ".inf_goods_salecircle"),
            )
            .merge(inf_brand, how="left", on=["brandcode"], suffixes=("", ".inf_brand"))
            .merge(
                inf_tradeunit,
                how="left",
                left_on=["lastsupplier"],
                right_on=["unitcode"],
                suffixes=("", ".inf_tradeunit"),
            )
            .merge(
                subquery2,
                how="inner",
                left_on=["classcode"],
                right_on=["foreign_category_lv3"],
                suffixes=("", ".lv"),
            )
        )
        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
            part2["cmid"] = self.cmid
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""
            part2["storage_time"] = datetime.now(_TZINFO)
            part2["last_updated"] = datetime.now(_TZINFO)
            part2["isvalid"] = "1"
            part2["show_code"] = part2["gdsincode"]
            part2["allot_method"] = None
            part2 = part2.rename(
                columns={
                    "stripecode": "barcode",
                    "gdsincode": "foreign_item_id",
                    "gdsname": "item_name",
                    "lastinprice": "lastin_price",
                    "saleprice": "sale_price",
                    "baseunit": "item_unit",
                    "circlename": "item_status",
                    "qcdays": "warranty",
                    "unitname": "supplier_name",
                    "unitcode": "supplier_code",
                    "brand": "brand_name",
                }
            )
            part2 = part2[columns]

        result = pd.concat([part1, part2])

        result['supplier_code'] = result['supplier_code'].str.strip()

        return pd.concat([part1, part2])

    def category(self):
        inf_goodsclass = self.data["inf_goodsclass"]

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
        part1 = inf_goodsclass[inf_goodsclass["classgrade"] == 1].copy()
        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["cmid"] = self.cmid
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
                    "classgrade": "level",
                    "classcode": "foreign_category_lv1",
                    "classname": "foreign_category_lv1_name",
                }
            )
            part1 = part1[columns]

        part2 = inf_goodsclass.merge(
            inf_goodsclass,
            how="left",
            left_on=["fatherclass"],
            right_on=["classcode"],
            suffixes=("", ".lv1"),
        )
        part2 = part2[part2["classgrade"] == 2]
        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
            part2["cmid"] = self.cmid
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["classcode.lv1"] + row["classcode"], axis=1
            )
            part2["foreign_category_lv3"] = ""
            part2["foreign_category_lv3_name"] = None
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv4_name"] = None
            part2["foreign_category_lv5"] = ""
            part2["foreign_category_lv5_name"] = None
            part2["last_updated"] = datetime.now(_TZINFO)
            part2 = part2.rename(
                columns={
                    "classgrade": "level",
                    "classcode.lv1": "foreign_category_lv1",
                    "classname.lv1": "foreign_category_lv1_name",
                    "classname": "foreign_category_lv2_name",
                }
            )
            part2 = part2[columns]

        part3 = inf_goodsclass.merge(
            inf_goodsclass,
            how="left",
            left_on=["fatherclass"],
            right_on=["classcode"],
            suffixes=("", ".lv2"),
        ).merge(
            inf_goodsclass,
            how="left",
            left_on=["fatherclass.lv2"],
            right_on=["classcode"],
            suffixes=("", ".lv1"),
        )
        part3 = part3[part3["classgrade"] == 3]
        if not len(part3):
            part3 = pd.DataFrame(columns=columns)
        else:
            part3["cmid"] = self.cmid
            part3["foreign_category_lv2"] = part3.apply(
                lambda row: row["classcode.lv1"] + row["classcode.lv2"], axis=1
            )
            part3["foreign_category_lv3"] = part3.apply(
                lambda row: row["foreign_category_lv2"] + row["classcode"], axis=1
            )
            part3["foreign_category_lv4"] = ""
            part3["foreign_category_lv4_name"] = None
            part3["foreign_category_lv5"] = ""
            part3["foreign_category_lv5_name"] = None
            part3["last_updated"] = datetime.now(_TZINFO)
            part3 = part3.rename(
                columns={
                    "classgrade": "level",
                    "classcode.lv1": "foreign_category_lv1",
                    "classname.lv1": "foreign_category_lv1_name",
                    "classname.lv2": "foreign_category_lv2_name",
                    "classname": "foreign_category_lv3_name",
                }
            )
            part3 = part3[columns]

        part4 = (
            inf_goodsclass.merge(
                inf_goodsclass,
                how="left",
                left_on=["fatherclass"],
                right_on=["classcode"],
                suffixes=("", ".lv3"),
            )
            .merge(
                inf_goodsclass,
                how="left",
                left_on=["fatherclass.lv3"],
                right_on=["classcode"],
                suffixes=("", ".lv2"),
            )
            .merge(
                inf_goodsclass,
                how="left",
                left_on=["fatherclass.lv2"],
                right_on=["classcode"],
                suffixes=("", ".lv1"),
            )
        )
        part4 = part4[part4["classgrade"] == 4]
        if not len(part4):
            part4 = pd.DataFrame(columns=columns)
        else:
            part4["cmid"] = self.cmid
            part4["foreign_category_lv2"] = part4.apply(
                lambda row: row["classcode.lv1"] + row["classcode.lv2"], axis=1
            )
            part4["foreign_category_lv3"] = part4.apply(
                lambda row: row["foreign_category_lv2"] + row["classcode.lv3"], axis=1
            )
            part4["foreign_category_lv4"] = part4.apply(
                lambda row: row["foreign_category_lv3"] + row["classcode"], axis=1
            )
            part4["foreign_category_lv5"] = ""
            part4["foreign_category_lv5_name"] = None
            part4["last_updated"] = datetime.now(_TZINFO)
            part4 = part4.rename(
                columns={
                    "classgrade": "level",
                    "classcode.lv1": "foreign_category_lv1",
                    "classname.lv1": "foreign_category_lv1_name",
                    "classname.lv2": "foreign_category_lv2_name",
                    "classname.lv3": "foreign_category_lv3_name",
                    "classname": "foreign_category_lv4_name",
                }
            )
            part4 = part4[columns]

        return pd.concat([part1, part2, part3, part4])

    def sales_target(self):
        columns = [
            "source_id",
            "cmid",
            "target_date",
            "foreign_store_id",
            "store_show_code",
            "store_name",
            "target_sales",
            "target_gross_profit",
            "category_level",
            "foreign_category_lv1",
            "foreign_category_lv2",
            "foreign_category_lv3",
            "foreign_category_lv4",
            "foreign_category_lv5",
            "last_updated",
        ]
        mbo_classsale = self.data["mbo_classsale"]
        inf_shop_message = self.data["inf_shop_message"]
        inf_goodsclass = self.data["inf_goodsclass"]
        if not len(mbo_classsale):
            return pd.DataFrame(columns=columns)

        part = mbo_classsale.merge(
            inf_shop_message,
            how="left",
            left_on=["shopcode"],
            right_on=["deptcode"],
            suffixes=("", ".inf_shop_message"),
        ).merge(
            inf_goodsclass,
            how="left",
            left_on=["classcode"],
            right_on=["classcode"],
            suffixes=("", ".lv"),
        )
        part["foreign_category_lv1"] = part.apply(
            lambda row: row["path"].split("/")[2], axis=1
        )

        def _foreign_category_lv2(path):
            lst = path.split("/")
            if len(lst) < 4 or lst[3] == "":
                return ""
            else:
                return f"{lst[2]}{lst[3]}"

        part["foreign_category_lv2"] = part.apply(
            lambda row: _foreign_category_lv2(row["path"]), axis=1
        )

        def _foreign_category_lv3(path):
            lst = path.split("/")
            if len(lst) < 5 or lst[4] == "":
                return ""
            else:
                return f"{lst[2]}{lst[3]}{lst[4]}"

        part["foreign_category_lv3"] = part.apply(
            lambda row: _foreign_category_lv3(row["path"]), axis=1
        )

        def _foreign_category_lv4(path):
            lst = path.split("/")
            if len(lst) < 6 or lst[5] == "":
                return ""
            else:
                return f"{lst[2]}{lst[3]}{lst[4]}{lst[5]}"

        part["foreign_category_lv4"] = part.apply(
            lambda row: _foreign_category_lv4(row["path"]), axis=1
        )

        part["foreign_category_lv5"] = ""
        part["last_updated"] = datetime.now(_TZINFO)
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id
        part["store_show_code"] = part["deptcode"]

        part = part.rename(
            columns={
                "recorddate": "target_date",
                "deptcode": "foreign_store_id",
                "shotname": "store_name",
                "objectsale": "target_sales",
                "objectprofit": "target_gross_profit",
                "classgrade": "category_level",
            }
        )

        part = part[columns]
        return part

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

        bil_damagedtl = self.data["bil_damagedtl"]
        inf_shop_message = self.data["inf_shop_message"]
        inf_goods = self.data["inf_goods"]
        if not len(bil_damagedtl):
            return pd.DataFrame(columns=columns)
        bil_damagedtl["deptcode_sub"] = bil_damagedtl.apply(
            lambda row: row["deptcode"][: self.store_id_len], axis=1
        )

        subquery1 = self._goodsclass_subquery_1()
        part1 = (
            bil_damagedtl.merge(
                inf_shop_message,
                how="left",
                left_on=["deptcode_sub"],
                right_on=["deptcode"],
                suffixes=("", ".inf_shop_message"),
            )
            .merge(inf_goods, how="left", on=["gdsincode"], suffixes=("", ".inf_goods"))
            .merge(
                subquery1,
                how="inner",
                left_on=["classcode"],
                right_on=["foreign_category_lv4"],
                suffixes=("", ".lv"),
            )
        )
        part1 = part1[~part1["deptcode"].str.contains(r"^998.*$")]
        if not len(part1):
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["cmid"] = self.cmid
            part1["source_id"] = self.source_id
            part1["quantity"] = part1.apply(lambda row: row["amount"] * -1, axis=1)
            part1["subtotal"] = part1.apply(lambda row: row["salemoney"] * -1, axis=1)
            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part1["foreign_category_lv4"] = part1.apply(
                lambda row: row["foreign_category_lv3"] + row["foreign_category_lv4"],
                axis=1,
            )
            part1["foreign_category_lv5"] = ""
            part1["store_show_code"] = part1["deptcode.inf_shop_message"]
            part1["item_showcode"] = part1["gdsincode"]

            part1 = part1.rename(
                columns={
                    "billno": "lossnum",
                    "recorddate": "lossdate",
                    "deptcode.inf_shop_message": "foreign_store_id",
                    "shotname": "store_name",
                    "gdsincode": "foreign_item_id",
                    "stripecode": "barcode",
                    "gdsname": "item_name",
                    "baseunit": "item_unit",
                }
            )
            part1 = part1[columns]

        subquery2 = self._goodsclass_subquery_2()
        part2 = (
            bil_damagedtl.merge(
                inf_shop_message,
                how="left",
                left_on=["deptcode_sub"],
                right_on=["deptcode"],
                suffixes=("", ".inf_shop_message"),
            )
            .merge(inf_goods, how="left", on=["gdsincode"], suffixes=("", ".inf_goods"))
            .merge(
                subquery2,
                how="inner",
                left_on=["classcode"],
                right_on=["foreign_category_lv3"],
                suffixes=("", ".lv"),
            )
        )
        if not len(part2):
            part2 = pd.DataFrame(columns=columns)
        else:
            part2 = part2[~part2["deptcode"].str.contains(r"^998.*$")]
            part2["cmid"] = self.cmid
            part2["source_id"] = self.source_id
            part2["quantity"] = part2.apply(lambda row: row["amount"] * -1, axis=1)
            part2["subtotal"] = part2.apply(lambda row: row["salemoney"] * -1, axis=1)
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["foreign_category_lv1"] + row["foreign_category_lv2"],
                axis=1,
            )
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["foreign_category_lv2"] + row["foreign_category_lv3"],
                axis=1,
            )
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""
            part2["store_show_code"] = part2["deptcode.inf_shop_message"]
            part2["item_showcode"] = part2["gdsincode"]
            part2 = part2.rename(
                columns={
                    "billno": "lossnum",
                    "recorddate": "lossdate",
                    "deptcode.inf_shop_message": "foreign_store_id",
                    "shotname": "store_name",
                    "gdsincode": "foreign_item_id",
                    "stripecode": "barcode",
                    "gdsname": "item_name",
                    "baseunit": "item_unit",
                }
            )
            part2 = part2[columns]
        return pd.concat([part1, part2])

    def store(self):
        inf_shop_message = self.data["inf_shop_message"]
        inf_whole_district = self.data["inf_whole_district"]

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
        part = inf_shop_message.merge(
            inf_whole_district,
            how="left",
            on=["dis_code"],
            suffixes=("", ".inf_whole_district"),
        )
        part["cmid"] = self.cmid
        part["source_id"] = self.source_id
        part["address_code"] = None
        part["device_id"] = None
        part["lat"] = None
        part["lng"] = None
        part["business_area"] = None
        part["property_id"] = None
        part["property"] = ""
        part["last_updated"] = datetime.now(_TZINFO)
        part["show_code"] = part["deptcode"]
        part = part.rename(
            columns={
                "deptcode": "foreign_store_id",
                "shotname": "store_name",
                "address": "store_address",
                "validflag": "store_status",
                "startdate": "create_date",
                "phonecode": "phone_number",
                "manager": "contacts",
                "dis_code": "area_code",
                "dis_name": "area_name",
            }
        )
        part = part[columns]

        part['foreign_store_id'] = part['foreign_store_id'].str.strip()
        part['show_code'] = part['show_code'].str.strip()
        part['area_code'] = part['area_code'].str.strip()

        return part

    def _sub_query_category_lv4(self):
        inf_goodsclass = self.data["inf_goodsclass"]
        lv4 = inf_goodsclass[inf_goodsclass["classgrade"] == 4]

        subquery = lv4.merge(inf_goodsclass, how="left", left_on="fatherclass", right_on="classcode", suffixes=(".lv4", ".lv3"))\
            .merge(inf_goodsclass, how="left", left_on="fatherclass.lv3", right_on="classcode")\
            .merge(inf_goodsclass, how="left", left_on="fatherclass", right_on="classcode", suffixes=(".lv2", ".lv1"))
        subquery = subquery[["classcode.lv1", "classcode.lv2", "classcode.lv3", "classcode.lv4"]]
        subquery = subquery.rename(columns={
            "classcode.lv1": "foreign_category_lv1",
            "classcode.lv2": "foreign_category_lv2",
            "classcode.lv3": "foreign_category_lv3",
            "classcode.lv4": "foreign_category_lv4",
        })
        return subquery

    def _sub_query_category_lv3(self):
        inf_goodsclass = self.data["inf_goodsclass"]
        lv3 = inf_goodsclass[inf_goodsclass["classgrade"] == 3]
        subquery = lv3.merge(inf_goodsclass, how="left", left_on="fatherclass", right_on="classcode", suffixes=(".lv3", ".lv2"))\
            .merge(inf_goodsclass, how="left", left_on="fatherclass.lv2", right_on="classcode")

        subquery = subquery[["classcode", "classcode.lv2", "classcode.lv3"]]
        subquery = subquery.rename(columns={
            "classcode": "foreign_category_lv1",
            "classcode.lv2": "foreign_category_lv2",
            "classcode.lv3": "foreign_category_lv3"
        })
        return subquery

    def delivery(self):
        columns = ["delivery_num", "delivery_date", "delivery_type", "foreign_store_id", "store_show_code",
                   "store_name", "foreign_item_id", "item_show_code", "barcode", "item_name", "item_unit",
                   "delivery_qty", "rtl_price", "rtl_amt", "warehouse_id", "warehouse_show_code", "warehouse_name",
                   "src_type", "delivery_state", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3",
                   "foreign_category_lv4", "foreign_category_lv5", "source_id", "cmid"]
        bil_send = self.data["bil_send"].rename(columns=lambda x: f"bil_send.{x}")
        warehouse = self.data["inf_department"].rename(columns=lambda x: f"warehouse.{x}")
        bil_senddtl= self.data["bil_senddtl"].rename(columns=lambda x: f"bil_senddtl.{x}")
        store_a = self.data["inf_department"].rename(columns=lambda x: f"store_a.{x}")
        store_b = self.data["inf_department"].rename(columns=lambda x: f"store_b.{x}")
        item = self.data["inf_goods"].rename(columns=lambda x: f"item.{x}")

        bil_send["bil_send.deptcode"] = bil_send["bil_send.deptcode"].str.strip()
        bil_send["bil_send.otherdeptcode"] = bil_send["bil_send.otherdeptcode"].str.strip()
        warehouse["warehouse.deptcode"] = warehouse["warehouse.deptcode"].str.strip()
        store_a["store_a.deptcode"] = store_a["store_a.deptcode"].str.strip()
        store_a["store_a.fatherdept"] = store_a["store_a.fatherdept"].str.strip()
        store_b["store_b.deptcode"] = store_b["store_b.deptcode"].str.strip()

        lv = self._sub_query_category_lv4().rename(columns=lambda x: f"lv.{x}")
        part1 = (
            bil_send
            .merge(warehouse, how="left", left_on="bil_send.deptcode", right_on="warehouse.deptcode")
            .merge(bil_senddtl, how="left", left_on="bil_send.billno", right_on="bil_senddtl.billno")
            .merge(store_a, how="left", left_on="bil_send.otherdeptcode", right_on="store_a.deptcode")
            .merge(store_b, how="left", left_on="store_a.fatherdept", right_on="store_b.deptcode")
            .merge(item, how="left", left_on="bil_senddtl.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv4")
        )

        part1 = part1[(part1["bil_send.billtype"] == 1) & (part1["store_b.type"] == 1)]
        if len(part1) == 0:
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["delivery_type"] = "统配出"
            part1["store_show_code"] = part1["store_b.deptcode"]
            part1["item_show_code"] = part1["item.gdsincode"]
            part1["warehouse_show_code"] = part1["warehouse.deptcode"]
            part1["delivery_state"] = part1["bil_send.receiveflag"].apply(lambda x: "未收货" if x == 0 else "已收货")
            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1
            )
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"]
                + row["lv.foreign_category_lv3"], axis=1
            )
            part1["foreign_category_lv4"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] +
                row["lv.foreign_category_lv3"] + row["lv.foreign_category_lv4"], axis=1
            )

            part1["foreign_category_lv5"] = ""
            part1["source_id"] = self.source_id
            part1["cmid"] = self.cmid
            part1 = part1.rename(columns={
                "bil_send.billno": "delivery_num",
                "bil_send.recorddate": "delivery_date",
                "store_b.deptcode": "foreign_store_id",
                "store_b.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "bil_senddtl.amount": "delivery_qty",
                "item.saleprice": "rtl_price",
                "bil_senddtl.salemoney": "rtl_amt",
                "warehouse.deptcode": "warehouse_id",
                "warehouse.deptname": "warehouse_name",
                "bil_send.comment": "src_type",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part1 = part1[columns]

        lv = self._sub_query_category_lv3().rename(columns=lambda x: f"lv.{x}")
        part2 = (
            bil_send
            .merge(warehouse, how="left", left_on="bil_send.deptcode", right_on="warehouse.deptcode")
            .merge(bil_senddtl, how="left", left_on="bil_send.billno", right_on="bil_senddtl.billno")
            .merge(store_a, how="left", left_on="bil_send.otherdeptcode", right_on="store_a.deptcode")
            .merge(store_b, how="left", left_on="store_a.fatherdept", right_on="store_b.deptcode")
            .merge(item, how="left", left_on="bil_senddtl.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv3")
        )

        part2 = part2[(part2["bil_send.billtype"] == 1) & part2["store_b.type"] == 1]
        if len(part2) == 0:
            part2 = pd.DataFrame(columns=columns)
        else:
            part2["delivery_type"] = "统配出"
            part2["store_show_code"] = part2["store_b.deptcode"]
            part2["item_show_code"] = part2["item.gdsincode"]
            part2["warehouse_show_code"] = part2["warehouse.deptcode"]
            part2["delivery_state"] = part2["bil_send.receiveflag"].apply(lambda x: "未收货" if x == 0 else "已收货")
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1
            )
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] +
                row["lv.foreign_category_lv3"], axis=1
            )
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""
            part2["source_id"] = self.source_id
            part2["cmid"] = self.cmid
            part2 = part2.rename(columns={
                "bil_send.billno": "delivery_num",
                "bil_send.recorddate": "delivery_date",
                "store_b.deptcode": "foreign_store_id",
                "store_b.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "bil_senddtl.amount": "delivery_qty",
                "item.saleprice": "rtl_price",
                "bil_senddtl.salemoney": "rtl_amt",
                "warehouse.deptcode": "warehouse_id",
                "warehouse.deptname": "warehouse_name",
                "bil_send.comment": "src_type",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part2 = part2[columns]

        bil_send["store.deptcode"] = bil_send["bil_send.deptcode"].apply(lambda x: x[:3])
        lv = self._sub_query_category_lv4().rename(columns=lambda x: f"lv.{x}")
        part3 = (
            bil_send
            .merge(warehouse, how="left", left_on="bil_send.otherdeptcode", right_on="warehouse.deptcode")
            .merge(bil_senddtl, how="left", left_on="bil_send.billno", right_on="bil_senddtl.billno")
            .merge(store_a, how="left", left_on="bil_send.otherdeptcode", right_on="store_a.deptcode")
            .merge(store_b, how="left", left_on="store_a.fatherdept", right_on="store_b.deptcode")
            .merge(item, how="left", left_on="bil_senddtl.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv4")
        )

        part3 = part3[(part3["bil_send.billtype"] == 2) & (part3["store_b.type"] == 1)]
        if len(part3) == 0:
            part3 = pd.DataFrame(columns=columns)
        else:
            part3["delivery_type"] = "统配出退"
            part3["store_show_code"] = part3["store_b.deptcode"]
            part3["item_show_code"] = part3["item.gdsincode"]
            part3["delivery_qty"] = part3["bil_senddtl.amount"].apply(lambda x: -1 * x)
            part3["rtl_amt"] = part3["bil_senddtl.salemoney"].apply(lambda x: -1 * x)

            part3["warehouse_show_code"] = part3["warehouse.deptcode"]
            part3["delivery_state"] = part3["bil_send.receiveflag"].apply(lambda x: "未收货" if x == 0 else "已收货")
            part3["foreign_category_lv2"] = part3.apply(
                lambda row: row["lv.foreign_category_lv1"] +
                                                                    row["lv.foreign_category_lv2"], axis=1)
            part3["foreign_category_lv3"] = part3.apply(lambda row: row["lv.foreign_category_lv1"] +
                                                                    row["lv.foreign_category_lv2"] +
                                                                    row["lv.foreign_category_lv3"], axis=1)
            part3["foreign_category_lv4"] = part3.apply(lambda row: row["lv.foreign_category_lv1"] +
                                                                    row["lv.foreign_category_lv2"] +
                                                                    row["lv.foreign_category_lv3"] +
                                                                    row["lv.foreign_category_lv4"], axis=1)

            part3["foreign_category_lv5"] = ""
            part3["source_id"] = self.source_id
            part3["cmid"] = self.cmid
            part3 = part3.rename(columns={
                "bil_send.billno": "delivery_num",
                "bil_send.recorddate": "delivery_date",
                "store_b.deptcode": "foreign_store_id",
                "store_b.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "item.saleprice": "rtl_price",
                "warehouse.deptcode": "warehouse_id",
                "warehouse.deptname": "warehouse_name",
                "bil_send.comment": "src_type",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part3 = part3[columns]

        lv = self._sub_query_category_lv3().rename(columns=lambda x: f"lv.{x}")
        part4 = (
            bil_send
            .merge(warehouse, how="left", left_on="bil_send.otherdeptcode", right_on="warehouse.deptcode")
            .merge(bil_senddtl, how="left", left_on="bil_send.billno", right_on="bil_senddtl.billno")
            .merge(store_a, how="left", left_on="bil_send.otherdeptcode", right_on="store_a.deptcode")
            .merge(store_b, how="left", left_on="store_a.fatherdept", right_on="store_b.deptcode")
            .merge(item, how="left", left_on="bil_senddtl.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv3")
        )

        part4 = part4[(part4["bil_send.billtype"] == 2) & (part4["store_b.type"] == 1)]
        if len(part4) == 0:
            part4 = pd.DataFrame(columns=columns)
        else:
            part4["delivery_type"] = "统配出退"
            part4["store_show_code"] = part4["store_b.deptcode"]
            part4["item_show_code"] = part4["item.gdsincode"]
            part4["delivery_qty"] = part4["bil_senddtl.amount"].apply(lambda x: -1 * x)
            part4["rtl_amt"] = part4["bil_senddtl.salemoney"].apply(lambda x: -1 * x)

            part4["warehouse_show_code"] = part4["warehouse.deptcode"]
            part4["delivery_state"] = part4["bil_send.receiveflag"].apply(lambda x: "未收货" if x == 0 else "已收货")
            part4["foreign_category_lv2"] = part4.apply(lambda row: row["lv.foreign_category_lv1"] +
                                                                    row["lv.foreign_category_lv2"], axis=1)
            part4["foreign_category_lv3"] = part4.apply(lambda row: row["lv.foreign_category_lv1"] +
                                                                    row["lv.foreign_category_lv2"] +
                                                                    row["lv.foreign_category_lv3"], axis=1)
            part4["foreign_category_lv4"] = ""
            part4["foreign_category_lv5"] = ""
            part4["source_id"] = self.source_id
            part4["cmid"] = self.cmid
            part4 = part4.rename(columns={
                "bil_send.billno": "delivery_num",
                "bil_send.recorddate": "delivery_date",
                "store_b.deptcode": "foreign_store_id",
                "store_b.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "item.saleprice": "rtl_price",
                "warehouse.deptcode": "warehouse_id",
                "warehouse.deptname": "warehouse_name",
                "bil_send.comment": "src_type",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part4 = part4[columns]

        return pd.concat([part1, part2, part3, part4])

    def purchase_warehouse(self):
        columns = [
            "source_id", "cmid", "purchase_num", "purchase_date", "purchase_type", "foreign_item_id", "item_show_code",
            "barcode", "item_name", "item_unit", "purchase_qty", "purchase_price", "purchase_total", "vendor_id",
            "vendor_show_code", "vendor_name", "brand_code", "brand_name", "warehouse_code", "warehouse_name",
            "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
            "foreign_category_lv5", "bill_status"
        ]
        header = self.data["bil_inorder"].rename(columns=lambda x: f"header.{x}")
        detail = self.data["bil_inorderdtl"].rename(columns=lambda x: f"detail.{x}")
        warehouse = self.data["inf_department"].rename(columns=lambda x: f"warehouse.{x}")
        item = self.data["inf_goods"].rename(columns=lambda x: f"item.{x}")
        brand = self.data["inf_brand"].rename(columns=lambda x: f"brand.{x}")
        vendor = self.data["inf_tradeunit"].rename(columns=lambda x: f"vendor.{x}")
        store = self.data["inf_department"].rename(columns=lambda x: f"store.{x}")

        item["item.gdsincode"] = item["item.gdsincode"].str.strip()
        vendor["vendor.unitcode"] = vendor["vendor.unitcode"].str.strip()
        warehouse["warehouse.deptcode"] = warehouse["warehouse.deptcode"].str.strip()
        warehouse["warehouse.deptname"] = warehouse["warehouse.deptname"].str.strip()
        store["store.deptcode"] = store["store.deptcode"].str.strip()
        store["store.deptname"] = store["store.deptname"].str.strip()
        header["header.deptcode"] = header["header.deptcode"].str.strip()
        detail["detail.gdscode"] = detail["detail.gdscode"].str.strip()
        detail["detail.suppliercode"] = detail["detail.suppliercode"].str.strip()

        lv = self._sub_query_category_lv4().rename(columns=lambda x: f"lv.{x}")
        part1 = (
            header
            .merge(detail, left_on="header.orderno", right_on="detail.orderno")
            .merge(warehouse, left_on="header.deptcode", right_on="warehouse.deptcode")
            .merge(store, left_on="detail.deptcode", right_on="store.deptcode")
            .merge(item, how="left", left_on="detail.gdscode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv4")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )
        part1 = part1[part1["warehouse.type"] == 5]

        if len(part1) == 0:
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["source_id"] = self.source_id
            part1["cmid"] = self.cmid

            def generate_purchase_type(row):
                if row == 1:
                    return "普通订单"
                elif row == 2:
                    return "赠品订单"
                elif row == 3:
                    return "特价订单"
                elif row == 4:
                    return "首次订单"
                elif row == 5:
                    return "永续订单"
                else:
                    return "大订单"
            part1["purchase_type"] = part1["header.ordertype"].apply(generate_purchase_type)
            part1["item_show_code"] = part1["item.gdsincode"]
            part1["vendor_show_code"] = part1["vendor.unitcode"]
            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row["lv.foreign_category_lv3"]
                , axis=1
            )
            part1["foreign_category_lv4"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] +
                row["lv.foreign_category_lv3"] + row["lv.foreign_category_lv4"], axis=1
            )
            part1["foreign_category_lv5"] = ""

            def generate_bill_status(row):
                if row == 0:
                    return "尚未到货"
                elif row == 2:
                    return "已经到货"
                elif row == 11:
                    return "作废"
            part1["bill_status"] = part1["header.orderflag"].apply(generate_bill_status)
            part1 = part1.rename(columns={
                "header.orderno": "purchase_num",
                "header.recorddate": "purchase_date",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.amount": "purchase_qty",
                "detail.inprice": "purchase_price",
                "detail.inmoneyio": "purchase_total",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "store.deptcode": "warehouse_code",
                "store.deptname": "warehouse_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part1 = part1[columns]

        lv = self._sub_query_category_lv3().rename(columns=lambda x: f"lv.{x}")
        part2 = (
            header
            .merge(detail, left_on="header.orderno", right_on="detail.orderno")
            .merge(warehouse, left_on="header.deptcode", right_on="warehouse.deptcode")
            .merge(store, left_on="detail.deptcode", right_on="store.deptcode")
            .merge(item, how="left", left_on="detail.gdscode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv3")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )
        part2 = part2[part2["warehouse.type"] == 5]

        if len(part2) == 0:
            part2 = pd.DataFrame(columns=columns)
        else:
            part2["source_id"] = self.source_id
            part2["cmid"] = self.cmid

            def generate_purchase_type(row):
                if row == 1:
                    return "普通订单"
                elif row == 2:
                    return "赠品订单"
                elif row == 3:
                    return "特价订单"
                elif row == 4:
                    return "首次订单"
                elif row == 5:
                    return "永续订单"
                else:
                    return "大订单"

            part2["purchase_type"] = part2["header.ordertype"].apply(generate_purchase_type)
            part2["item_show_code"] = part2["item.gdsincode"]
            part2["vendor_show_code"] = part2["vendor.unitcode"]
            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row["lv.foreign_category_lv3"]
                , axis=1
            )
            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""

            def generate_bill_status(row):
                if row == 0:
                    return "尚未到货"
                elif row == 2:
                    return "已经到货"
                elif row == 11:
                    return "作废"

            part2["bill_status"] = part2["header.orderflag"].apply(generate_bill_status)
            part2 = part2.rename(columns={
                "header.orderno": "purchase_num",
                "header.recorddate": "purchase_date",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.amount": "purchase_qty",
                "detail.inprice": "purchase_price",
                "detail.inmoneyio": "purchase_total",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "store.deptcode": "warehouse_code",
                "store.deptname": "warehouse_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part2 = part2[columns]

        header = self.data["bil_returnfac"].rename(columns=lambda x: f"header.{x}")
        detail = self.data["bil_returnfacdtl"].rename(columns=lambda x: f"detail.{x}")
        detail["detail.gdsincode"] = detail["detail.gdsincode"].str.strip()
        detail["detail.suppliercode"] = detail["detail.suppliercode"].str.strip()
        lv = self._sub_query_category_lv4().rename(columns=lambda x: f"lv.{x}")
        part3 = (
            header
            .merge(detail, left_on="header.billno", right_on="detail.billno")
            .merge(warehouse, left_on="header.deptcode", right_on="warehouse.deptcode")
            .merge(item, how="left", left_on="detail.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv4")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )
        part3 = part3[part3["warehouse.type"] == 4]
        if len(part3) == 0:
            part3 = pd.DataFrame(columns=columns)
        else:
            part3["source_id"] = self.source_id
            part3["cmid"] = self.cmid
            part3["purchase_type"] = "大仓退货单"
            part3["item_show_code"] = part3["item.gdsincode"]
            part3["purchase_qty"] = part3["detail.returnamount"].apply(lambda x: -1 * x)
            part3["purchase_total"] = part3["detail.actualreturnmoney"].apply(lambda x: -1 * x)
            part3["vendor_show_code"] = part3["vendor.unitcode"]

            part3["foreign_category_lv2"] = part3.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part3["foreign_category_lv3"] = part3.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row["lv.foreign_category_lv3"]
                , axis=1
            )
            part3["foreign_category_lv4"] = part3.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] +
                row["lv.foreign_category_lv3"] + row["lv.foreign_category_lv4"], axis=1
            )
            part3["foreign_category_lv5"] = ""

            def generate_bill_status(row):
                if row == 0:
                    return "未审核"
                elif row == 1:
                    return "已审核"
                elif row == 11:
                    return "作废"

            part3["bill_status"] = part3["header.dealflag"].apply(generate_bill_status)
            part3 = part3.rename(columns={
                "header.billno": "purchase_num",
                "header.recorddate": "purchase_date",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.actualinprice": "purchase_price",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "warehouse.deptcode": "warehouse_code",
                "warehouse.deptname": "warehouse_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part3 = part3[columns]

        lv = self._sub_query_category_lv3().rename(columns=lambda x: f"lv.{x}")
        part4 = (
            header
            .merge(detail, left_on="header.billno", right_on="detail.billno")
            .merge(warehouse, left_on="header.deptcode", right_on="warehouse.deptcode")
            .merge(item, how="left", left_on="detail.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv3")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )
        part4 = part4[part4["warehouse.type"] == 4]
        if len(part4) == 0:
            part4 = pd.DataFrame(columns=columns)
        else:
            part4["source_id"] = self.source_id
            part4["cmid"] = self.cmid
            part4["purchase_type"] = "大仓退货单"
            part4["item_show_code"] = part4["item.gdsincode"]
            part4["purchase_qty"] = part4["detail.returnamount"].apply(lambda x: -1 * x)
            part4["purchase_total"] = part4["detail.actualreturnmoney"].apply(lambda x: -1 * x)
            part4["vendor_show_code"] = part4["vendor.unitcode"]

            part4["foreign_category_lv2"] = part4.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part4["foreign_category_lv3"] = part4.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row["lv.foreign_category_lv3"]
                , axis=1
            )
            part4["foreign_category_lv4"] = ""
            part4["foreign_category_lv5"] = ""

            def generate_bill_status(row):
                if row == 0:
                    return "未审核"
                elif row == 1:
                    return "已审核"
                elif row == 11:
                    return "作废"

            part4["bill_status"] = part4["header.dealflag"].apply(generate_bill_status)
            part4 = part4.rename(columns={
                "header.billno": "purchase_num",
                "header.recorddate": "purchase_date",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.actualinprice": "purchase_price",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "warehouse.deptcode": "warehouse_code",
                "warehouse.deptname": "warehouse_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part4 = part4[columns]

        return pd.concat([part1, part2, part3, part4])

    def purchase_store(self):
        columns = [
            "source_id", "cmid", "purchase_num", "purchase_date", "purchase_type", "foreign_store_id", "store_show_code"
            , "store_name", "foreign_item_id", "item_show_code", "barcode", "item_name", "item_unit", "purchase_qty",
            "purchase_price", "purchase_total", "vendor_id", "vendor_show_code", "vendor_name", "brand_code",
            "brand_name", "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3",
            "foreign_category_lv4", "foreign_category_lv5", "bill_status"
        ]
        header = self.data["bil_inorder"].rename(columns=lambda x: f"header.{x}")
        detail = self.data["bil_inorderdtl"].rename(columns=lambda x: f"detail.{x}")
        store = self.data["inf_department"].rename(columns=lambda x: f"store.{x}")
        item = self.data["inf_goods"].rename(columns=lambda x: f"item.{x}")
        brand = self.data["inf_brand"].rename(columns=lambda x: f"brand.{x}")
        vendor = self.data["inf_tradeunit"].rename(columns=lambda x: f"vendor.{x}")
        item["item.gdsincode"] = item["item.gdsincode"].str.strip()
        vendor["vendor.unitcode"] = vendor["vendor.unitcode"].str.strip()
        store["store.deptcode"] = store["store.deptcode"].str.strip()
        store["store.deptname"] = store["store.deptname"].str.strip()
        header["header.deptcode"] = header["header.deptcode"].str.strip()
        header["header.orderno"] = header["header.orderno"].str.strip()
        detail["detail.gdscode"] = detail["detail.gdscode"].str.strip()
        detail["detail.suppliercode"] = detail["detail.suppliercode"].str.strip()
        detail["detail.orderno"] = detail["detail.orderno"].str.strip()

        lv = self._sub_query_category_lv4().rename(columns=lambda x: f"lv.{x}")
        part1 = (
            header
            .merge(detail, left_on="header.orderno", right_on="detail.orderno")
            .merge(store, left_on="header.deptcode", right_on="store.deptcode")
            .merge(item, how="left", left_on="detail.gdscode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv4")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )
        part1 = part1[part1["store.type"] == 1]

        if len(part1) == 0:
            part1 = pd.DataFrame(columns=columns)
        else:
            part1["source_id"] = self.source_id
            part1["cmid"] = self.cmid

            def generate_purchase_type(row):
                if row == 1:
                    return "普通订单"
                elif row == 2:
                    return "赠品订单"
                elif row == 3:
                    return "特价订单"
                elif row == 4:
                    return "首次订单"
                elif row == 5:
                    return "永续订单"
                else:
                    return "大订单"

            part1["purchase_type"] = part1["header.ordertype"].apply(generate_purchase_type)

            part1["foreign_category_lv2"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part1["foreign_category_lv3"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row[
                    "lv.foreign_category_lv3"]
                , axis=1
            )
            part1["foreign_category_lv4"] = part1.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] +
                            row["lv.foreign_category_lv3"] + row["lv.foreign_category_lv4"], axis=1
            )
            part1["foreign_category_lv5"] = ""
            part1["store_show_code"] = part1["store.deptcode"]
            part1["item_show_code"] = part1["item.gdsincode"]
            part1["vendor_show_code"] = part1["vendor.unitcode"]

            def generate_bill_status(row):
                if row == 0:
                    return "尚未到货"
                elif row == 2:
                    return "已经到货"
                elif row == 11:
                    return "作废"

            part1["bill_status"] = part1["header.orderflag"].apply(generate_bill_status)
            part1 = part1.rename(columns={
                "header.orderno": "purchase_num",
                "header.recorddate": "purchase_date",
                "store.deptcode": "foreign_store_id",
                "store.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.amount": "purchase_qty",
                "detail.inprice": "purchase_price",
                "detail.inmoneyio": "purchase_total",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part1 = part1[columns]

        lv = self._sub_query_category_lv3().rename(columns=lambda x: f"lv.{x}")
        part2 = (
            header
            .merge(detail, left_on="header.orderno", right_on="detail.orderno")
            .merge(store, left_on="header.deptcode", right_on="store.deptcode")
            .merge(item, how="left", left_on="detail.gdscode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv3")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )

        part2 = part2[part2["store.type"] == 1]
        if len(part2) == 0:
            part2 = pd.DataFrame(columns=columns)
        else:
            part2["source_id"] = self.source_id
            part2["cmid"] = self.cmid

            def generate_purchase_type(row):
                if row == 1:
                    return "普通订单"
                elif row == 2:
                    return "赠品订单"
                elif row == 3:
                    return "特价订单"
                elif row == 4:
                    return "首次订单"
                elif row == 5:
                    return "永续订单"
                else:
                    return "大订单"

            part2["purchase_type"] = part2["header.ordertype"].apply(generate_purchase_type)
            part2["store_show_code"] = part2["store.deptcode"]
            part2["item_show_code"] = part2["item.gdsincode"]
            part2["vendor_show_code"] = part2["vendor.unitcode"]

            part2["foreign_category_lv2"] = part2.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part2["foreign_category_lv3"] = part2.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row[
                    "lv.foreign_category_lv3"]
                , axis=1
            )

            part2["foreign_category_lv4"] = ""
            part2["foreign_category_lv5"] = ""

            def generate_bill_status(row):
                if row == 0:
                    return "尚未到货"
                elif row == 2:
                    return "已经到货"
                elif row == 11:
                    return "作废"

            part2["bill_status"] = part2["header.orderflag"].apply(generate_bill_status)
            part2 = part2.rename(columns={
                "header.orderno": "purchase_num",
                "header.recorddate": "purchase_date",
                "store.deptcode": "foreign_store_id",
                "store.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.amount": "purchase_qty",
                "detail.inprice": "purchase_price",
                "detail.inmoneyio": "purchase_total",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part2 = part2[columns]

        header = self.data["bil_returnfac"].rename(columns=lambda x: f"header.{x}")
        detail = self.data["bil_returnfacdtl"].rename(columns=lambda x: f"detail.{x}")
        warehouse = self.data["inf_department"].rename(columns=lambda x: f"warehouse.{x}")

        header["header.deptcode"] = header["header.deptcode"].str.strip()
        warehouse["warehouse.deptcode"] = warehouse["warehouse.deptcode"].str.strip()
        warehouse["warehouse.fatherdept"] = warehouse["warehouse.fatherdept"].str.strip()
        detail["detail.gdsincode"] = detail["detail.gdsincode"].str.strip()
        detail["detail.suppliercode"] = detail["detail.suppliercode"].str.strip()

        lv = self._sub_query_category_lv4().rename(columns=lambda x: f"lv.{x}")
        part3 = (
            header
            .merge(detail, left_on="header.billno", right_on="detail.billno")
            .merge(warehouse, left_on="header.deptcode", right_on="warehouse.deptcode")
            .merge(store, how="left", left_on="warehouse.fatherdept", right_on="store.deptcode")
            .merge(item, how="left", left_on="detail.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv4")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )
        part3 = part3[part3["warehouse.type"] == 3]
        if len(part3) == 0:
            part3 = pd.DataFrame(columns=columns)
        else:
            part3["source_id"] = self.source_id
            part3["cmid"] = self.cmid
            part3["purchase_type"] = "门店退货"
            part3["store_show_code"] = part3["store.deptcode"]
            part3["item_show_code"] = part3["item.gdsincode"]
            part3["purchase_qty"] = part3["detail.returnamount"].apply(lambda x: -1 * x)
            part3["purchase_total"] = part3["detail.actualreturnmoney"].apply(lambda x: -1 * x)
            part3["vendor_show_code"] = part3["vendor.unitcode"]
            part3["foreign_category_lv2"] = part3.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part3["foreign_category_lv3"] = part3.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row[
                    "lv.foreign_category_lv3"]
                , axis=1
            )
            part3["foreign_category_lv4"] = part3.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] +
                            row["lv.foreign_category_lv3"] + row["lv.foreign_category_lv4"], axis=1
            )
            part3["foreign_category_lv5"] = ""

            def generate_bill_status(row):
                if row == 0:
                    return "未审核"
                elif row == 1:
                    return "已审核"
                elif row == 11:
                    return "作废"

            part3["bill_status"] = part3["header.dealflag"].apply(generate_bill_status)
            part3 = part3.rename(columns={
                "header.billno": "purchase_num",
                "header.recorddate": "purchase_date",
                "store.deptcode": "foreign_store_id",
                "store.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.actualinprice": "purchase_price",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part3 = part3[columns]

        lv = self._sub_query_category_lv3().rename(columns=lambda x: f"lv.{x}")
        part4 = (
            header
            .merge(detail, left_on="header.billno", right_on="detail.billno")
            .merge(warehouse, left_on="header.deptcode", right_on="warehouse.deptcode")
            .merge(store, how="left", left_on="warehouse.fatherdept", right_on="store.deptcode")
            .merge(item, how="left", left_on="detail.gdsincode", right_on="item.gdsincode")
            .merge(lv, left_on="item.classcode", right_on="lv.foreign_category_lv3")
            .merge(brand, how="left", left_on="item.brandcode", right_on="brand.brandcode")
            .merge(vendor, how="left", left_on="detail.suppliercode", right_on="vendor.unitcode")
        )
        part4 = part4[part4["warehouse.type"] == 3]
        if len(part4) == 0:
            part4 = pd.DataFrame(columns=columns)
        else:
            part4["source_id"] = self.source_id
            part4["cmid"] = self.cmid
            part4["purchase_type"] = "门店退货"
            part4["store_show_code"] = part4["store.deptcode"]
            part4["item_show_code"] = part4["item.gdsincode"]
            part4["purchase_qty"] = part4["detail.returnamount"].apply(lambda x: -1 * x)
            part4["purchase_total"] = part4["detail.actualreturnmoney"].apply(lambda x: -1 * x)
            part4["vendor_show_code"] = part4["vendor.unitcode"]

            part4["foreign_category_lv2"] = part4.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"], axis=1)
            part4["foreign_category_lv3"] = part4.apply(
                lambda row: row["lv.foreign_category_lv1"] + row["lv.foreign_category_lv2"] + row[
                    "lv.foreign_category_lv3"]
                , axis=1
            )
            part4["foreign_category_lv4"] = ""
            part4["foreign_category_lv5"] = ""

            def generate_bill_status(row):
                if row == 0:
                    return "未审核"
                elif row == 1:
                    return "已审核"
                elif row == 11:
                    return "作废"

            part4["bill_status"] = part4["header.dealflag"].apply(generate_bill_status)
            part4 = part4.rename(columns={
                "header.billno": "purchase_num",
                "header.recorddate": "purchase_date",
                "store.deptcode": "foreign_store_id",
                "store.deptname": "store_name",
                "item.gdsincode": "foreign_item_id",
                "item.stripecode": "barcode",
                "item.gdsname": "item_name",
                "item.baseunit": "item_unit",
                "detail.actualinprice": "purchase_price",
                "vendor.unitcode": "vendor_id",
                "vendor.unitname": "vendor_name",
                "brand.brandcode": "brand_code",
                "brand.brand": "brand_name",
                "lv.foreign_category_lv1": "foreign_category_lv1"
            })
            part4 = part4[columns]
        part = pd.concat([part1, part2, part3, part4])
        part["store_show_code"] = part["store_show_code"].apply(lambda x: x.zfill(self.store_id_len))
        part["foreign_store_id"] = part["foreign_store_id"].apply(lambda x: x.zfill(self.store_id_len))
        return part
