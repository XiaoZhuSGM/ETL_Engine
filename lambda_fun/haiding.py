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
# purchase_warehouse:
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
# requireorder:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "requireorder",
    "origin_table_columns": {
        "HD40.otrequireorder": [
            "billnumber",
            "billtype",
            "buyercode",
            "finishtime",
            "state",
            "uuid",
        ],
        "HD40.otrequireorderline": [
            "munit",
            "checkedqty",
            "price",
            "checkedtotal",
            "bill",
            "product",
        ],
        "HD40.store": ["gid", "code", "name"],
        "HD40.goods": ["code", "code2", "gid", "name", "psr", "sort", "vdrgid"],
        "HD40.sort": ["code"],
        "HD40.vendor": ["gid", "code", "name"],
        "HD40.employee": ["name", "gid"],
    },
    "converts": {
        "HD40.otrequireorder": {
            "billnumber": "str",
            "billtype": "str",
            "buyercode": "str",
            "finishtime": "str",
            "uuid": "str",
        },
        "HD40.otrequireorderline": {"munit": "str", "bill": "str", "product": "str"},
        "HD40.store": {"gid": "str", "code": "str", "name": "str"},
        "HD40.goods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "name": "str",
            "psr": "str",
            "sort": "str",
            "vdrgid": "str",
        },
        "HD40.sort": {"code": "str"},
        "HD40.vendor": {"gid": "str", "code": "str", "name": "str"},
        "HD40.employee": {"name": "str", "gid": "str"},
    },
}
# delivery:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "delivery",
    "origin_table_columns": {
        "HD40.stkout": ["billto", "cls", "num", "ocrdate", "stat"],
        "HD40.stkoutdtl": ["alcsrc", "cls", "gdgid", "munit", "num", "qty", "wrh"],
        "HD40.store": ["gid", "name", "code"],
        "HD40.warehouse": ["gid", "name", "code"],
        "HD40.goods": ["code", "code2", "gid", "name", "rtlprc", "sort"],
        "HD40.sort": ["code"],
        "HD40.stkoutbck": ["billto", "cls", "num", "ocrdate", "stat"],
        "HD40.stkoutbckdtl": ["bckcls", "cls", "gdgid", "munit", "num", "qty", "wrh"],
    },
    "converts": {
        "HD40.stkout": {
            "billto": "str",
            "cls": "str",
            "num": "str",
            "ocrdate": "str",
            "stat": "str",
        },
        "HD40.stkoutdtl": {
            "cls": "str",
            "gdgid": "str",
            "munit": "str",
            "num": "str",
            "wrh": "str",
        },
        "HD40.store": {"gid": "str", "name": "str", "code": "str"},
        "HD40.warehouse": {"gid": "str", "name": "str", "code": "str"},
        "HD40.goods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "name": "str",
            "sort": "str",
        },
        "HD40.sort": {"code": "str"},
        "HD40.stkoutbck": {
            "billto": "str",
            "cls": "str",
            "num": "str",
            "ocrdate": "str",
            "stat": "str",
        },
        "HD40.stkoutbckdtl": {
            "bckcls": "str",
            "cls": "str",
            "gdgid": "str",
            "munit": "str",
            "num": "str",
            "wrh": "str",
        },
    },
}
# purchase_store:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "purchase_store",
    "origin_table_columns": {
        "HD40.diralc": ["cls", "fildate", "num", "receiver", "stat", "vendor"],
        "HD40.diralcdtl": ["cls", "gdgid", "num", "price", "qpc", "qty", "total"],
        "HD40.vendor": ["gid", "code", "name"],
        "HD40.store": ["gid", "code", "name"],
        "HD40.modulestat": ["statname", "no"],
        "HD40.goods": ["brand", "code", "code2", "gid", "munit", "name", "sort"],
        "HD40.brand": ["code", "name"],
    },
    "converts": {
        "HD40.diralc": {
            "cls": "str",
            "num": "str",
            "fildate": "str",
            "receiver": "str",
            "vendor": "str",
        },
        "HD40.diralcdtl": {"cls": "str", "num": "str", "gdgid": "str"},
        "HD40.vendor": {"gid": "str", "code": "str", "name": "str"},
        "HD40.store": {"gid": "str", "code": "str", "name": "str"},
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
    },
}
# sales_promotion:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "sales_promotion",
    "origin_table_columns": {
        "HD40.v_prom_gd": [
            "audittime",
            "begintime",
            "cls",
            "createtime",
            "endtime",
            "gdcode",
            "gdgid",
            "gdname",
            "munit",
            "num",
            "promprc",
            "rtlprc",
            "storecode",
            "storegid",
            "storename",
        ],
        "HD40.goods": ["sort", "gid"],
    },
    "converts": {
        "HD40.v_prom_gd": {
            "audittime": "str",
            "begintime": "str",
            "cls": "str",
            "createtime": "str",
            "endtime": "str",
            "gdcode": "str",
            "gdgid": "str",
            "gdname": "str",
            "munit": "str",
            "num": "str",
            "storecode": "str",
            "storegid": "str",
            "storename": "str",
        },
        "HD40.goods": {"sort": "str", "gid": "str"},
    },
}
# move_store:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "move_store",
    "origin_table_columns": {
        "HD40.invxf": ["cls", "fildate", "fromstore", "num", "stat", "tostore"],
        "HD40.invxfdtl": ["qty", "price", "total", "num", "cls", "gdgid"],
        "HD40.store": ["gid", "code", "name"],
        "HD40.goods": ["code", "code2", "gid", "munit", "name", "sort"],
        "HD40.modulestat": ["statname", "no"],
    },
    "converts": {
        "HD40.invxf": {
            "cls": "str",
            "fildate": "str",
            "fromstore": "str",
            "num": "str",
            "tostore": "str",
        },
        "HD40.invxfdtl": {"num": "str", "cls": "str", "gdgid": "str"},
        "HD40.store": {"gid": "str", "code": "str", "name": "str"},
        "HD40.goods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "HD40.modulestat": {"statname": "str"},
    },
}
# move_warehouse:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "move_warehouse",
    "origin_table_columns": {
        "HD40.invxf": ["cls", "fildate", "fromwrh", "num", "stat", "towrh"],
        "HD40.invxfdtl": ["qty", "price", "total", "num", "cls", "gdgid"],
        "HD40.warehouse": ["gid", "code", "name"],
        "HD40.goods": ["code", "code2", "gid", "munit", "name", "sort"],
        "HD40.modulestat": ["statname", "no"],
    },
    "converts": {
        "HD40.invxf": {
            "cls": "str",
            "fildate": "str",
            "fromwrh": "str",
            "num": "str",
            "towrh": "str",
        },
        "HD40.invxfdtl": {"num": "str", "cls": "str", "gdgid": "str"},
        "HD40.warehouse": {"gid": "str", "code": "str", "name": "str"},
        "HD40.goods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "HD40.modulestat": {"statname": "str"},
    },
}
# store:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "store",
    "origin_table_columns": {
        "HD40.store": [
            "address",
            "area",
            "circle",
            "code",
            "contactor",
            "gid",
            "name",
            "phone",
            "property",
            "stat",
        ],
        "HD40.area": ["code", "name"],
    },
    "converts": {
        "HD40.store": {
            "gid": "str",
            "name": "str",
            "address": "str",
            "code": "str",
            "area": "str",
            "circle": "str",
            "contactor": "str",
            "phone": "str",
        },
        "HD40.area": {"code": "str", "name": "str"},
    },
}
# goods:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "goods",
    "origin_table_columns": {
        "HD40.goods": [
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
        ],
        "HD40.goodsbusgate": ["gid", "name"],
        "HD40.brand": ["name", "code"],
        "HD40.vendor": ["name", "code", "gid"],
    },
    "converts": {
        "HD40.goods": {
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
        },
        "HD40.goodsbusgate": {"gid": "str", "name": "str"},
        "HD40.brand": {"name": "str", "code": "str"},
        "HD40.vendor": {"gid": "str", "code": "str", "name": "str"},
    },
}
# category:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "category",
    "origin_table_columns": {"HD40.sort": ["code", "name"]},
    "converts": {"HD40.sort": {"code": "str", "name": "str"}},
}
# goods_loss:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "goods_loss",
    "origin_table_columns": {
        "HD40.ckdatas": [
            "rtlbal",
            "acntqty",
            "cktime",
            "gdgid",
            "num",
            "qty",
            "stat",
            "store",
        ],
        "HD40.store": ["gid", "code", "name"],
        "HD40.goods": ["code", "code2", "gid", "munit", "name", "sort"],
        "HD40.sort": ["code"],
    },
    "converts": {
        "HD40.ckdatas": {"cktime": "str", "gdgid": "str", "num": "str", "store": "str"},
        "HD40.store": {"gid": "str", "code": "str", "name": "str"},
        "HD40.goods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "HD40.sort": {"code": "str"},
    },
}
# check_warehouse:
event = {
    "source_id": "43YYYYYYYYYYYYY",
    "erp_name": "海鼎",
    "date": "2018-08-10",
    "target_table": "check_warehouse",
    "origin_table_columns": {
        "HD40.ckdatas": [
            "rtlbal",
            "acntqty",
            "cktime",
            "gdgid",
            "num",
            "qty",
            "stat",
            "wrh",
        ],
        "HD40.warehouse": ["gid", "code", "name"],
        "HD40.goods": ["code", "code2", "gid", "munit", "name", "sort"],
        "HD40.sort": ["code"],
    },
    "converts": {
        "HD40.ckdatas": {"cktime": "str", "gdgid": "str", "num": "str", "wrh": "str"},
        "HD40.warehouse": {"gid": "str", "code": "str", "name": "str"},
        "HD40.goods": {
            "code": "str",
            "code2": "str",
            "gid": "str",
            "munit": "str",
            "name": "str",
            "sort": "str",
        },
        "HD40.sort": {"code": "str"},
    },
}

"""


import pandas as pd
import boto3
from datetime import datetime
import tempfile
import time
import pytz
from typing import Dict


S3_BUCKET = "ext-etl-data"
S3 = boto3.client("s3")
_TZINFO = pytz.timezone("Asia/Shanghai")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


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
            df.to_csv(f"{target_table}-8-10-1.csv", index=False)
            # self.up_load_to_s3(df, target_table)
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
        otrequireorder = self.data["HD40.otrequireorder"]
        otrequireorderline = self.data["HD40.otrequireorderline"]
        store = self.data["HD40.store"]
        goods = self.data["HD40.goods"]
        sort = self.data["HD40.sort"]
        vendor = self.data["HD40.vendor"]
        employee = self.data["HD40.employee"]
        otrequireorder = otrequireorder.drop_duplicates()
        otrequireorderline = otrequireorderline.drop_duplicates()
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
        stkout = self.data["HD40.stkout"]
        stkoutdtl = self.data["HD40.stkoutdtl"]
        store = self.data["HD40.store"]
        warehouse = self.data["HD40.warehouse"]
        goods = self.data["HD40.goods"]
        sort = self.data["HD40.sort"]
        stkoutbck = self.data["HD40.stkoutbck"]
        stkoutbckdtl = self.data["HD40.stkoutbckdtl"]
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

    def purchase_store(self):
        diralc = self.data["HD40.diralc"]
        diralcdtl = self.data["HD40.diralcdtl"]
        vendor = self.data["HD40.vendor"]
        store = self.data["HD40.store"]
        modulestat = self.data["HD40.modulestat"]
        goods = self.data["HD40.goods"]
        brand = self.data["HD40.brand"]

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

    def sales_promotion(self):
        v_prom_gd = self.data["HD40.v_prom_gd"]
        goods = self.data["HD40.goods"]

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
        part = v_prom_gd.merge(
            goods,
            how="left",
            left_on=["gdgid"],
            right_on=["gid"],
            suffixes=("", ".goods"),
        )
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
        invxf = self.data["HD40.invxf"]
        invxfdtl = self.data["HD40.invxfdtl"]
        store = self.data["HD40.store"]
        goods = self.data["HD40.goods"]
        modulestat = self.data["HD40.modulestat"]

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

    def move_warehouse(self):
        invxf = self.data["HD40.invxf"]
        invxfdtl = self.data["HD40.invxfdtl"]
        warehouse = self.data["HD40.warehouse"]
        goods = self.data["HD40.goods"]
        modulestat = self.data["HD40.modulestat"]

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

    def store(self):
        store = self.data["HD40.store"]
        area = self.data["HD40.area"]

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
        part["create_date"] = datetime.now()
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
        goods = self.data["HD40.goods"]
        goodsbusgate = self.data["HD40.goodsbusgate"]
        brand = self.data["HD40.brand"]
        vendor = self.data["HD40.vendor"]

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
        part["storage_time"] = datetime.now()
        part["last_updated"] = datetime.now()
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
            }
        )
        part = part[columns]
        return part

    def category(self):
        sort = self.data["HD40.sort"]
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

    def goods_loss(self):
        ckdatas = self.data["HD40.ckdatas"]
        store = self.data["HD40.store"]
        goods = self.data["HD40.goods"]
        sort = self.data["HD40.sort"]
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

    def check_warehouse(self):
        ckdatas = self.data["HD40.ckdatas"]
        warehouse = self.data["HD40.warehouse"]
        goods = self.data["HD40.goods"]
        sort = self.data["HD40.sort"]
        goods["sort1"] = goods.apply(lambda row: row["sort"][:2], axis=1)
        goods["sort2"] = goods.apply(lambda row: row["sort"][:4], axis=1)
        goods["sort3"] = goods.apply(lambda row: row["sort"][:6], axis=1)

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
