import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import boto3
import calendar
import dask.dataframe as dd

s3 = boto3.resource("s3")
BUCKET = "replenish"

# fmt: off
store_hash = {
    "6ec1": {"cmid": "43", "store_id": "1000381", "show_code": "00000197", "store_name": "邢台世纪城三店"},
    "3b47": {"cmid": "43", "store_id": "1000642", "show_code": "00001207", "store_name": "邢台红星西店"},
    "0d78": {"cmid": "43", "store_id": "1000110", "show_code": "00000082", "store_name": "邢台阳光印象店"},
    "a46a": {"cmid": "43", "store_id": "1000040", "show_code": "00001154", "store_name": "邢台华北店"},
    "3823": {"cmid": "43", "store_id": "1001322", "show_code": "00001222", "store_name": "邢台水岸蓝庭店"},
    "4961": {"cmid": "43", "store_id": "1000095", "show_code": "00000049", "store_name": "邢台万城店"},
    "dc96": {"cmid": "43", "store_id": "1000781", "show_code": "00001210", "store_name": "邢台学院店"},
    "2d90": {"cmid": "43", "store_id": "1000113", "show_code": "00000089", "store_name": "邢台维也纳店"},
    "e44c": {"cmid": "43", "store_id": "1000139", "show_code": "00000155", "store_name": "邢台泉南店"},
    "e586": {"cmid": "43", "store_id": "1000179", "show_code": "00001095", "store_name": "邢台李家庄店"},
    "9761": {"cmid": "43", "store_id": "1000541", "show_code": "00001205", "store_name": "邢台新纪元店"},
    "fef7": {"cmid": "79", "store_id": "1000037", "show_code": "1016", "store_name": "壶山店"},
    "b1f8": {"cmid": "79", "store_id": "1000044", "show_code": "1023", "store_name": "福兴店"},
    "85d8": {"cmid": "79", "store_id": "1000051", "show_code": "1030", "store_name": "荔南店"},
    "11f2": {"cmid": "79", "store_id": "1000076", "show_code": "1055", "store_name": "工业店"},
    "33d2": {"cmid": "79", "store_id": "1000090", "show_code": "1069", "store_name": "鲤东店"},
    "fc21": {"cmid": "79", "store_id": "1000121", "show_code": "1100", "store_name": "南门店"},
    "33a2": {"cmid": "79", "store_id": "1000129", "show_code": "1108", "store_name": "郊中店"},
    "b724": {"cmid": "79", "store_id": "1000681", "show_code": "1167", "store_name": "天成店"},
    "5831": {"cmid": "79", "store_id": "1001102", "show_code": "1206", "store_name": "莲盛店"},
    "420c": {"cmid": "79", "store_id": "1001182", "show_code": "1218", "store_name": "大磨店"},
    "93a9": {"cmid": "58", "store_id": "1006804", "show_code": "0809", "store_name": "万达南店"},
    "3f4c": {"cmid": "58", "store_id": "1002500", "show_code": "0496", "store_name": "水上人间"},
    "b86a": {"cmid": "58", "store_id": "1002720", "show_code": "0510", "store_name": "滏东南店"},
    "d485": {"cmid": "58", "store_id": "1003260", "show_code": "0532", "store_name": "高铁店"},
    "b0a8": {"cmid": "58", "store_id": "1000274", "show_code": "0266", "store_name": "邯山政府店"},
    "bb9b": {"cmid": "58", "store_id": "1000086", "show_code": "0022", "store_name": "人和店"},
    "42db": {"cmid": "58", "store_id": "1006482", "show_code": "0781", "store_name": "启信店"},
    "5b34": {"cmid": "58", "store_id": "1006160", "show_code": "0757", "store_name": "新一中三楼店"},
    "2033": {"cmid": "58", "store_id": "1007081", "show_code": "0835", "store_name": "美乐成五楼店"},
    "d8c6": {"cmid": "58", "store_id": "1005400", "show_code": "0686", "store_name": "锦绣江南店"},
}

enterprise_hash = {
    "f604": "43",
    "8cae": "58",
    "7e88": "79",
}
# fmt: on

r_store_hash: dict = defaultdict(dict)
for command, info in store_hash.items():
    r_store_hash[info["cmid"]][info["store_id"]] = {"command": command, **info}


class ForecastError(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return str(self.info)


class ForecastService:
    def login(self, command):
        store_info = store_hash.get(command)
        if not store_info:
            raise ForecastError("command not found")
        return store_info

    def lacking_rate(self, cmid, store_id):
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=60)
        dates = pd.date_range(start, end, closed="right")
        data = []
        for d in dates:
            try:
                df = pd.read_excel(
                    f"s3://{BUCKET}/lacking_rate/{cmid.ljust(15, 'Y')}/{d.strftime('%Y-%m-%d')}.xlsx",
                    sheet_name=0,
                    dtype={"门店ID": str},
                )
                df.set_index("门店ID", inplace=True)
                if store_id not in df.index:
                    continue
            except Exception as e:
                print(f"{d}:{store_id}:{e}")
                continue
            data.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "count": int(df.loc[store_id]["现门店已缺货 SKU 数"]),
                    "rate": float(df.loc[store_id]["门店缺货率"]),
                }
            )
        return data

    def performance_process(self, cmid):
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(Prefix=f"graph/sales_process/{cmid}/"),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]

        date = datetime.strptime(
            obj.key.rsplit("/", 1)[-1].split(".", 1)[0], "%Y-%m-%d"
        )
        month_days = calendar.monthrange(date.year, date.month)[1]
        should_achieve = date.day / month_days
        data = pd.read_csv(
            f"s3://replenish/{obj.key}", dtype={"foreign_store_id": "str"}
        )
        achieved = []
        for _, row in data.iterrows():
            store_name = r_store_hash[cmid][row["foreign_store_id"]]["store_name"]
            achieved.append(
                {
                    "store": store_name,
                    "achieved": row["total_sale"] / row["target"],
                    "sales": row["total_sale"],
                    "target": row["target"],
                }
            )
        achieved.sort(key=lambda x: x["achieved"])
        return {"achieved": achieved, "should_achieve": should_achieve}

    def order_rate(self, cmid, store_id):
        show_code = r_store_hash[cmid][store_id]["show_code"]
        end = datetime.now() - timedelta(days=2)
        start = end - timedelta(days=60)
        dates = pd.date_range(start, end, closed="right")

        def percent_to_float(x):
            return float(x[:-1]) / 100

        data = []
        for d in dates:
            try:
                df = pd.read_excel(
                    f"s3://{BUCKET}/everyday_delivery_{cmid}/{d.strftime('%Y-%m-%d')}.xlsx",
                    sheet_name="补货监控",
                    dtype={"门店编码": str},
                    usecols=[0, 10, 11, 12],
                )
                df.set_index("门店编码", inplace=True)
                if show_code not in df.index:
                    print(f"{d}:{show_code}:no show_code")
                    continue
            except Exception as e:
                print(f"{d}:{show_code}:{e}")
                continue
            try:
                so = df.loc[show_code][0]
                om = df.loc[show_code][1]
                us = df.loc[show_code][2]
            except Exception as e:
                print(f"{d}:{show_code}:{e}")
                continue
            if any([pd.isna(so), pd.isna(om), pd.isna(us)]):
                continue
            data.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "suggest_order": percent_to_float(so),
                    "order_match": percent_to_float(om),
                    "unsuggest": percent_to_float(us),
                }
            )

        return data

    def best_lacking(self, cmid, store_id):
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=60)
        dates = pd.date_range(start, end, closed="right")
        data = []
        for d in dates:
            try:
                df = pd.read_excel(
                    f"s3://{BUCKET}/best_selling_and_best_lacking/{cmid.ljust(15, 'Y')}/{d.strftime('%Y-%m-%d')}.xlsx",
                    sheet_name=0,
                    dtype={"门店ID": str},
                )
                df.set_index("门店ID", inplace=True)
                if store_id not in df.index:
                    continue
            except Exception as e:
                print(f"{d}:{store_id}:{e}")
                continue
            data.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "count": int(df.loc[store_id]["门店畅缺品 SKU 数（过滤非统配）"]),
                }
            )
        return data

    def sale_amount(self, cmid, store_id):
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=60)
        dates = pd.date_range(start, end, closed="right")
        data = []
        for d in dates:
            try:
                year, month, day = str(d.date()).split('-')
                df = dd.read_csv(f"s3://standard-data/{year}/{month}/{day}/{cmid.ljust(15, 'Y')}/cost/*.gz",
                                 compression="gzip",
                                 blocksize=None,
                                 names=[
                                     'source_id',
                                     'foreign_store_id',
                                     'foreign_item_id',
                                     'date',
                                     'cost_type',
                                     'total_quantity',
                                     'total_sale',
                                     'total_cost',
                                     'foreign_category_lv1',
                                     'foreign_category_lv2',
                                     'foreign_category_lv3',
                                     'foreign_category_lv4',
                                     'foreign_category_lv5',
                                     'cmid'
                                 ],
                                 dtype={
                                     'source_id': str,
                                     'foreign_store_id': str,
                                     'foreign_item_id': str,
                                     'date': str,
                                     'cost_type': str,
                                     'total_quantity': float,
                                     'total_sale': float,
                                     'total_cost': float,
                                     'foreign_category_lv1': str,
                                     'foreign_category_lv2': str,
                                     'foreign_category_lv3': str,
                                     'foreign_category_lv4': str,
                                     'foreign_category_lv5': str,
                                     'cmid': str
                                 },
                                 usecols=[
                                     'foreign_store_id',
                                     'foreign_item_id',
                                     'date',
                                     'total_quantity',
                                     'total_sale',
                                     'total_cost',
                                     'foreign_category_lv1'
                                 ])
                cost_data = df.compute()
                cost_data = cost_data[cost_data["foreign_store_id"] == store_id]
                data.append({"date": d.strftime("%Y-%m-%d"), "total_sale": cost_data["total_sale"].sum()})
            except Exception as e:
                print(e)
            finally:
                pass

        return data
