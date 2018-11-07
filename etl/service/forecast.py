import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import boto3
import calendar
import dask.dataframe as dd
import random
from etl.extensions import cache

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

boss_hash = {
    "46a4": "43",
    "4f23": "58",
    "1b4d": "79",
}
# fmt: on

r_store_hash: dict = defaultdict(dict)
for command, info in store_hash.items():
    r_store_hash[info["cmid"]][info["store_id"]] = {"command": command, **info}


def percent_to_float(x):
    return round(float(x[:-1]) / 100, 3)


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

    @cache.memoize(timeout=300)
    def lacking_rate(self, cmid, store_id):
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(Prefix=f"agg_lacking_rate/{cmid}/"),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]
        rate = pd.read_csv(f"s3://{BUCKET}/{obj.key}", dtype={"门店ID": str})
        rate = rate[rate["门店ID"] == store_id]
        data = []
        for _, row in rate.iterrows():
            data.append(
                {
                    "date": row["date"],
                    "count": row["现门店已缺货 SKU 数"],
                    "rate": round(row["门店缺货率"], 3),
                }
            )
        return data

    @cache.memoize(timeout=300)
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
                    "achieved": round(row["total_sale"] / row["target"], 3),
                    "sales": round(row["total_sale"], 3),
                    "target": round(row["target"], 3),
                }
            )
        achieved.sort(key=lambda x: x["achieved"])
        return {"achieved": achieved, "should_achieve": round(should_achieve, 3)}

    @cache.memoize(timeout=300)
    def order_rate(self, cmid, store_id):
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(Prefix=f"order_coincidence_rate/{cmid}/"),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]
        order_coincidence_rate = pd.read_csv(
            f"s3://{BUCKET}/{obj.key}", dtype={"门店编码": str, "store_id": str}
        )
        order_coincidence_rate.set_index("store_id", inplace=True)
        stores = order_coincidence_rate.loc[store_id]
        data = []
        for _, row in stores.iterrows():
            data.append(
                {
                    "date": row["date"],
                    "suggest_order": round(row["订货sku建议成功率"], 3),
                    "order_match": round(row["订货sku及数量建议成功率"], 3),
                    "unsuggest": round(row["未建议sku订货比率"], 3),
                }
            )
        return data

    @cache.memoize(timeout=300)
    def best_lacking(self, cmid, store_id):
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(Prefix=f"best_lacking/{cmid}/"),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]
        lacking = pd.read_csv(f"s3://{BUCKET}/{obj.key}", dtype={"门店ID": str})
        lacking.set_index("门店ID", inplace=True)
        data = []
        for _, row in lacking.iterrows():
            data.append({"date": row["date"], "count": row["门店畅缺品 SKU 数（过滤非统配）"]})
        return data

    @cache.memoize(timeout=300)
    def lost_sales(self, cmid, store_id):
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=60)
        dates = pd.date_range(start.replace(day=1), end, closed="right")
        data = []
        month_data = defaultdict(lambda: defaultdict(float))
        for d in dates:
            try:
                df = pd.read_csv(
                    f"s3://replenish/lost_sales/{cmid.ljust(15, 'Y')}/{d.strftime('%Y-%m-%d')}.csv",
                    dtype={"foreign_store_id": "str"},
                )
                df.set_index("foreign_store_id", inplace=True)
            except Exception as e:
                print(e)
                continue
            _lost_sales = df.loc[store_id]["lost_sales"]
            _lost_gross = df.loc[store_id]["lost_gross"]
            month_data[d.month]["sales"] += _lost_sales
            month_data[d.month]["gross"] += _lost_gross
            data.append(
                {
                    "date": d,
                    "lost_sales": round(_lost_sales, 3),
                    "lost_gross": round(_lost_gross, 3),
                }
            )

        for info in data:
            info["month_lost_sales"] = round(month_data[info["date"].month]["sales"], 3)
            info["month_lost_gross"] = round(month_data[info["date"].month]["gross"], 3)
            info["date"] = info["date"].strftime("%Y-%m-%d")
        return data[-60:]

    @cache.memoize(timeout=300)
    def sale_amount(self, cmid, store_id):
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(Prefix=f"total_sales/{cmid}/"),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]
        total_sales = pd.read_csv(
            f"s3://{BUCKET}/{obj.key}", dtype={"foreign_store_id": str}
        )
        total_sales = total_sales[total_sales["foreign_store_id"] == store_id]
        data = []
        for _, row in total_sales.iterrows():
            data.append(
                {"date": row["date"], "total_sale": round(row["total_sale"], 3)}
            )
        return data


class BossService:
    start_at = {"79": "2018-10-07", "43": "2018-09-25", "58": "2018-09-25"}

    def login(self, command):
        cmid = boss_hash.get(command)
        if not cmid:
            raise ForecastError("command not found")
        return {"cmid": cmid}

    @cache.memoize(timeout=300)
    def lacking_rate(self, cmid):
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
            except Exception as e:
                print(f"{d}:{cmid}:{e}")
                continue
            less_than_5 = df[df["门店缺货率"] <= 0.05].shape[0]
            between_5_and_7 = df[(0.05 < df["门店缺货率"]) & (df["门店缺货率"] <= 0.07)].shape[0]
            between_7_and_10 = df[(0.07 < df["门店缺货率"]) & (df["门店缺货率"] <= 0.10)].shape[0]
            more_than_10 = df[0.10 < df["门店缺货率"]].shape[0]
            data.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "less_than_5": less_than_5,
                    "between_5_and_7": between_5_and_7,
                    "between_7_and_10": between_7_and_10,
                    "more_than_10": more_than_10,
                }
            )
        return {"data": data, "start_at": self.start_at[cmid]}

    @cache.memoize(timeout=300)
    def lost_sales(self, cmid):
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=60)
        dates = pd.date_range(start, end, closed="right")
        data = []
        for d in dates:
            try:
                df = pd.read_csv(
                    f"s3://{BUCKET}/lost_sales/{cmid.ljust(15, 'Y')}/{d.strftime('%Y-%m-%d')}.csv",
                    dtype={"foreign_store_id": str},
                )
            except Exception as e:
                print(f"{d}:{cmid}:{e}")
                continue
            lost_sales_ = df["lost_sales"].sum()
            lost_gross = df["lost_gross"].sum()
            data.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "lost_sales": lost_sales_,
                    "lost_gross": lost_gross,
                }
            )
        return {"data": data, "start_at": self.start_at[cmid]}

    @cache.memoize(timeout=300)
    def best_lacking(self, cmid):
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=60)
        dates = pd.date_range(start, end, closed="right")
        data = []
        for d in dates:
            print(d)
            try:
                df = pd.read_excel(
                    f"s3://{BUCKET}/best_selling_and_best_lacking/{cmid.ljust(15, 'Y')}/{d.strftime('%Y-%m-%d')}.xlsx",
                    sheet_name=0,
                    dtype={"门店ID": str},
                )
            except Exception as e:
                print(f"{d}:{cmid}:{e}")
                continue
            less_than_0 = df[df["门店畅缺品 SKU 数（过滤非统配）"] <= 0].shape[0]
            between_1_and_3 = df[
                (1 <= df["门店畅缺品 SKU 数（过滤非统配）"]) & (df["门店畅缺品 SKU 数（过滤非统配）"] <= 3)
            ].shape[0]
            between_4_and_7 = df[
                (4 <= df["门店畅缺品 SKU 数（过滤非统配）"]) & (df["门店畅缺品 SKU 数（过滤非统配）"] <= 7)
            ].shape[0]
            more_than_7 = df[7 < df["门店畅缺品 SKU 数（过滤非统配）"]].shape[0]
            data.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "less_than_0": less_than_0,
                    "between_1_and_3": between_1_and_3,
                    "between_4_and_7": between_4_and_7,
                    "more_than_7": more_than_7,
                }
            )
        return {"data": data, "start_at": self.start_at[cmid]}

    @cache.memoize(timeout=300)
    def lost_sales_of_best_lacking(self, cmid):
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=60)
        dates = pd.date_range(start, end, closed="right")
        data = []
        for d in dates:
            try:
                df = pd.read_csv(
                    f"s3://{BUCKET}/lost_sales_of_best_lacking/{cmid.ljust(15, 'Y')}/{d.strftime('%Y-%m-%d')}.csv",
                    dtype={"foreign_store_id": str},
                )
            except Exception as e:
                print(f"{d}:{cmid}:{e}")
                continue
            lost_sales_ = df["lost_sales"].sum()
            lost_gross = df["lost_gross"].sum()
            data.append(
                {
                    "date": d.strftime("%Y-%m-%d"),
                    "lost_sales": lost_sales_,
                    "lost_gross": lost_gross,
                }
            )
        return {"data": data, "start_at": self.start_at[cmid]}

    @cache.memoize(timeout=300)
    def stores(self, cmid):
        store_infos = r_store_hash[cmid]
        data = []
        end = datetime.now() - timedelta(days=1)
        start = end - timedelta(days=30)
        dates = pd.date_range(start, end, closed="right")
        order_matchs = defaultdict(lambda: defaultdict(float))
        for d in dates:
            try:
                df = pd.read_excel(
                    f"s3://{BUCKET}/everyday_delivery_{cmid}/{d.strftime('%Y-%m-%d')}.xlsx",
                    sheet_name="补货监控",
                    dtype={"门店编码": str},
                    usecols=[0, 10, 11, 12],
                )
                df.set_index("门店编码", inplace=True)
            except Exception as e:
                print(f"{d}:{cmid}:{e}")
                continue
            for code in df.index:
                om = df.loc[code][0]
                if not isinstance(om, str):
                    continue
                order_matchs[code]["amount"] += percent_to_float(om)
                order_matchs[code]["count"] += 1
        lack_rates = {}
        objs = sorted(
            s3.Bucket(BUCKET).objects.filter(
                Prefix=f"lacking_rate/{cmid.ljust(15, 'Y')}/"
            ),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[:2]
        for obj in objs:
            df = pd.read_excel(
                f"s3://{BUCKET}/{obj.key}", sheet_name=0, dtype={"门店ID": str}
            )
            df.set_index("门店ID", inplace=True)
            for store_id in df.index:
                if store_id not in lack_rates:
                    lack_rates[store_id] = float(df.loc[store_id]["门店缺货率"])
        best_lacking = {}
        objs = sorted(
            s3.Bucket(BUCKET).objects.filter(
                Prefix=f"best_selling_and_best_lacking/{cmid.ljust(15, 'Y')}/"
            ),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[:2]
        for obj in objs:
            df = pd.read_excel(
                f"s3://{BUCKET}/{obj.key}", sheet_name=0, dtype={"门店ID": str}
            )
            df.set_index("门店ID", inplace=True)
            for store_id in df.index:
                if store_id not in best_lacking:
                    best_lacking[store_id] = int(df.loc[store_id]["门店畅缺品 SKU 数（过滤非统配）"])
        for store_id, info in store_infos.items():
            match = order_matchs[info["show_code"]]
            data.append(
                {
                    "store_name": info["store_name"],
                    "lack_rate": lack_rates[store_id],
                    "best_lacking": best_lacking[store_id],
                    "match_rate": match["amount"] / match["count"],
                }
            )
        return data

    @cache.memoize(timeout=300)
    def goods(self, cmid, query):
        end = datetime.now() - timedelta(days=2)
        all_goods = pd.read_csv(
            f"s3://standard-data/{end.strftime('%Y/%m/%d')}/{cmid.ljust(15, 'Y')}/chain_goods/chain_goods000.gz",
            compression="gzip",
            names=[
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
            ],
            dtype={"foreign_item_id": str, "show_code": str},
            usecols=["foreign_item_id", "show_code", "item_name"],
        )
        all_goods.set_index("foreign_item_id", inplace=True)
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(
                Prefix=f"suggest_times_and_same_times/{cmid}/"
            ),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]
        suggest_times_and_same_times = pd.read_csv(
            f"s3://{BUCKET}/{obj.key}",
            dtype={"foreign_item_id": str, "foreign_store_id": str},
        )
        suggest_times_and_same_times = (
            suggest_times_and_same_times.groupby("foreign_item_id")
            .agg({"same_times": "sum", "suggest_times": "sum"})
            .reset_index()
        )
        suggest_times_and_same_times.sort_values(
            "same_times", ascending=False, inplace=True
        )
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(
                Prefix=f"velocity_of_circulation/{cmid.ljust(15, 'Y')}/intermediate/item_view/"
            ),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]
        voc = pd.read_csv(f"s3://{BUCKET}/{obj.key}", dtype={"商品ID": str})
        voc.set_index("商品ID", inplace=True)
        before_suggest = pd.read_csv(
            f"s3://replenish/velocity_of_circulation/{cmid.ljust(15, 'Y')}/intermediate/item_view/2018-09-25.csv",
            dtype={"商品ID": str},
        )
        before_suggest.set_index("商品ID", inplace=True)
        data = []
        for _, row in suggest_times_and_same_times.iterrows():
            item_id = row["foreign_item_id"]
            if any(item_id not in df.index for df in (voc, before_suggest, all_goods)):
                continue
            show_code = all_goods.loc[item_id]["show_code"]
            item_name = all_goods.loc[item_id]["item_name"]
            if query and query not in show_code:
                continue
            if query and query not in item_name:
                continue
            avg_turnover = float(voc.loc[item_id]["商品周转周期"])
            before_turnover = float(before_suggest.loc[item_id]["商品周转周期"])
            if avg_turnover < 0 or before_turnover < 0:
                continue
            turnover_contrast = (
                (before_turnover - avg_turnover) / before_turnover
                if before_turnover
                else 0
            )
            same_times = row["same_times"]
            suggest_times = row["suggest_times"]
            data.append(
                {
                    "show_code": show_code,
                    "foreign_item_id": item_id,
                    "item_name": item_name,
                    "avg_turnover": round(avg_turnover, 3),
                    "turnover_contrast": round(turnover_contrast, 3),
                    "same_times": int(same_times),
                    "suggest_times": int(suggest_times),
                }
            )
        top_50 = data[:50]
        middle_50 = data[len(data) // 2 - 25 : len(data) // 2 + 25]
        empty = [d for d in data if d["same_times"] == 0]
        empty_50 = (
            random.choices([d for d in data if d["same_times"] == 0], k=50)
            if len(empty) >= 50
            else empty
        )
        all_150 = [*top_50, *middle_50, *empty_50]
        return all_150

    @cache.memoize(timeout=300)
    def goods_detail(self, cmid, item_id):
        end = datetime.now() - timedelta(days=2)
        start = end - timedelta(days=30)
        dates = pd.date_range(start, end, closed="right")
        urls = []
        for d in dates:
            urls.append(
                f"s3://replenish/velocity_of_circulation/{cmid.ljust(15, 'Y')}/intermediate/item_view/{d.strftime('%Y-%m-%d')}.csv"
            )
        _item_voc = [
            dd.read_csv(url, dtype={"商品ID": str}, blocksize=None) for url in urls
        ]
        _item_voc = dd.concat(_item_voc)
        _item_voc = _item_voc[_item_voc["商品ID"] == item_id]
        item_voc = _item_voc.compute()

        turnover = []
        inventory_sales = []
        for _, row in item_voc.iterrows():
            turnover.append({"days": round(row["商品周转周期"], 3), "date": row["时间"]})
            inventory_sales.append(
                {
                    "inventory": round(row["商品平均库存量"], 3),
                    "sales": round(row["日均销量"], 3),
                    "date": row["时间"],
                }
            )
        obj = sorted(
            s3.Bucket(BUCKET).objects.filter(Prefix=f"goods_turnover_detail/{cmid}/"),
            key=lambda obj: int(obj.last_modified.strftime("%s")),
            reverse=True,
        )[0]
        turnover_detail = pd.read_csv(
            f"s3://{BUCKET}/{obj.key}", dtype={"item_id": str, "store_id": str}
        )
        goods_detail = turnover_detail[turnover_detail["item_id"] == item_id]
        return {
            "start_at": self.start_at[cmid],
            "turnover": turnover,
            "inventory_sales": inventory_sales,
            "stores": goods_detail.to_dict("records"),
        }

    @cache.memoize(timeout=300)
    def store_goods_detail(self, cmid, item_id, store_id):
        end = datetime.now() - timedelta(days=2)
        start = end - timedelta(days=30)
        dates = pd.date_range(start, end, closed="right")
        urls = []
        for d in dates:
            urls.append(
                f"s3://replenish/velocity_of_circulation/{cmid.ljust(15, 'Y')}/intermediate/store_item_view/{d.strftime('%Y-%m-%d')}.csv"
            )
        _voc = [
            dd.read_csv(url, dtype={"商品ID": str, "门店ID": str}, blocksize=None)
            for url in urls
        ]
        _voc = dd.concat(_voc)
        _voc = _voc[(_voc["商品ID"] == item_id) & (_voc["门店ID"] == store_id)]
        voc = _voc.compute()
        turnover = []
        inventory_sales = []
        for _, row in voc.iterrows():
            turnover.append({"days": round(row["商品周转周期"], 3), "date": row["时间"]})
            inventory_sales.append(
                {
                    "inventory": round(row["商品平均库存量"], 3),
                    "sales": round(row["日均销量"], 3),
                    "date": row["时间"],
                }
            )

        return {
            "start_at": self.start_at[cmid],
            "turnover": turnover,
            "inventory_sales": inventory_sales,
        }
