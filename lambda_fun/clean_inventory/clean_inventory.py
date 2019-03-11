# -*- coding: utf-8 -*-
"""
库存清洗逻辑的入口
"""
import json

import boto3
import pandas as pd
import pytz
import re
from datetime import datetime, timedelta
import tempfile
import time
from typing import Dict

S3_BUCKET = "ext-etl-data"

INV_HISTORY_HUMP_JSON = (
    "inventory/source_id={source_id}/ext_date={date}/history_dump_json/{hour}/"
)
CONVERTERS = {"str": str, "int": int, "float": float}

BLANK_CHAR = re.compile(r"[\r\n\t]+")

_TZINFO = pytz.timezone("Asia/Shanghai")
S3 = boto3.resource("s3")
INV_CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/hour={hour}/dump={timestamp}&rowcount={rowcount}.csv.gz"


class InventoryCleaner:

    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame], hour: str, target_table) -> None:
        self.source_id = source_id
        self.date = date
        self.cmid = self.source_id.split("Y", 1)[0]
        self.data = data
        self.hour = hour
        self.target_table = target_table

    def clean_sixun_inventory(self):
        """
        清洗思讯库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["t_im_branch_stock"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["branch_no"] = frames["branch_no"].apply(lambda x: x[:4])
            frames["foreign_store_id"] = frames["branch_no"]
            frames["foreign_item_id"] = frames["item_no"].str.strip()
            frames["quantity"] = frames["stock_qty"]
            # frames["amount"] = frames["stock_qty"] * frames["avg_cost"]
            frames["amount"] = frames.apply(
                lambda row: row["stock_qty"] * row["avg_cost"], axis=1
            )
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_bianzhaijia_inventory(self):
        """
        清洗便宅家库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["zetl_inventory"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames["storeuuid"]
            frames["foreign_item_id"] = frames["productuuid"]
            frames["quantity"] = frames["qty"]
            frames["amount"] = frames["costamount"]
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_shanghai_inventory(self):
        """
        清洗商海连锁版库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
                """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["goodsorg"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames["orgcode"]
            frames["foreign_item_id"] = frames["plucode"]
            frames["quantity"] = frames["gcount"]
            frames["amount"] = frames["cost"]
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_tianshen_inventory(self):
        """
                清洗田森库存表
                :param source_id:
                :param date:
                :param target_table:
                :param data_frames:
                :return:
                        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["tstklskc"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames["orgcode"].str.strip()
            frames["foreign_item_id"] = frames["pluid"].str.strip()
            frames["quantity"] = frames["kccount"]
            frames["amount"] = frames["hcost"]
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0) & (frames['foreign_store_id'] != 00)
                ]
        return self.up_load_to_s3(frames)

    def clean_angjie_inventory(self):
        """
    清洗昂捷中间库库存表
    :param source_id:
    :param date:
    :param target_table:
    :param data_frames:
    :return:
                """
        columns_inv = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        columns_store = ["c_id", "c_web_page"]
        cmid = self.cmid
        inventroy_part = self.data["tbstocks"]
        base_table_date = (datetime.strptime(self.date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        prefix = f"datapipeline/source_id={self.source_id}/ext_date={base_table_date}/table=tb_store/"
        key = get_matching_s3_keys(S3_BUCKET, prefix=prefix, suffix=".csv.gz")
        key = f"S3://{S3_BUCKET}/{key}"
        store_part = pd.read_csv(key, encoding="utf-8", compression="gzip", usecols=columns_store)
        print(store_part)
        if not len(inventroy_part):
            frames = pd.DataFrame(columns=columns_inv)
        else:
            frames = inventroy_part.merge(store_part, how='left',
                                          left_on='c_web_page', right_on='c_web_page')
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames["c_id"]
            frames["foreign_item_id"] = frames["goodscode"]
            frames["quantity"] = frames["amount"]
            frames["amount"] = frames["salemoney"]
            frames = frames[columns_inv]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_zhibaiwei_inventory(self):
        """
            清洗智百威库存表
            :param source_id:
            :param date:
            :param target_table:
            :param data_frames:
            :return:
                        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["ic_t_branch_stock"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames["branch_no"].str.strip().apply(lambda x: x[:2])
            frames["foreign_item_id"] = frames["item_no"].str.strip()
            frames["quantity"] = frames["stock_qty"]
            frames["amount"] = frames["cost_amt"]
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_fujirongtong_inventory(self):
        """
                    清洗富基融通库存表
                    :param source_id:
                    :param date:
                    :param target_table:
                    :param data_frames:
                    :return:
                                """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["shopsstock"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames["shopid"].str.strip()
            frames["foreign_item_id"] = frames["goodsid"].str.strip()
            frames["quantity"] = frames["qty"]
            frames["amount"] = frames.apply(
                lambda row: row["costvalue"] + row["costtaxvalue"], axis=1
            )
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_kemaiyunding_inventory(self):
        """
        清洗科脉云鼎库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["t_sk_master"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames["fbrh_no"]
            frames["foreign_item_id"] = frames["fitem_id"]
            frames["quantity"] = frames["fqty"]
            frames["amount"] = frames["fcost_amt"]
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_hongye_inventory(self):
        """
        清洗宏业库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
        """
        store_id_len_map = {"34": 4, "61": 3, "65": 3, "85": 3, "92": 4, "94": 4, "95": 3, "97": 3, "98": 3}
        store_id_len = store_id_len_map[self.cmid]
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["acc_incodeamount"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["foreign_store_id"] = frames.apply(
                lambda row: row["deptcode"][: store_id_len], axis=1
            )
            frames["foreign_item_id"] = frames["gdsincode"].str.strip()
            frames["quantity"] = frames["nowamount"]
            frames["amount"] = frames["nowinmoney"]
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_haiding_inventory(self):
        columns = [
            "cmid",
            "foreign_store_id",
            "foreign_item_id",
            "date",
            "quantity",
            "amount",
        ]
        if self.source_id == "101YYYYYYYYYYYY":
            base_table_date = (datetime.now(tz=_TZINFO) - timedelta(days=2)).strftime("%Y-%m-%d")
            prefix = f"datapipeline/source_id=101YYYYYYYYYYYY/ext_date={base_table_date}/table=store/"
            key = get_matching_s3_keys(S3_BUCKET, prefix=prefix, suffix=".csv.gz")
            key = f"s3://{S3_BUCKET}/{key}"
            store_data = pd.read_csv(key, encoding="utf-8")
            store_data = store_data[store_data["orggid"] == 1003890]
            store_data["gid"] = str(store_data["gid"])
            actinvs = self.data.get("actinvs")
            inventory = actinvs.merge(store_data, how="left", left_on="store", right_on="gid")
        else:
            inventory = self.data.get("actinvs")
        if len(inventory) == 0:
            return pd.DataFrame(columns=columns)
        else:
            inventory["cmid"] = self.cmid
            inventory["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            inventory = inventory[inventory['store'] != 1000000]
            inventory['amount'] = inventory['amt'] + inventory['tax']
            inventory = inventory.groupby(['cmid', 'gdgid', 'store', 'date']).agg(
                {'qty': 'sum', 'amount': 'sum'}).reset_index()
            inventory = inventory.rename(
                columns={
                    "gdgid": "foreign_item_id",
                    "store": "foreign_store_id",
                    'qty': 'quantity',
                }
            )
            inventory = inventory[['cmid', 'foreign_store_id', 'foreign_item_id', 'date', 'quantity', 'amount']]
            part = inventory[columns]
        return self.up_load_to_s3(part)

    def clean_haixinv5_inventory(self):
        columns = [
            "cmid",
            "foreign_store_id",
            "foreign_item_id",
            "date",
            "quantity",
            "amount",
        ]
        inventory = self.data.get("tstklskc")
        if len(inventory) == 0:
            return pd.DataFrame(columns=columns)
        inventory["cmid"] = self.cmid
        inventory["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
        inventory = inventory[inventory['orgcode'] != '00']
        inventory["orgcode"] = inventory["orgcode"].str.strip()
        inventory["pluid"] = inventory["pluid"].str.strip()
        # inventory = inventory.groupby(['cmid', 'pluid', 'orgcode', 'date']).agg(
        #     {'kccount': 'sum', 'hcost': 'sum'}).reset_index()
        inventory = inventory.rename(
            columns={
                "pluid": "foreign_item_id",
                "orgcode": "foreign_store_id",
                'kccount': 'quantity',
                'hcost': 'amount'
            }
        )
        inventory = inventory[['cmid', 'foreign_store_id', 'foreign_item_id', 'date', 'quantity', 'amount']]
        part = inventory[columns]
        return self.up_load_to_s3(part)

    def clean_kemaiv9_inventory(self):
        """
        清洗科脉御商v9库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["ic_t_branch_stock"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames = frames[frames["cost_amt"].notnull()]
            frames = frames.rename(
                columns={
                    "branch_id": "foreign_store_id",
                    "item_id": "foreign_item_id",
                    "stock_qty": "quantity",
                    "cost_amt": "amount"
                }
            )
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_meishilin_inventory(self):
        """
        清洗美食林库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["skstoregdsskuinf"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames["amount"] = frames["amt"] + frames["tax"]
            frames = frames.rename(
                columns={
                    "store": "foreign_store_id",
                    "gdgid": "foreign_item_id",
                    "qty": "quantity"
                }
            )

            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def clean_chaoying_inventory(self):
        """
        清洗超赢库存表
        :param source_id:
        :param date:
        :param target_table:
        :param data_frames:
        :return:
        """
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["tz_sp_kc"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            frames = frames.rename(
                columns={
                    "id_gsjg": "foreign_store_id",
                    "id_sp": "foreign_item_id",
                    "sl_qm": "quantity",
                    "je_qm_hs": "amount"
                }
            )
            frames = frames[columns]
            frames = frames[
                (frames["quantity"] != 0)
                | (frames["amount"] != 0)
                ]
        return self.up_load_to_s3(frames)

    def up_load_to_s3(self, dataframe):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as file:
            count = len(dataframe)
            dataframe.to_csv(
                file.name, index=False, compression="gzip", float_format="%.2f"
            )
            file.seek(0)
            key = INV_CLEANED_PATH.format(
                source_id=self.source_id,
                target_table=self.target_table,
                date=self.date,
                timestamp=datetime.fromtimestamp(time.time(), tz=_TZINFO),
                rowcount=count,
                hour=self.hour
            )
            S3.Bucket(S3_BUCKET).upload_file(file.name, key)

        print(key)
        return key


def get_matching_s3_keys(bucket, prefix="", suffix=""):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """

    objects = S3.Bucket(bucket).objects.filter(Prefix=prefix)
    objects = sorted([obj for obj in objects if obj.key.endswith(suffix)],
                     key=lambda obj: int(obj.last_modified.strftime("%s")))
    return objects[-1].key


def map_converter(converts: dict):
    for table, convert in converts.items():
        for column, value in convert.items():
            convert[column] = CONVERTERS[value]
        converts[table] = convert


def fetch_data_frames(keys, origin_table_columns, converts):
    datas = {}
    content = S3.Object(S3_BUCKET, keys).get()
    data = json.loads(content["Body"].read().decode("utf-8"))
    print(data)
    extract_data_dict = data["extract_data"]
    for table_name, records in extract_data_dict.items():
        if (
                table_name in origin_table_columns.keys()
                and table_name not in datas.keys()
        ):
            datas[table_name] = records

    data_frames = {}

    for table, columns in origin_table_columns.items():
        for csv_path in datas[table]:
            key = f"s3://{S3_BUCKET}/{csv_path}"
            if table in converts:
                frame = pd.read_csv(
                    key, compression="gzip", usecols=columns, converters=converts[table]
                )
            else:
                frame = pd.read_csv(key, compression="gzip", usecols=columns)

            if table in data_frames:
                data_frames[table] = data_frames[table].append(frame)
            else:
                data_frames[table] = frame.copy(deep=True)
    return data_frames


def handler(event, context):
    # Check if the incoming message was sent by SNS
    if "Records" in event:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
    else:
        message = event

    source_id = message["source_id"]
    erp_name = message["erp_name"]
    date = message["date"]
    target_table = message["target_table"]
    origin_table_columns = message["origin_table_columns"]
    converts = message["converts"]
    # inventory_table = list(origin_table_columns.keys())[0]
    hour = datetime.now(tz=_TZINFO).hour
    hour_delta = hour - 3
    print(hour)

    map_converter(converts)

    while True:
        prefix = INV_HISTORY_HUMP_JSON.format(source_id=source_id, date=date, hour=str(hour))
        print(prefix)
        objects = list(S3.Bucket(S3_BUCKET).objects.filter(Prefix=prefix))

        if objects is not None and len(objects) > 0:
            break
        hour -= 1

        if hour == hour_delta:
            print('当前时间段没有找到数据，退出')
            return
    print("清洗{}点的那一份".format(str(hour)))

    keys = get_matching_s3_keys(
        S3_BUCKET,
        prefix=INV_HISTORY_HUMP_JSON.format(source_id=source_id, date=date, hour=hour),
        suffix=".json",
    )
    data_frames = fetch_data_frames(keys, origin_table_columns, converts)
    for k, v in data_frames.items():
        data_frames[k] = v.applymap(
            lambda e: BLANK_CHAR.sub(" ", e).strip() if isinstance(e, str) else e
        )

    if erp_name == "科脉云鼎":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_kemaiyunding_inventory()
    elif erp_name == "海鼎":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_haiding_inventory()
    elif erp_name == '海信商定天下v5' or erp_name == '海信商定天下':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_haixinv5_inventory()
    elif erp_name == '宏业':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_hongye_inventory()
    elif erp_name == '思迅' or erp_name == "衡阳联邦":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_sixun_inventory()
    elif erp_name == "便宅家中间库":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_bianzhaijia_inventory()
    elif erp_name == "商海（连锁版）":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_shanghai_inventory()
    elif erp_name == "晋中田森":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_tianshen_inventory()
    elif erp_name == "昂捷-中间库":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_angjie_inventory()
    elif erp_name == "智百威":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_zhibaiwei_inventory()
    elif erp_name == "富基融通":
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_fujirongtong_inventory()
    elif erp_name == '科脉御商v9':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_kemaiv9_inventory()
    elif erp_name == '美食林':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_meishilin_inventory()
    elif erp_name == '超赢':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_chaoying_inventory()


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp


if __name__ == "__main__":
    pass
