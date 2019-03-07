# -*- coding: utf-8 -*-
"""
库存清洗逻辑的入口
"""
import json

import boto3
import pandas as pd
import pytz
import re
from datetime import datetime
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
            frames["amount"] = frames["stock_qty"] * frames["avg_cost"]
            frames = frames[columns]
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
        columns = ["cmid", "foreign_store_id", "foreign_item_id", "date", "quantity", "amount"]
        cmid = self.cmid
        frames = self.data["acc_incodeamount"]
        if not len(frames):
            frames = pd.DataFrame(columns=columns)
        else:
            frames["cmid"] = cmid
            frames["date"] = datetime.now(_TZINFO).strftime("%Y-%m-%d")
            if self.source_id == '85YYYYYYYYYYYYY':
                frames["deptcode"] = frames["deptcode"].apply(lambda x: x[:3])
            elif self.source_id == "34YYYYYYYYYYYYY":
                frames["deptcode"] = frames["deptcode"].apply(lambda x: x[:4])
            frames["foreign_store_id"] = frames["deptcode"]
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
        inventory = self.data.get("actinvs")
        if len(inventory) == 0:
            return pd.DataFrame(columns=columns)
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
    elif erp_name == '海信商定天下v5':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_haixinv5_inventory()
    elif erp_name == '宏业':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_hongye_inventory()
    elif erp_name == '思迅':
        cleaner = InventoryCleaner(source_id, date, data_frames, hour, target_table)
        return cleaner.clean_sixun_inventory()


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp


if __name__ == "__main__":
    pass
