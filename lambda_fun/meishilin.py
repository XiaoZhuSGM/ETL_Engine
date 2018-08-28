# -*- coding: utf-8 -*-
# @Time    : 2018/8/27 10:38
# @Author  : 范佳楠
import ssl
import tempfile
import time
from datetime import datetime

import boto3
import pandas as pd
import pytz
from typing import Dict

S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"
_TZINFO = pytz.timezone("Asia/Shanghai")


class MeiShiLinCleaner(object):

    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        self.source_id = source_id
        self.date = date
        self.cmid = self.source_id.split('Y', 1)[0]
        self.data = data

    def clean(self, target_table):
        method = getattr(self, target_table, None)
        if method and callable(method):
            df = getattr(self, target_table)()
            return self.up_load_to_s3(df, target_table)
        else:
            raise RuntimeError(f"没有这个表:{target_table}")

    def up_load_to_s3(self, dataframe, target_table):
        file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8')
        count = len(dataframe)
        dataframe.to_csv(file.name, index=False, compression='gzip', float_format='%.4f')
        file.seek(0)
        key = CLEANED_PATH.format(
            source_id=self.source_id,
            target_table=target_table,
            date=self.date,
            timestamp=datetime.fromtimestamp(time.time(), tz=_TZINFO),
            rowcount=count,
        )

        S3.Bucket(S3_BUCKET).upload_file(file.name, key)

        return key

    """
    "origin_table_columns": {
            "dbo.skstoresellingwater": ['sgid','gid','flowno','rtlprc','qty','realamt','fildate']
            "dbo.skstore":['gid', 'name'],
            "dbo.skgoods":['gid', 'code2', 'name', 'munit'],
            "dbo.skgoodssort":['gid', 'ascode', 'asname', 'bscode', 'bsname', 'cscode', 'csname']
        },

    "converts": {
        "dbo.skstoresellingwater": {"sgid": "str", "gid": "str", "flowno": "str"， "rtlprc":"float", 'qty':'float', 'realamt':'float'}
        "dbo.skstore": {'gid':'str', 'name':'str'}
        'dbo.skgoods': {'gid': 'str', 'code2':'str', 'name':'str', 'munit':'str'},
        'dbo.skgoodssort':{'gid':'str', 'ascode':'str','asname':'str', 'bscode':'str', 'bsname':'str', 'cscode':'str', 'csname':'str'}
    }
    """

    def goodsflow(self):
        flow_frame = self.data['dbo.skstoresellingwater']
        store_frame = self.data['dbo.skstore']
        goods_frame = self.data['dbo.skgoods']
        gsort_frame = self.data['dbo.skgoodssort']
        flow_frame['flowno'] = flow_frame['flowno'].str.strip()
        result_frame = pd.merge(flow_frame,
                                store_frame,
                                left_on='sgid',
                                right_on='gid',
                                how='left',
                                suffixes=('_flow', '_store')).merge(goods_frame,
                                                                    left_on='gid_flow',
                                                                    right_on='gid',
                                                                    how='left',
                                                                    suffixes=('_store', '_goods')).merge(gsort_frame,
                                                                                                         left_on='gid',
                                                                                                         right_on='gid',
                                                                                                         how='left')

        result_frame['source_id'] = self.source_id
        result_frame['cmid'] = self.cmid
        result_frame['last_updated'] = datetime.now()
        result_frame['foreign_category_lv4'] = ''
        result_frame['foreign_category_lv4_name'] = None
        result_frame['foreign_category_lv5'] = ''
        result_frame['foreign_category_lv5_name'] = None
        result_frame['pos_id'] = ''
        result_frame['consumer_id'] = None

        result_frame.rename(columns={
            'gid_store': 'foreign_store_id',
            'name_store': 'store_name',
            'flow_no': 'receipt_id',
            'fildate': 'saletime',
            'gid': 'foreign_item_id',
            'code2': 'barcode',
            'name_goods': 'item_name',
            'munit': 'item_unit',
            'rtlprc': 'saleprice',
            'qty': 'quantity',
            'realamt': 'subtotal',
            'ascode': 'foreign_category_lv1',
            'asname': 'foreign_category_lv1_name',
            'bscode': 'foreign_category_lv2',
            'bsname': 'foreign_category_lv2_name',
            'cscode': 'foreign_category_lv3',
            'csname': 'foreign_category_lv3_name'

        })

        result_frame = result_frame[[
            'source_id',
            'cmid',
            'foreign_store_id',
            'store_name',
            'receipt_id',
            'consumer_id',
            'saletime',
            'last_updated',
            'foreign_item_id',
            'barcode',
            'item_name',
            'item_unit',
            'saleprice',
            'quantity',
            'subtotal',
            'foreign_category_lv1',
            'foreign_category_lv1_name',
            'foreign_category_lv2',
            'foreign_category_lv2_name',
            'foreign_category_lv3',
            'foreign_category_lv3_name',
            'foreign_category_lv4',
            'foreign_category_lv4_name',
            'foreign_category_lv5',
            'foreign_category_lv5_name',
            'pos_id'
        ]]

        return result_frame
