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
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/" \
               "target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


class LiuYiCleaner:
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

    def _goodsflow_subquery(self):
        lv = self.data['tb_gdsclass']
        origin_table = lv.copy()
        lv['item_clsno_1'] = lv['c_ccode'].apply(lambda x: x[:1])
        lv['item_clsno_2'] = lv['c_ccode'].apply(lambda x: x[:2])
        lv['item_clsno_3'] = lv['c_ccode'].apply(lambda x: x[:3])
        lv['item_clsno_4'] = lv['c_ccode'].apply(lambda x: x[:5])
        df = lv.merge(
            origin_table,
            how='left',
            left_on=['item_clsno_4'],
            right_on=['c_ccode'],
            suffixes=('_lv5', '_lv4')
        ).merge(
            origin_table,
            how='left',
            left_on=['item_clsno_3'],
            right_on=['c_ccode']
        ).merge(
            origin_table,
            how='left',
            left_on=['item_clsno_2'],
            right_on=['c_ccode'],
            suffixes=('_lv3', '_lv2')
        ).merge(
            origin_table,
            how='left',
            left_on=['item_clsno_1'],
            right_on=['c_ccode']
        )
        df = df[df['c_ccode_lv5'].str.len() == 7]
        df = df.rename(columns={
            'c_ccode': 'foreign_category_lv1',
            'c_name': 'foreign_category_lv1_name',
            'c_ccode_lv2': 'foreign_category_lv2',
            'c_name_lv2': 'foreign_category_lv2_name',
            'c_ccode_lv3': 'foreign_category_lv3',
            'c_name_lv3': 'foreign_category_lv3_name',
            'c_ccode_lv4': 'foreign_category_lv4',
            'c_name_lv4': 'foreign_category_lv4_name',
            'c_ccode_lv5': 'foreign_category_lv5',
            'c_name_lv5': 'foreign_category_lv5_name',

        })
        df = df[[
            'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2', 'foreign_category_lv2_name',
            'foreign_category_lv3', 'foreign_category_lv3_name', 'foreign_category_lv4', 'foreign_category_lv4_name',
            'foreign_category_lv5', 'foreign_category_lv5_name'
        ]]
        return df

    def goodsflow(self):
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

        flow = self.data['tb_pos_flow_goods']
        store = self.data['tb_store']
        lv = self._goodsflow_subquery()
        df = flow.merge(store, how='left', left_on='c_store_id', right_on='c_id', suffixes=('_flow', '_store'))\
            .merge(lv, left_on='c_ccode', right_on='foreign_category_lv5')
        if df.shape[0] == 0:
            return pd.DataFrame(columns=columns)
        df = df.rename(columns={
            'c_id': 'foreign_store_id',
            'c_name_store': 'store_name',
            'c_no': 'receipt_id',
            'c_sale_time': 'saletime',
            'c_gcode': 'foreign_item_id',
            'c_barcode': 'barcode',
            'c_basic_unit': 'item_unit',
            'c_qty_sum': 'quantity',
            'c_amount': 'subtotal',
            'c_pos': 'pos_id',
            'c_name_flow': 'item_name',
        })
        df['source_id'] = self.source_id
        df['cmid'] = self.cmid
        df['consumer_id'] = ''
        df['last_updated'] = datetime.now(_TZINFO)
        df['saleprice'] = df.apply(
            lambda row: row['c_price_sale'] if row['c_pro_status'] in ('普通商品', '组合商品', '')
            else row['c_price_sale_disc'], axis=1
        )
        return df[columns]

    def cost(self):
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
        cost = self.data['tb_o_sg']
        goods = self.data['tbgoods']
        df = cost.merge(goods, left_on='c_gcode', right_on='goodscode')
        if df.shape[0] == 0:
            return pd.DataFrame(columns=columns)
        df = df.rename(columns={
            'c_store_id': 'foreign_store_id',
            'c_gcode': 'foreign_item_id',
            'c_type': 'cost_type',
            'c_qtty': 'total_quantity',
            'c_amount': 'total_sale',
            'c_pt_cost': 'total_cost',
            'categorycode': 'foreign_category_lv4',
        })
        df['date'] = df['c_datetime'].apply(lambda x: x.split(' ')[0])
        df['source_id'] = self.source_id
        df['foreign_category_lv1'] = df['foreign_category_lv4'].apply(lambda x: x[:1])
        df['foreign_category_lv2'] = df['foreign_category_lv4'].apply(lambda x: x[:3])
        df['foreign_category_lv3'] = df['foreign_category_lv4'].apply(lambda x: x[:5])
        df['foreign_category_lv5'] = ''
        df['cmid'] = self.cmid
        return df[columns]

    def store(self):
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
        df = self.data['tb_store']
        if df.shape[0] == 0:
            return pd.DataFrame(columns=columns)
        df = df.rename(columns={
            'c_id': 'foreign_store_id',
            'c_name': 'store_name',
            'c_address': 'store_address',
            'c_region': 'address_code',
            'c_status': 'store_status',
            'c_open_date': 'create_date',
            'c_web_page': 'show_code',
            'c_tele': 'phone_number',
            'c_server_name': 'contacts',
            'c_ownership_type': 'property',
        })
        df['cmid'] = self.cmid
        df['device_id'] = ''
        df['lat'] = ''
        df['lng'] = ''
        df['area_code'] = ''
        df['area_name'] = None
        df['business_area'] = ''
        df['property_id'] = ''
        df['source_id'] = self.source_id
        df['last_updated'] = datetime.now(_TZINFO)
        df = df[columns]
        return df

    def goods(self):
        columns = [
            "cmid", "barcode", "foreign_item_id", "item_name", "lastin_price", "sale_price", "item_unit", "item_status",
            "foreign_category_lv1", "foreign_category_lv2", "foreign_category_lv3", "foreign_category_lv4",
            "storage_time", "last_updated", "isvalid", "warranty", "show_code", "foreign_category_lv5", "allot_method",
            "supplier_name", "supplier_code", "brand_name"
        ]
        goods = self.data['tb_gds']
        vendor = self.data['tbsupplier']
        df = goods.merge(vendor, how='left', left_on='c_provider', right_on='suppliercode')
        df = df[df['c_ccode'] != '**']
        if df.shape[0] == 0:
            return pd.DataFrame(columns=columns)
        df['foreign_category_lv1'] = df['c_ccode'].apply(lambda x: x[:1])
        df['foreign_category_lv2'] = df['c_ccode'].apply(lambda x: x[:3])
        df['foreign_category_lv3'] = df['c_ccode'].apply(lambda x: x[:5])
        df = df.rename(columns={
            'c_barcode': 'barcode',
            'c_gcode': 'foreign_item_id',
            'c_name': 'item_name',
            'c_price': 'sale_price',
            'c_basic_unit': 'item_unit',
            'c_status': 'item_status',
            'c_ccode': 'foreign_category_lv4',
            'c_introduce_date': 'storage_time',
            'c_sale_frequency': 'isvalid',
            'c_od_day': 'warranty',
            'c_delivery_type': 'allot_method',
            'suppliername': 'supplier_name',
            'suppliercode': 'supplier_code',
            'c_brand': 'brand_name',
        })
        df['cmid'] = self.cmid
        df['lastin_price'] = ''
        df['foreign_category_lv5'] = ''
        df['last_updated'] = datetime.now(_TZINFO)
        df['show_code'] = df['foreign_item_id']
        df = df[columns]

        return df

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
            "foreign_category_lv5_name"
        ]
        lv = self.data['tb_gdsclass']
        
        df1 = lv.copy()
        df1 = df1[df1['c_ccode'].str.len() == 1]
        if df1.shape[0] == 0:
            df1 = pd.DataFrame(columns=columns)
        else:
            df1 = df1.rename(columns={
                'c_ccode': 'foreign_category_lv1',
                'c_name': 'foreign_category_lv1_name',
            })
            df1['cmid'] = self.cmid
            df1['level'] = 1
            df1['foreign_category_lv2'] = ''
            df1['foreign_category_lv2_name'] = None
            df1['foreign_category_lv3'] = ''
            df1['foreign_category_lv3_name'] = None
            df1['foreign_category_lv4'] = ''
            df1['foreign_category_lv4_name'] = None
            df1['foreign_category_lv5'] = ''
            df1['foreign_category_lv5_name'] = None
            df1['last_updated'] = datetime.now(_TZINFO)
            df1 = df1[columns]

        df2 = lv.copy()
        df2['item_clsno_1'] = df2['c_ccode'].str.slice(0, 1)
        df2 = df2.merge(
            lv,
            how='left',
            left_on=['item_clsno_1'],
            right_on=['c_ccode'],
            suffixes=('_lv2', '_lv1')
        )
        df2 = df2[df2['c_ccode_lv2'].str.len() == 2]
        if df2.shape[0] == 0:
            df2 = pd.DataFrame(columns=columns)
        else:
            df2 = df2.rename(columns={
                'c_ccode_lv1': 'foreign_category_lv1',
                'c_name_lv1': 'foreign_category_lv1_name',
                'c_ccode_lv2': 'foreign_category_lv2',
                'c_name_lv2': 'foreign_category_lv2_name',
            })
            df2['cmid'] = self.cmid
            df2['level'] = 2
            df2['foreign_category_lv3'] = ''
            df2['foreign_category_lv3_name'] = None
            df2['foreign_category_lv4'] = ''
            df2['foreign_category_lv4_name'] = None
            df2['foreign_category_lv5'] = ''
            df2['foreign_category_lv5_name'] = None
            df2['last_updated'] = datetime.now(_TZINFO)
            df2 = df2[columns]

        df3 = lv.copy()
        df3['item_clsno_1'] = df3['c_ccode'].str.slice(0, 1)
        df3['item_clsno_2'] = df3['c_ccode'].str.slice(0, 2)
        df3 = df3.merge(
            lv,
            how='left',
            left_on=['item_clsno_2'],
            right_on=['c_ccode'],
            suffixes=('_lv3', '_lv2')
        ).merge(
            lv,
            how='left',
            left_on=['item_clsno_1'],
            right_on=['c_ccode']
        )
        df3 = df3[df3['c_ccode_lv3'].str.len() == 3]
        if df3.shape[0] == 0:
            df3 = pd.DataFrame(columns=columns)
        else:
            df3 = df3.rename(columns={
                'c_ccode': 'foreign_category_lv1',
                'c_name': 'foreign_category_lv1_name',
                'c_ccode_lv2': 'foreign_category_lv2',
                'c_name_lv2': 'foreign_category_lv2_name',
                'c_ccode_lv3': 'foreign_category_lv3',
                'c_name_lv3': 'foreign_category_lv3_name'
            })
            df3['cmid'] = self.cmid
            df3['level'] = 3
            df3['foreign_category_lv4'] = ''
            df3['foreign_category_lv4_name'] = None
            df3['foreign_category_lv5'] = ''
            df3['foreign_category_lv5_name'] = None
            df3['last_updated'] = datetime.now(_TZINFO)
            df3 = df3[columns]

        df4 = lv.copy()
        df4['item_clsno_1'] = df4['c_ccode'].str.slice(0, 1)
        df4['item_clsno_2'] = df4['c_ccode'].str.slice(0, 2)
        df4['item_clsno_3'] = df4['c_ccode'].str.slice(0, 3)
        df4 = df4.merge(
            lv,
            how='left',
            left_on=['item_clsno_3'],
            right_on=['c_ccode'],
            suffixes=('_lv4', '_lv3')
        ).merge(
            lv,
            how='left',
            left_on=['item_clsno_2'],
            right_on=['c_ccode']
        ).merge(
            lv,
            how='left',
            left_on=['item_clsno_1'],
            right_on=['c_ccode'],
            suffixes=('_lv2', '_lv1')
        )
        df4 = df4[df4['c_ccode_lv4'].str.len() == 5]
        if df4.shape[0] == 0:
            df4 = pd.DataFrame(columns=columns)
        else:
            df4 = df4.rename(columns={
                'c_ccode_lv1': 'foreign_category_lv1',
                'c_name_lv1': 'foreign_category_lv1_name',
                'c_ccode_lv2': 'foreign_category_lv2',
                'c_name_lv2': 'foreign_category_lv2_name',
                'c_ccode_lv3': 'foreign_category_lv3',
                'c_name_lv3': 'foreign_category_lv3_name',
                'c_ccode_lv4': 'foreign_category_lv4',
                'c_name_lv4': 'foreign_category_lv4_name'
            })
            df4['cmid'] = self.cmid
            df4['level'] = 4
            df4['foreign_category_lv5'] = ''
            df4['foreign_category_lv5_name'] = None
            df4['last_updated'] = datetime.now(_TZINFO)
            df4 = df4[columns]

        df5 = lv.copy()
        df5['item_clsno_1'] = df5['c_ccode'].str.slice(0, 1)
        df5['item_clsno_2'] = df5['c_ccode'].str.slice(0, 2)
        df5['item_clsno_3'] = df5['c_ccode'].str.slice(0, 3)
        df5['item_clsno_4'] = df5['c_ccode'].str.slice(0, 5)
        df5 = df5.merge(
            lv,
            how='left',
            left_on=['item_clsno_4'],
            right_on=['c_ccode'],
            suffixes=('_lv5', '_lv4')
        ).merge(
            lv,
            how='left',
            left_on=['item_clsno_3'],
            right_on=['c_ccode'],
        ).merge(
            lv,
            how='left',
            left_on=['item_clsno_2'],
            right_on=['c_ccode'],
            suffixes=('_lv3', '_lv2')
        ).merge(
            lv,
            how='left',
            left_on=['item_clsno_1'],
            right_on=['c_ccode']
        )
        df5 = df5[df5['c_ccode_lv5'].str.len() == 7]
        if df5.shape[0] == 0:
            df5 = pd.DataFrame(columns=columns)
        else:
            df5 = df5.rename(columns={
                'c_ccode': 'foreign_category_lv1',
                'c_name': 'foreign_category_lv1_name',
                'c_ccode_lv2': 'foreign_category_lv2',
                'c_name_lv2': 'foreign_category_lv2_name',
                'c_ccode_lv3': 'foreign_category_lv3',
                'c_name_lv3': 'foreign_category_lv3_name',
                'c_ccode_lv4': 'foreign_category_lv4',
                'c_name_lv4': 'foreign_category_lv4_name',
                'c_ccode_lv5': 'foreign_category_lv5',
                'c_name_lv5': 'foreign_category_lv5_name',
            })
            df5['cmid'] = self.cmid
            df5['level'] = 5
            df5['last_updated'] = datetime.now(_TZINFO)
            df5 = df5[columns]

        return pd.concat([df1, df2, df3, df4, df5])

