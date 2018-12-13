# -*- coding: utf-8 -*-

import ssl
import tempfile
import time
from datetime import datetime

import boto3
import pandas as pd
import pytz

ssl._create_default_https_context = ssl._create_unverified_context

_TZINFO = pytz.timezone("Asia/Shanghai")

S3_BUCKET = "ext-etl-data"
S3 = boto3.resource("s3")
CLEANED_PATH = "clean_data/source_id={source_id}/clean_date={date}/target_table={target_table}/dump={timestamp}&rowcount={rowcount}.csv.gz"


def clean_lianbang(source_id, date, target_table, data_frames):
    if target_table == "goodsflow":
        return clean_goodsflow(source_id, date, target_table, data_frames)
    elif target_table == "cost":
        return clean_cost(source_id, date, target_table, data_frames)
    elif target_table == "store":
        return clean_store(source_id, date, target_table, data_frames)
    elif target_table == "goods":
        return clean_goods(source_id, date, target_table, data_frames)
    elif target_table == "category":
        return clean_category(source_id, date, target_table, data_frames)


# 门店表
'''
  "origin_table_columns": {
            "t_bd_branch_info": [
                "branch_no",
                "branch_name",
                "address",
                "branch_tel",
                "branch_man"
            ]
        },
        "converts": {
            "t_bd_branch_info": {
                "branch_no": "str",
                "branch_tel": "str"
            }
        }
'''


def clean_store(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    store = data_frames['t_bd_branch_info']
    store['cmid'] = cmid
    store['source_id'] = source_id
    store['show_code'] = store['branch_no']
    store = store[store["trade_type"] != '5']

    def area_type_convert(area_code):
        if area_code == '00':
            return '默认区域'
        if area_code == '01':
            return '江东'
        if area_code == '02':
            return '湘江西'
        if area_code == '03':
            return '加盟'

    def property_type_convert(code):
        if code == '0':
            return '总部'
        if code == '5':
            return '仓库'
        if code == '3':
            return '自营店'

    store = store.rename(columns={'branch_no': 'foreign_store_id',
                                  'branch_name': 'store_name',
                                  'address': 'store_address',
                                  'branch_tel': 'phone_number',
                                  'branch_man': 'contacts',
                                  'branch_clsno': 'area_code',
                                  'trade_type': 'property_id'
                                  })

    store['area_name'] = store['area_code'].map(area_type_convert)
    store['property'] = store['property_id'].map(property_type_convert)
    store['address_code'] = ''
    store['device_id'] = ''
    store['create_date'] = datetime.now(_TZINFO)
    store['lat'] = None
    store['lng'] = None
    store['last_updated'] = datetime.now(_TZINFO)
    store['business_area'] = None
    store['store_status'] = None
    store = store[[
        'cmid',
        'foreign_store_id',
        'store_name',
        'store_address',
        'address_code',
        'device_id',
        'store_status',
        'create_date',
        'lat',
        'lng',
        'show_code',
        'phone_number',
        'contacts',
        'area_code',
        'area_name',
        'business_area',
        'property_id',
        'property',
        'source_id',
        'last_updated'
    ]]
    return upload_to_s3(store, source_id, date, target_table)


# 商品分类

'''
    {
        "source_id": "91YYYYYYYYYYYYY",
        "erp_name": "衡阳联邦",
        "date": "2018-11-14",
        "target_table": "category",
        "origin_table_columns": {
            "t_bd_item_cls": [
                "item_clsno",
                "item_clsname",
                "cls_parent"
            ]
        },
        "converts": {
            "t_bd_item_cls": {
                "cls_parent": "str",
                "item_clsname": "str",
                "item_clsno": "str"
            }
        }
    }
'''


def clean_category(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    lv = data_frames['t_bd_item_cls']
    lv1 = data_frames['t_bd_item_cls'].rename(columns=lambda x: f"lv1.{x}")
    lv2 = data_frames['t_bd_item_cls'].rename(columns=lambda x: f"lv2.{x}")
    lv3 = data_frames['t_bd_item_cls'].rename(columns=lambda x: f"lv3.{x}")

    # # 处理lv1
    lv1 = lv1[lv1['lv1.cls_parent'].str.len() == 0]
    lv1 = lv1.fillna('')
    lv1['cmid'] = cmid
    lv1['level'] = 1
    lv1['last_updated'] = datetime.now(_TZINFO)
    lv1['foreign_category_lv2'] = ''
    lv1['foreign_category_lv2_name'] = None
    lv1['foreign_category_lv3'] = ''
    lv1['foreign_category_lv3_name'] = None
    lv1['foreign_category_lv4'] = ''
    lv1['foreign_category_lv4_name'] = None
    lv1['foreign_category_lv5'] = ''
    lv1['foreign_category_lv5_name'] = None
    lv1 = lv1.rename(columns={'lv1.item_clsno': 'foreign_category_lv1',
                              'lv1.item_clsname': 'foreign_category_lv1_name'})
    lv1 = lv1[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    # # 处理lv2
    lv2 = lv2[(lv2['lv2.cls_parent'].str.len() == 1) | (lv2['lv2.cls_parent'].str.len() == 2)]
    lv2 = lv2.merge(lv, how='left', left_on='lv2.cls_parent', right_on='item_clsno')
    lv2 = lv2.rename(columns={'lv2.item_clsno': 'foreign_category_lv2',
                              'lv2.item_clsname': 'foreign_category_lv2_name',
                              'lv2.cls_parent': 'foreign_category_lv1',
                              'item_clsname': 'foreign_category_lv1_name'})
    lv2['level'] = 2
    lv2['foreign_category_lv3'] = ''
    lv2['foreign_category_lv3_name'] = None
    lv2['foreign_category_lv4'] = ''
    lv2['foreign_category_lv4_name'] = None
    lv2['foreign_category_lv5'] = ''
    lv2['foreign_category_lv5_name'] = None
    lv2['cmid'] = cmid
    lv2['last_updated'] = datetime.now(_TZINFO)
    lv2 = lv2[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]

    # # 处理lv3
    lv3 = lv3[lv3['lv3.cls_parent'].str.len() == 4]
    lv3 = lv3.merge(lv, how='left', left_on='lv3.cls_parent', right_on='item_clsno')
    lv3 = lv3.merge(lv, how='left', left_on='cls_parent', right_on='item_clsno',
                    suffixes=('_lv2', '_lv1'))
    lv3 = lv3.rename(columns={
        'lv3.item_clsno': 'foreign_category_lv3',
        'lv3.item_clsname': 'foreign_category_lv3_name',
        'lv3.cls_parent': 'foreign_category_lv2',
        'item_clsname_lv2': 'foreign_category_lv2_name',
        'cls_parent_lv2': 'foreign_category_lv1',
        'item_clsname_lv1': 'foreign_category_lv1_name'
    })
    lv3['foreign_category_lv4'] = ''
    lv3['foreign_category_lv4_name'] = None
    lv3['foreign_category_lv5'] = ''
    lv3['foreign_category_lv5_name'] = None
    lv3['level'] = 3
    lv3['cmid'] = cmid
    lv3['last_updated'] = datetime.now(_TZINFO)
    lv3 = lv3[[
        'cmid', 'level', 'foreign_category_lv1', 'foreign_category_lv1_name', 'foreign_category_lv2',
        'foreign_category_lv2_name', 'foreign_category_lv3', 'foreign_category_lv3_name',
        'last_updated', 'foreign_category_lv4', 'foreign_category_lv4_name', 'foreign_category_lv5',
        'foreign_category_lv5_name'
    ]]
    # ###整理
    category_frame = pd.concat([lv1, lv2, lv3])

    return upload_to_s3(category_frame, source_id, date, target_table)


# 商品

'''
  {
        "source_id": "91YYYYYYYYYYYYY",
        "erp_name": "衡阳联邦",
        "date": "2018-11-14",
        "target_table": "goods",
        "origin_table_columns": {
            "t_bd_item_brand": [
                "item_brand",
                "item_brandname"
            ],
            "t_bd_item_info": [
                "item_clsno",
                "main_supcust",
                "status",
                "item_no",
                "item_name",
                "price",
                "sale_price",
                "unit_no",
                "item_brand",
                "build_date",
                "item_subno"
            ],
            "t_bd_supcust_info": [
                "supcust_no",
                "sup_name",
                "supcust_flag"
            ]
        },
        "converts":
            {
                "t_bd_item_brand": {
                    "item_brand": "str"
                },
                "t_bd_item_info": {
                    "item_brand": "str",
                    "item_clsno": "str",
                    "item_name": "str",
                    "item_no": "str",
                    "main_supcust": "str",
                    "status": "str",
                    'item_subno':'str'
                },
                "t_bd_supcust_info": {
                    "supcust_no": "str"
                }
            }
    }

'''


def clean_goods(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    goods: pd.DataFrame = data_frames['t_bd_item_info']
    supcust = data_frames['t_bd_supcust_info']
    brand = data_frames['t_bd_item_brand']
    goods = goods.merge(
        brand, how='left', on='item_brand').merge(
        supcust, left_on='main_supcust', right_on='supcust_no')
    goods['foreign_category_lv1'] = goods['item_clsno'].apply(
        lambda x: x[:1] if (len(x) == 1 or len(x) == 3) else x[:2])
    goods['foreign_category_lv2'] = goods['item_clsno'].apply(
        lambda x: '' if (len(x) == 1 or len(x) == 2)
        else x if (len(x) == 3 or len(x) == 4)
        else x[:4])
    goods['foreign_category_lv3'] = goods['item_clsno'].apply(
        lambda x: x if len(x) == 6 else '')
    goods = goods.rename(columns={
        'item_no': 'foreign_item_id',
        'price': 'lastin_price',
        'unit_no': 'item_unit',
        'build_date': 'storage_time',
        'main_supcust': 'supplier_code',
        'sup_name': 'supplier_name',
        'item_brandname': 'brand_name',
        'item_subno': 'show_code'
    })
    goods['cmid'] = cmid
    goods['barcode'] = goods['foreign_item_id']
    goods['last_updated'] = datetime.now(_TZINFO)
    goods['isvalid'] = '1'
    goods['foreign_category_lv4'] = ''
    goods['foreign_category_lv5'] = ''
    goods['allot_method'] = ''
    goods['warranty'] = None
    def item_status_convert(status):
        if status == '1':
            return '正常'
        else:
            return ''
    goods['item_status'] = goods['status'].map(item_status_convert)
    goods = goods[[
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
    ]]
    return upload_to_s3(goods, source_id, date, target_table)


# 商品销售表
'''
     "origin_table_columns": {

                "t_bd_branch_info": [
                    "branch_name",
                    "branch_no"
                ],
                "t_bd_item_cls": [
                    "cls_parent",
                    "item_clsno",
                    "item_clsname"
                ],
                "t_bd_item_info": [
                    "item_no",
                    "unit_no",
                    "item_clsno",
                    'item_name'
                ],
                "t_rm_saleflow": [
                    "branch_no",
                    "flow_no",
                    "oper_date",
                    "item_no",
                    "sale_price",
                    "sale_qnty",
                    "sale_money",
                    "sell_way"
                ]
            }

        ,
        "converts": {
            "t_bd_branch_info": {
                "branch_no": "str"
            },
            "t_bd_item_cls": {
                "cls_parent": "str",
                "item_clsno": "str"
            },
            "t_bd_item_info": {
                "item_clsno": "str",
                "item_no": "str"
            },
            "t_rm_saleflow": {
                "branch_no": "str",
                "flow_no": "str",
                "item_no": "str"
            }
        }

'''


def clean_goodsflow(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    sale = data_frames['t_rm_saleflow']
    store = data_frames['t_bd_branch_info']
    goods = data_frames['t_bd_item_info']
    lv = data_frames['t_bd_item_cls']
    lv = lv[['item_clsno', 'item_clsname', 'cls_parent']]
    lv1 = lv[lv['cls_parent'].str.len() == 0]
    lv2 = lv[(lv['cls_parent'].str.len() == 1) | (lv['cls_parent'].str.len() == 2)]
    lv3 = lv[lv['cls_parent'].str.len() == 4]
    goods['lv_1'] = goods['item_clsno'].apply(
        lambda x: x[:2] if (len(x) == 6 or len(x) == 4)
        else x[:1] if len(x) == 3
        else x)
    goods['lv_2'] = goods['item_clsno'].apply(
        lambda x: x[:4] if (len(x) == 6 or len(x) == 4)
        else x[:3] if len(x) == 3
        else '')
    goods['lv_3'] = goods['item_clsno'].apply(
        lambda x: x if len(x) == 6
        else '')
    goods = goods.merge(
        lv1, how='left', left_on='lv_1', right_on='item_clsno', suffixes=['', '_lv_1']).merge(
        lv2, how='left', left_on='lv_2', right_on='item_clsno', suffixes=['', '_lv_2']).merge(
        lv3, how='left', left_on='lv_3', right_on='item_clsno', suffixes=['', '_lv_3'])
    goods = goods[['item_no', 'item_name', 'item_clsno', 'unit_no', 'lv_1',
                   'item_clsname', 'lv_2', 'item_clsname_lv_2', 'lv_3',
                   'item_clsname_lv_3']]
    sale['branch_no'] = sale['branch_no'].str.slice(0, 4)
    result_frame = sale.merge(
        store, how='left', on='branch_no').merge(
        goods, how='left', on='item_no')

    def saleprice_convert(row):
        if row['sell_way'] == 'C':
            return 0
        else:
            return row['sale_price']

    def quantity_convert(row):
        if row['sell_way'] == 'A' or row['sell_way'] == 'C':
            return row['sale_qnty']
        else:
            return -1 * row['sale_qnty']

    def subtotal(row):
        if row['sell_way'] == 'A':
            return row['sale_money']
        elif row['sell_way'] == 'B':
            return -1 * row['sale_money']
        elif row['sell_way'] == 'C':
            return 0
        else:
            pass

    result_frame['saleprice'] = result_frame.apply(saleprice_convert, axis=1)
    result_frame['quantity'] = result_frame.apply(quantity_convert, axis=1)
    result_frame['subtotal'] = result_frame.apply(subtotal, axis=1)
    result_frame = result_frame.rename(columns={
        'branch_no': 'foreign_store_id',
        'branch_name': 'store_name',
        'flow_no': 'receipt_id',
        'oper_date': 'saletime',
        'item_no': 'foreign_item_id',
        'item_name': 'item_name',
        'unit_no': 'item_unit',
        'lv_1': 'foreign_category_lv1',
        'item_clsname': 'foreign_category_lv1_name',
        'lv_2': 'foreign_category_lv2',
        'item_clsname_lv_2': 'foreign_category_lv2_name',
        'lv_3': 'foreign_category_lv3',
        'item_clsname_lv_3': 'foreign_category_lv3_name'
    })
    result_frame['source_id'] = source_id
    result_frame['cmid'] = cmid
    result_frame['consumer_id'] = None
    result_frame['last_updated'] = datetime.now(_TZINFO)
    result_frame['barcode'] = result_frame['foreign_item_id']
    result_frame['foreign_category_lv4'] = ''
    result_frame['foreign_category_lv4_name'] = None
    result_frame['foreign_category_lv5'] = ''
    result_frame['foreign_category_lv5_name'] = None
    result_frame['pos_id'] = ''

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
    return upload_to_s3(result_frame, source_id, date, target_table)


# 成本表
'''
   "origin_table_columns": {
            "t_bd_item_info": [
                "item_no",
                "item_clsno"
            ],
            "t_da_jxc_daysum": [
                "item_no",
                "branch_no",
                "oper_date",
                "pos_qty",
                "pos_qty",
                "avg_cost_amt"
            ]
        }

        ,
        "converts": {
            "t_bd_item_info": {
                "item_clsno": "str",
                "item_no": "str"
            },
            "t_da_jxc_daysum": {
                "branch_no": "str",
                "item_no": "str"
            }
        }
'''


def clean_cost(source_id, date, target_table, data_frames):
    columns = [
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
    ]

    cmid = source_id.split("Y")[0]
    cost = data_frames['t_da_jxc_daysum']
    cost['total_quantity'] = cost['pos_qty'] - cost['pos_ret_qty']
    cost['total_sale'] = cost['pos_amt'] - cost['pos_ret_amt']
    if not len(cost):
        return upload_to_s3(pd.DataFrame(columns=columns), source_id, date, target_table)

    goods = data_frames['t_bd_item_info']
    goods['foreign_category_lv1'] = goods['item_clsno'].apply(
        lambda x: x[:1] if (len(x) == 1 or len(x) == 3) else x[:2])
    goods['foreign_category_lv2'] = goods['item_clsno'].apply(
        lambda x: '' if (len(x) == 1 or len(x) == 2)
        else x if (len(x) == 3 or len(x) == 4)
        else x[:4])
    goods['foreign_category_lv3'] = goods['item_clsno'].apply(
        lambda x: x if len(x) == 6 else '')
    result_frame = pd.merge(cost, goods, on='item_no', how='left', suffixes=('_cost', '_goods'))
    result_frame['foreign_category_lv4'] = ''
    result_frame['foreign_category_lv5'] = ''
    result_frame['source_id'] = source_id
    result_frame['cmid'] = cmid
    result_frame['cost_type'] = ''
    result_frame = result_frame.rename(columns={
        'branch_no': 'foreign_store_id',
        'item_no': 'foreign_item_id',
        'oper_date': 'date',
        'avg_cost_amt': 'total_cost'

    })
    result_frame = result_frame[columns]

    return upload_to_s3(result_frame, source_id, date, target_table)


def upload_to_s3(frame, source_id, date, target_table):
    filename = tempfile.NamedTemporaryFile(mode="w", encoding="utf-8")
    count = len(frame)
    frame.to_csv(filename.name, index=False, compression="gzip", float_format='%.4f')
    filename.seek(0)
    key = CLEANED_PATH.format(
        source_id=source_id,
        target_table=target_table,
        date=date,
        timestamp=now_timestamp(),
        rowcount=count,
    )
    S3.Bucket(S3_BUCKET).upload_file(filename.name, key)

    return key


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time(), tz=_TZINFO)
    return _timestamp
