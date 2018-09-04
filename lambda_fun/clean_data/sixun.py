# -*- coding: utf-8 -*-
# @Time    : 2018/8/15 下午2:13
# @Author  : 范佳楠

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

category_dict = {
    '69YYYYYYYYYYYYY': (1, 2, 3),
    '72YYYYYYYYYYYYY': (2, 4, 6)
}

branch_dict = {
    '69YYYYYYYYYYYYY': 2,
    '72YYYYYYYYYYYYY': 2
}


def clean_sixun(source_id, date, target_table, data_frames):
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
    else:
        pass


# 门店表
'''
    'origin_table_columns': {
            "t_bd_branch_info": ['branch_no',
                                 'branch_name',
                                 'address',
                                 'dj_yw',
                                 'init_date',
                                 'branch_no',
                                 'branch_tel',
                                 'branch_fax',
                                 'other1',
                                 'trade_type',
                                 'property',
                                 ]
        },

        'converts': {"t_bd_branch_info": {'branch_no': 'str', 'property': 'int', 'trade_type': 'int', 'dj_yw': 'int'}}
'''


def clean_store(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    branch_info_frame = data_frames['t_bd_branch_info']
    branch_info_frame['dj_yw'] = branch_info_frame['dj_yw'].map(lambda value: '已冻结' if value == 1 else '未冻结')
    branch_info_frame = (branch_info_frame[(branch_info_frame['property'] == 0)])

    def trade_type_convert(value):
        if value == 2:
            return '加盟店'
        elif value == 0:
            return '总部托管'
        else:
            return '独立管理'

    branch_info_frame['property'] = branch_info_frame['trade_type'].map(trade_type_convert)
    branch_info_frame['cmid'] = cmid
    branch_info_frame['source_id'] = source_id
    branch_info_frame['address_code'] = ''
    branch_info_frame['device_id'] = ''
    branch_info_frame['lat'] = None
    branch_info_frame['lng'] = None
    branch_info_frame['area_code'] = None
    branch_info_frame['area_name'] = None
    branch_info_frame = branch_info_frame.rename(columns={'branch_no': 'foreign_store_id',
                                                          'branch_name': 'store_name',
                                                          'address': 'store_address',
                                                          'dj_yw': 'store_status',
                                                          'init_date': 'create_date',
                                                          'branch_tel': 'phone_number',
                                                          'branch_fax': 'contacts',
                                                          'other1': 'business_area',
                                                          'trade_type': 'property_id'
                                                          })

    branch_info_frame['show_code'] = branch_info_frame['foreign_store_id']
    branch_info_frame['last_updated'] = datetime.now()

    branch_info_frame = branch_info_frame[[
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
    return upload_to_s3(branch_info_frame, source_id, date, target_table)


# 商品分类

'''
    'origin_table_columns': {
            "t_bd_item_cls": ['item_clsno',
                              'item_clsname',
                              'cls_parent',
                              ]
        },

    'converts': {"t_bd_item_cls": {'item_clsno': 'str', 'item_clsname': 'str', 'cls_parent': 'str'}}
'''


def clean_category(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    lv1_len, lv2_len, lv3_len = category_dict[source_id]
    item_cls_frame_1 = data_frames['t_bd_item_cls']
    # item_cls_frame_1 = pd.read_csv(filepath_or_buffer=item_cls_path)
    item_cls_frame_2 = item_cls_frame_1.copy(deep=True)
    item_cls_frame_3 = item_cls_frame_1.copy(deep=True)
    # 处理item_cls_frame1
    item_cls_frame_1 = item_cls_frame_1[(item_cls_frame_1['item_clsno'].str.strip()).str.len() == lv1_len]
    item_cls_frame_1 = item_cls_frame_1[['item_clsno', 'item_clsname']]
    item_cls_frame_1 = item_cls_frame_1.rename(columns={'item_clsno': 'foreign_category_lv1',
                                                        'item_clsname': 'foreign_category_lv1_name'})
    item_cls_frame_1['cmid'] = cmid
    item_cls_frame_1['level'] = 1
    item_cls_frame_1['last_updated'] = datetime.now()
    item_cls_frame_1['foreign_category_lv2'] = ''
    item_cls_frame_1['foreign_category_lv2_name'] = None
    item_cls_frame_1['foreign_category_lv3'] = ''
    item_cls_frame_1['foreign_category_lv3_name'] = None
    item_cls_frame_1['foreign_category_lv4'] = ''
    item_cls_frame_1['foreign_category_lv4_name'] = None
    item_cls_frame_1['foreign_category_lv5'] = ''
    item_cls_frame_1['foreign_category_lv5_name'] = None
    item_cls_frame_1['last_updated'] = datetime.now()

    # 处理item_cls_frame2
    item_cls_frame_2['cls_parent'] = item_cls_frame_2['cls_parent'].str.strip()
    item_cls_frame_2['item_clsno'] = item_cls_frame_2['item_clsno'].str.strip()
    item_cls_frame_2 = pd.merge(item_cls_frame_2, item_cls_frame_2, how='left', left_on='item_clsno',
                                right_on='cls_parent',
                                suffixes=('_lv1', '_lv2'))
    item_cls_frame_2 = item_cls_frame_2[(item_cls_frame_2['item_clsno_lv2'].str.strip()).str.len() == lv2_len]

    item_cls_frame_2 = item_cls_frame_2[['item_clsno_lv1', 'item_clsname_lv1', 'item_clsno_lv2', 'item_clsname_lv2']]
    item_cls_frame_2 = item_cls_frame_2.rename(columns={'item_clsno_lv1': 'foreign_category_lv1',
                                                        'item_clsname_lv1': 'foreign_category_lv1_name',
                                                        'item_clsno_lv2': 'foreign_category_lv2',
                                                        'item_clsname_lv2': 'foreign_category_lv2_name'})
    item_cls_frame_2['level'] = 2
    item_cls_frame_2['foreign_category_lv3'] = ''
    item_cls_frame_2['foreign_category_lv3_name'] = None
    item_cls_frame_2['foreign_category_lv4'] = ''
    item_cls_frame_2['foreign_category_lv4_name'] = None
    item_cls_frame_2['foreign_category_lv5'] = ''
    item_cls_frame_2['foreign_category_lv5_name'] = None
    item_cls_frame_2['cmid'] = cmid
    item_cls_frame_2['last_updated'] = datetime.now()

    # 处理item_cls_frame3

    item_cls_frame_3['cls_parent'] = item_cls_frame_3['cls_parent'].str.strip()
    item_cls_frame_3['item_clsno'] = item_cls_frame_3['item_clsno'].str.strip()

    item_cls_frame_3 = pd.merge(item_cls_frame_3, item_cls_frame_3,
                                left_on='item_clsno',
                                right_on='cls_parent',
                                suffixes=('_lv1', '_lv2'),
                                how='left').merge(item_cls_frame_3, how='left', left_on='item_clsno_lv2',
                                                  right_on='cls_parent')

    item_cls_frame_3 = item_cls_frame_3[(item_cls_frame_3['item_clsno'].str.strip()).str.len() == lv3_len]
    item_cls_frame_3 = item_cls_frame_3[
        ['item_clsno_lv1', 'item_clsname_lv1', 'item_clsno_lv2', 'item_clsname_lv2', 'item_clsno', 'item_clsname']]
    item_cls_frame_3 = item_cls_frame_3.rename(columns={'item_clsno_lv1': 'foreign_category_lv1',
                                                        'item_clsname_lv1': 'foreign_category_lv1_name',
                                                        'item_clsno_lv2': 'foreign_category_lv2',
                                                        'item_clsname_lv2': 'foreign_category_lv2_name',
                                                        'item_clsno': 'foreign_category_lv3',
                                                        'item_clsname': 'foreign_category_lv3_name'})
    item_cls_frame_3['foreign_category_lv4'] = ''
    item_cls_frame_3['foreign_category_lv4_name'] = None
    item_cls_frame_3['foreign_category_lv5'] = ''
    item_cls_frame_3['foreign_category_lv5_name'] = None
    item_cls_frame_3['level'] = 3
    item_cls_frame_3['cmid'] = cmid
    item_cls_frame_3['last_updated'] = datetime.now()

    category_frame = pd.concat([item_cls_frame_1, item_cls_frame_2, item_cls_frame_3])
    category_frame = category_frame[[
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
    ]]
    return upload_to_s3(category_frame, source_id, date, target_table)


# 商品

'''
     'origin_table_columns': {
            't_bd_item_info': ['item_clsno',
                               'main_supcust',
                               'status',
                               'num2',
                               'item_no',
                               'item_name',
                               'price',
                               'sale_price',
                               'unit_no',
                               'item_subno',
                               'item_brandname',
                               'build_date'
                               ],
            't_bd_supcust_info': ['supcust_no', 'sup_name', 'supcust_flag'],
            "t_bd_item_cls": ['item_clsno']

        },
        'converts': {
            "t_bd_item_cls": {'item_clsno': 'str'},
            't_bd_item_info': {'item_clsno': 'str', 'num2': 'float',
                               'main_supcust': 'str',
                               'status': 'int',
                               'item_no': 'str',

                               },
            't_bd_supcust_info': {'supcust_no': 'str'}

'''


def clean_goods(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    lv1_len, lv2_len, lv3_len = category_dict[source_id]
    goods_frame = data_frames['t_bd_item_info']
    item_cls_frame = data_frames['t_bd_item_cls']
    supcust_info_frame = data_frames['t_bd_supcust_info']

    supcust_info_frame['supcust_no'] = supcust_info_frame['supcust_no'].str.strip()
    item_cls_frame['item_clsno'] = item_cls_frame['item_clsno'].str.strip()

    goods_frame_1 = goods_frame.copy(deep=True)
    goods_frame_1['item_clsno_1'] = goods_frame['item_clsno'].str.strip().str.slice(0, lv1_len)
    goods_frame_1['item_clsno_2'] = goods_frame['item_clsno'].str.strip().str.slice(0, lv2_len)
    goods_frame_1['item_clsno_3'] = goods_frame['item_clsno'].str.strip().str.slice(0, lv3_len)
    goods_frame_1['main_supcust'] = goods_frame_1['main_supcust'].str.strip()

    goods_frame_1 = pd.merge(goods_frame_1, item_cls_frame, left_on='item_clsno_1', right_on='item_clsno', how='left',
                             suffixes=('_goods', '_lv1'))

    lv2 = item_cls_frame.copy(deep=True)
    lv2 = lv2[lv2['item_clsno'].str.len() == lv2_len]

    goods_frame_1 = pd.merge(goods_frame_1, lv2, left_on='item_clsno_2', right_on='item_clsno', how='left',
                             suffixes=('_2', '_lv2'))

    lv3 = item_cls_frame.copy(deep=True)
    lv3 = lv3[lv3['item_clsno'].str.len() == lv3_len]

    goods_frame_1 = pd.merge(goods_frame_1, lv3, how='left', left_on='item_clsno_3', right_on='item_clsno',
                             suffixes=('_end', '_lv3'))

    goods_frame_1 = pd.merge(goods_frame_1, supcust_info_frame, left_on='main_supcust', right_on='supcust_no')
    goods_frame_1 = goods_frame_1[goods_frame_1['supcust_flag'] == 'S']

    # 映射字段
    goods_frame_1 = goods_frame_1.rename(columns={
        'item_no': 'barcode',
        'item_name': 'item_name',
        'price': 'lastin_price',
        'sale_price': 'sale_price',
        'unit_no': 'item_unit',
        'status': 'item_status',
        'item_clsno_lv1': 'foreign_category_lv1',
        'item_clsno_end': 'foreign_category_lv2',
        'item_clsno_lv3': 'foreign_category_lv3',
        'build_date': 'storage_time',
        'num2': 'warranty',
        'item_subno': 'show_code',
        'sup_name': 'supplier_name',
        'supcust_no': 'supplier_code',
        'item_brandname': 'brand_name'
    })

    def status_convert(status):
        if status == 0:
            return '建档'
        elif status == 1:
            return '新品'
        elif status == 2:
            return '正常'
        elif status == 3 or status == 5:
            return '停购'
        elif status == 4:
            return '停售'
        else:
            return '其他'

    goods_frame_1['item_status'] = goods_frame_1['item_status'].map(status_convert)
    goods_frame_1['cmid'] = cmid
    goods_frame_1['last_updated'] = datetime.now()
    goods_frame_1['isvalid'] = '1'
    goods_frame_1['foreign_category_lv4'] = ''
    goods_frame_1['foreign_category_lv5'] = ''
    goods_frame_1['allot_method'] = ''
    goods_frame_1['foreign_item_id'] = goods_frame_1['barcode']

    goods_frame_1['warranty'] = goods_frame_1['warranty'].round(decimals=1)

    goods_frame_1 = goods_frame_1[[
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

    return upload_to_s3(goods_frame_1, source_id, date, target_table)


# 商品销售表
'''
     'origin_table_columns': {
            't_rm_saleflow': ['branch_no',
                              'item_no',
                              'sale_price',
                              'sale_qnty',
                              'sell_way',
                              'sale_money',
                              'flow_no',
                              'oper_date'],
            't_bd_branch_info': ['branch_no', 'branch_name'],
            't_bd_item_info': ['item_no', 'item_clsno', 'item_name', 'unit_no'],
            't_bd_item_cls': ['item_clsno', 'item_clsname'],
        },

        'converts': {
            't_rm_saleflow': {'branch_no': 'str',
                              'item_no': 'str',
                              'sale_price': 'float',
                              'sale_qnty': 'float',
                              'sell_way': 'str',
                              'sale_money': 'float',
                              'flow_no': 'str'
                              },

            't_bd_branch_info': {'branch_no': 'str'},
            't_bd_item_info': {'item_no': 'str', 'item_clsno': 'str'},
            't_bd_item_cls': {'item_clsno': 'str'},
        }

'''


def clean_goodsflow(source_id, date, target_table, data_frames):
    lv1_len, lv2_len, lv3_len = category_dict[source_id]
    cmid = source_id.split("Y")[0]
    branch_no_len = branch_dict[source_id]
    sale_flow_frame = data_frames['t_rm_saleflow']
    branch_info_frame = data_frames['t_bd_branch_info']
    goods_frame = data_frames['t_bd_item_info']
    item_cls_frame = data_frames['t_bd_item_cls']

    # 分类
    item_cls_frame['item_clsno'] = item_cls_frame['item_clsno'].str.strip()

    # 商品
    goods_frame['item_clsno'] = goods_frame['item_clsno'].str.strip()
    goods_frame['item_clsno_1'] = goods_frame['item_clsno'].str.slice(0, lv1_len)
    goods_frame['item_clsno_2'] = goods_frame['item_clsno'].str.slice(0, lv2_len)
    goods_frame['item_clsno_3'] = goods_frame['item_clsno'].str.slice(0, lv3_len)
    goods_frame['item_no'] = goods_frame['item_no'].str.strip()

    # 销售
    sale_flow_frame['branch_no_1'] = sale_flow_frame['branch_no'].str.strip().str.slice(0, branch_no_len)
    sale_flow_frame['item_no'] = sale_flow_frame['item_no'].str.strip()
    sale_flow_frame['unit_no'] = sale_flow_frame['unit_no'].str.strip()

    # 门店
    branch_info_frame['branch_no'] = branch_info_frame['branch_no'].str.strip()

    result_frame = pd.merge(sale_flow_frame, branch_info_frame, left_on='branch_no_1', right_on='branch_no', how='left',
                            suffixes=('_s1', '_store'))

    # item_clsno
    result_frame = pd.merge(result_frame, goods_frame, left_on='item_no', right_on='item_no', how='left',
                            suffixes=('_s2', '_goods'))
    '''
        lv1: item_cls_lv1 item_clsname
    '''

    result_frame = pd.merge(result_frame, item_cls_frame, left_on='item_clsno_1', right_on='item_clsno', how='left',
                            suffixes=('_s3', '_lv1'))

    lv2 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == lv2_len]

    '''
        lv1 item_cls_lv1 item_clsname_s4
        lv2 item_clsno item_clsname_lv2
    '''
    result_frame = pd.merge(result_frame, lv2, left_on='item_clsno_2', right_on='item_clsno', how='left',
                            suffixes=('_s4', '_lv2'))

    lv3 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == lv3_len]

    '''
        lv2 item_clsno_s5  item_clsname_lv2
        lv3 item_clsno_lv3 item_clsname
    '''
    result_frame = pd.merge(result_frame, lv3, left_on='item_clsno_3', right_on='item_clsno', how='left',
                            suffixes=('_s5', '_lv3'))

    result_frame = result_frame[(result_frame['item_no'].notna()) & (result_frame['branch_no_store'].notna())]

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
    result_frame['item_clsno_s5'] = result_frame['item_clsno_s5'].map(lambda x: x if x else '')
    result_frame['item_clsno_lv3'] = result_frame['item_clsno_lv3'].map(lambda x: x if x else '')

    result_frame = result_frame.rename(columns={
        'branch_no_store': 'foreign_store_id',
        'branch_name': 'store_name',
        'flow_no': 'receipt_id',
        'oper_date': 'saletime',
        'item_no': 'foreign_item_id',
        'item_name': 'item_name',
        'unit_no': 'item_unit',
        'item_clsno_lv1': 'foreign_category_lv1',
        'item_clsname_s4': 'foreign_category_lv1_name',
        'item_clsno_s5': 'foreign_category_lv2',
        'item_clsname_lv2': 'foreign_category_lv2_name',
        'item_clsno_lv3': 'foreign_category_lv3',
        'item_clsname': 'foreign_category_lv3_name',
    })

    result_frame['source_id'] = source_id
    result_frame['cmid'] = cmid
    result_frame['consumer_id'] = None
    result_frame['last_updated'] = datetime.now()
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
    'origin_table_columns': {
            "t_bd_item_cls": ['item_clsno', ],
            't_da_jxc_daysum': [
                'branch_no',
                'oper_date',
                'so_qty',
                'pos_qty',
                'so_amt',
                'pos_amt',
                'fifo_cost_amt',
                'item_no'
            ],
            't_bd_item_info': ['item_no', 'item_clsno']
        },

        'converts': {
            "t_bd_item_cls": {'item_clsno': 'str'},
            't_bd_item_info': {'item_no': 'str', 'item_clsno': 'str'},
            't_da_jxc_daysum': {
                'branch_no': 'str',
                'so_qty': 'float',
                'pos_qty': 'float',
                'so_amt': 'float',
                'pos_amt': 'float',
                'fifo_cost_amt': 'float',
                'item_no': 'str'
            }
        }
'''


def clean_cost(source_id, date, target_table, data_frames):
    cmid = source_id.split("Y")[0]
    cost_frame = data_frames['t_da_jxc_daysum']
    goods_frame = data_frames['t_bd_item_info']
    item_cls_frame = data_frames['t_bd_item_cls']

    lv1_len, lv2_len, lv3_len = category_dict[source_id]
    branch_no_len = branch_dict[source_id]
    goods_frame['item_clsno'] = goods_frame['item_clsno'].str.strip()
    goods_frame['item_clsno_1'] = goods_frame['item_clsno'].str.slice(0, lv1_len)
    goods_frame['item_clsno_2'] = goods_frame['item_clsno'].str.slice(0, lv2_len)
    goods_frame['item_clsno_3'] = goods_frame['item_clsno'].str.slice(0, lv3_len)

    goods_frame['item_no'] = goods_frame['item_no'].str.strip()
    cost_frame['item_no'] = cost_frame['item_no'].str.strip()

    item_cls_frame['item_clsno'] = item_cls_frame['item_clsno'].str.strip()

    result_frame = pd.merge(cost_frame, goods_frame, left_on='item_no', right_on='item_no', how='left',
                            suffixes=('_cost', '_goods'))
    # s3 -> c1
    '''
        lv1 item_clsno_lv1 item_clsname
    '''

    result_frame = pd.merge(result_frame, item_cls_frame, left_on='item_clsno_1', right_on='item_clsno', how='left',
                            suffixes=('_c1', '_lv1'))

    lv2 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == lv2_len]

    '''
        lv1 item_clsno_lv1 item_clsname_c2
        lv2 item_clsno item_clsname_lv2
    '''
    # s4 - c2
    result_frame = pd.merge(result_frame, lv2, left_on='item_clsno_2', right_on='item_clsno', how='left',
                            suffixes=('_c2', '_lv2'))

    lv3 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == lv3_len]

    '''
        lv1 item_clsno_lv1 item_clsname_c2
        lv2 item_clsno_c3 item_clsname_lv2
        lv3 item_clsno_lv3 item_clsname
    '''
    # s5 - c3
    result_frame = pd.merge(result_frame, lv3, left_on='item_clsno_3', right_on='item_clsno', how='left',
                            suffixes=('_c3', '_lv3'))

    result_frame = result_frame[(result_frame['so_qty'] != 0)
                                | (result_frame['pos_qty'] != 0)
                                | (result_frame['so_amt'] != 0)
                                | (result_frame['pos_amt'] != 0)
                                ]
    result_frame['branch_no'] = result_frame['branch_no'].str.slice(0, branch_no_len)

    branch_no_zero = '0' * branch_no_len
    result_frame = result_frame[(result_frame['branch_no'] != branch_no_zero) & (result_frame['item_no'].notna())]

    result_frame['item_clsno_c3'] = result_frame['item_clsno_c3'].map(lambda x: x if x else '')
    result_frame['item_clsno_lv3'] = result_frame['item_clsno_lv3'].map(lambda x: x if x else '')

    result_frame = result_frame.rename(columns={
        'branch_no': 'foreign_store_id',
        'item_no': 'foreign_item_id',
        'fifo_cost_amt': 'total_cost',
        'item_clsno_lv1': 'foreign_category_lv1',
        'item_clsno_c3': 'foreign_category_lv2',
        'item_clsno_lv3': 'foreign_category_lv3',
    })

    result_frame['date'] = pd.to_datetime(result_frame['oper_date'], format='%Y-%m-%d')
    result_frame['cost_type'] = ''

    def total_qualtity_convert(row):
        return row['so_qty'] + row['pos_qty']

    def total_sale_convert(row):
        return row['so_amt'] + row['pos_amt']

    result_frame['total_quantity'] = result_frame.apply(total_qualtity_convert, axis=1)
    result_frame['total_sale'] = result_frame.apply(total_sale_convert, axis=1)

    result_frame['foreign_category_lv4'] = ''
    result_frame['foreign_category_lv5'] = ''
    result_frame['source_id'] = source_id
    result_frame['cmid'] = cmid

    result_frame = result_frame[[
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
    ]]
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
