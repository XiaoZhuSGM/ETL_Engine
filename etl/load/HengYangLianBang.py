# -*- coding: utf-8 -*-
# @Time    : 2018/8/15 下午2:13
# @Author  : 范佳楠

import ssl
from datetime import datetime

import boto3
import pandas as pd
from sqlalchemy import create_engine

ssl._create_default_https_context = ssl._create_unverified_context

boto_client = boto3.client('s3')
engine = create_engine('postgresql+psycopg2://beanan:root@123.206.60.59:5432/test')
branch_info_path = 'https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/datapipeline/source_id%3D72YYYYYYYYYYYYY/ext_date%3D2018-08-13/table%3Dt_bd_branch_info/dump%3D2018-08-16+16%3A11%3A03.327773%2B08%3A00%26rowcount%3D24.csv.gz'
item_cls_path = 'https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/datapipeline/source_id%3D72YYYYYYYYYYYYY/ext_date%3D2018-08-13/table%3Dt_bd_item_cls/dump%3D2018-08-16+16%3A11%3A04.803990%2B08%3A00%26rowcount%3D336.csv.gz'
item_info_path = 'https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/datapipeline/source_id%3D72YYYYYYYYYYYYY/ext_date%3D2018-08-13/table%3Dt_bd_item_info/dump%3D2018-08-16+16%3A11%3A22.781142%2B08%3A00%26rowcount%3D29118.csv.gz'
sale_flow = 'https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/datapipeline/source_id%3D72YYYYYYYYYYYYY/ext_date%3D2018-08-13/table%3Dt_rm_saleflow/dump%3D2018-08-16+16%3A11%3A08.865099%2B08%3A00%26rowcount%3D7992.csv.gz'
cost_path = 'https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/datapipeline/source_id%3D72YYYYYYYYYYYYY/ext_date%3D2018-08-13/table%3Dt_da_jxc_daysum/dump%3D2018-08-16+16%3A11%3A08.502242%2B08%3A00%26rowcount%3D3745.csv.gz'


# 门店表
def store():
    branch_info_frame = pd.read_csv(filepath_or_buffer=branch_info_path)
    branch_info_frame['dj_yw'] = branch_info_frame['dj_yw'].map(lambda value: '已冻结' if value == '1' else '未冻结')
    branch_info_frame = (branch_info_frame[(branch_info_frame['property'] == 0)])

    def trade_type_convert(value):
        if value == '2':
            return '加盟店'
        elif value == '0':
            return '总部托管'
        else:
            return '独立管理'

    branch_info_frame['property'] = branch_info_frame['trade_type'].map(trade_type_convert)
    branch_info_frame['cmid'] = '72'
    branch_info_frame['source_id'] = '72YYYYYYYYYYYYY'
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

    branch_info_frame.to_csv(path_or_buf='store.csv', index=False)


# 商品分类
def category():
    item_cls_frame_1 = pd.read_csv(filepath_or_buffer=item_cls_path)
    item_cls_frame_2 = item_cls_frame_1.copy(deep=True)
    item_cls_frame_3 = item_cls_frame_1.copy(deep=True)
    # 处理item_cls_frame1
    item_cls_frame_1 = item_cls_frame_1[(item_cls_frame_1['item_clsno'].str.strip()).str.len() == 2]
    item_cls_frame_1 = item_cls_frame_1[['item_clsno', 'item_clsname']]
    item_cls_frame_1 = item_cls_frame_1.rename(columns={'item_clsno': 'foreign_category_lv1',
                                                        'item_clsname': 'foreign_category_lv1_name'})
    item_cls_frame_1['cmid'] = '72'
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

    # 处理item_cls_frame2
    item_cls_frame_2['cls_parent'] = item_cls_frame_2['cls_parent'].str.strip()
    item_cls_frame_2['item_clsno'] = item_cls_frame_2['item_clsno'].str.strip()
    item_cls_frame_2 = pd.merge(item_cls_frame_2, item_cls_frame_2, how='left', left_on='item_clsno',
                                right_on='cls_parent',
                                suffixes=('_lv1', '_lv2'))
    item_cls_frame_2 = item_cls_frame_2[(item_cls_frame_2['item_clsno_lv2'].str.strip()).str.len() == 4]

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
    item_cls_frame_2['cmid'] = 72
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

    item_cls_frame_3 = item_cls_frame_3[(item_cls_frame_3['item_clsno'].str.strip()).str.len() == 6]
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
    item_cls_frame_3['cmid'] = 72
    item_cls_frame_3['last_updated'] = datetime.now()

    category_frame = pd.concat([item_cls_frame_1, item_cls_frame_2, item_cls_frame_3])
    category_frame = category_frame[[
        'cmid',
        'level',
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
        'last_updated'
    ]]
    category_frame.to_csv(path_or_buf='category.csv', index=False)


# 商品
def goods():
    goods_frame = pd.read_csv(filepath_or_buffer=item_info_path, dtype={'item_clsno': str})
    item_cls_frame = pd.read_csv(filepath_or_buffer=item_cls_path, dtype={'item_clsno': str})

    item_cls_frame['item_clsno'] = item_cls_frame['item_clsno'].str.strip()

    goods_frame_1 = goods_frame.copy(deep=True)
    goods_frame_1['item_clsno_1'] = goods_frame['item_clsno'].str.strip().str.slice(0, 2)
    goods_frame_1['item_clsno_2'] = goods_frame['item_clsno'].str.strip().str.slice(0, 4)
    goods_frame_1['item_clsno_3'] = goods_frame['item_clsno'].str.strip().str.slice(0, 6)

    goods_frame_1 = pd.merge(goods_frame_1, item_cls_frame, left_on='item_clsno_1', right_on='item_clsno', how='left',
                             suffixes=('_goods', '_lv1'))

    lv2 = item_cls_frame.copy(deep=True)
    lv2 = lv2[lv2['item_clsno'].str.len() == 4]

    goods_frame_1 = pd.merge(goods_frame_1, lv2, left_on='item_clsno_2', right_on='item_clsno', how='left',
                             suffixes=('_2', '_lv2'))

    lv3 = item_cls_frame.copy(deep=True)
    lv3 = lv3[lv3['item_clsno'].str.len() == 6]

    goods_frame_1 = pd.merge(goods_frame_1, lv3, how='left', left_on='item_clsno_3', right_on='item_clsno',
                             suffixes=('_end', '_lv3'))

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
    })

    def status_convert(status):
        if status == '0':
            return '建档'
        elif status == '1':
            return '新品'
        elif status == '2':
            return '正常'
        elif status == '3' or status == '5':
            return '停购'
        elif status == '4':
            return '停售'
        else:
            return '其他'

    goods_frame_1['item_status'] = goods_frame_1['item_status'].map(status_convert)
    goods_frame_1['cmid'] = '72'
    goods_frame_1['last_updated'] = datetime.now()
    goods_frame_1['isvalid'] = '1'
    goods_frame_1['foreign_category_lv4'] = ''
    goods_frame_1['foreign_category_lv5'] = ''
    goods_frame_1['allot_method'] = ''
    goods_frame_1['supplier_name'] = ''
    goods_frame_1['supplier_code'] = ''
    goods_frame_1['brand_name'] = ''
    goods_frame_1['foreign_item_id'] = goods_frame_1['barcode']

    goods_frame_1 = goods_frame_1[[
        'cmid',
        'barcode',
        'foreign_item_id',
        'item_name',
        'lastin_price',
        'sale_price',
        'item_unit',
        'item_status',
        'foreign_category_lv1',
        'foreign_category_lv2',
        'foreign_category_lv3',
        'foreign_category_lv4',
        'foreign_category_lv5',
        'storage_time',
        'last_updated',
        'isvalid',
        'warranty',
        'show_code',
        'allot_method',
        'supplier_name',
        'supplier_code',
        'brand_name'
    ]]

    # print(goods_frame_1)
    goods_frame_1.to_csv(path_or_buf='goods.csv', index=False)


# 商品销售表
def goodsflow():
    sale_flow_frame = pd.read_csv(filepath_or_buffer=sale_flow, dtype={'branch_no': str,
                                                                       'item_no': str,
                                                                       'sell_price': float,
                                                                       'flow_no': str})

    # print(sale_flow_frame['flow_no'])
    branch_info_frame = pd.read_csv(filepath_or_buffer=branch_info_path, dtype={'branch_no': str})
    goods_frame = pd.read_csv(filepath_or_buffer=item_info_path, dtype={'item_no': str, 'item_clsno': str})
    item_cls_frame = pd.read_csv(filepath_or_buffer=item_cls_path, dtype={"item_clsno": str})

    # print(item_cls_frame['item'])
    # 分类
    item_cls_frame['item_clsno'] = item_cls_frame['item_clsno'].str.strip()

    # 商品
    goods_frame['item_clsno_1'] = goods_frame['item_clsno'].str.strip().str.slice(0, 2)
    goods_frame['item_clsno_2'] = goods_frame['item_clsno'].str.strip().str.slice(0, 4)
    goods_frame['item_clsno_3'] = goods_frame['item_clsno'].str.strip().str.slice(0, 6)
    goods_frame['item_no'] = goods_frame['item_no'].str.strip()

    # print(goods_frame['item_no'])
    # 销售
    sale_flow_frame['branch_no_1'] = sale_flow_frame['branch_no'].str.strip().str.slice(0, 2)
    sale_flow_frame['item_no'] = sale_flow_frame['item_no'].str.strip()

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
    print(result_frame['item_clsno_1'])
    result_frame = pd.merge(result_frame, item_cls_frame, left_on='item_clsno_1', right_on='item_clsno', how='left',
                            suffixes=('_s3', '_lv1'))

    lv2 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == 4]

    '''
        lv1 item_cls_lv1 item_clsname_s4
        lv2 item_clsno item_clsname_lv2
    '''
    result_frame = pd.merge(result_frame, lv2, left_on='item_clsno_2', right_on='item_clsno', how='left',
                            suffixes=('_s4', '_lv2'))

    lv3 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == 6]

    '''
        lv2 item_clsno_s5  item_clsname_lv2
        lv3 item_clsno_lv3 item_clsname
    '''
    result_frame = pd.merge(result_frame, lv3, left_on='item_clsno_3', right_on='item_clsno', how='left',
                            suffixes=('_s5', '_lv3'))

    # print(result_frame['item_no_goods'])

    result_frame = result_frame[(result_frame['item_no'].notna()) & (result_frame['branch_no_store'].notna())]

    def saleprice_convert(row):
        # print(row['sale_price'])
        if row['sell_way'] == 'C':
            return 0
        else:
            return row['sale_price_s2']

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

    result_frame['source_id'] = '72YYYYYYYYYYYYY'
    result_frame['cmid'] = '72'
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
    result_frame.to_csv(path_or_buf='goodsflow.csv', index=False)


# 成本表
def cost():
    cost_frame = pd.read_csv(filepath_or_buffer=cost_path, dtype={'item_no': 'str', 'branch_no': str})
    goods_frame = pd.read_csv(filepath_or_buffer=item_info_path, dtype={'item_no': str, 'item_clsno': str,
                                                                        'branch_no': str})
    goods_frame['item_clsno_1'] = goods_frame['item_clsno'].str.strip().str.slice(0, 2)
    goods_frame['item_clsno_2'] = goods_frame['item_clsno'].str.strip().str.slice(0, 4)
    goods_frame['item_clsno_3'] = goods_frame['item_clsno'].str.strip().str.slice(0, 6)

    goods_frame['item_no'] = goods_frame['item_no'].str.strip()
    cost_frame['item_no'] = cost_frame['item_no'].str.strip()

    item_cls_frame = pd.read_csv(filepath_or_buffer=item_cls_path, dtype={"item_clsno": str})
    item_cls_frame['item_clsno'] = item_cls_frame['item_clsno'].str.strip()

    result_frame = pd.merge(cost_frame, goods_frame, left_on='item_no', right_on='item_no', how='left',
                            suffixes=('_cost', '_goods'))
    # s3 -> c1
    '''
        lv1 item_clsno_lv1 item_clsname
    '''

    result_frame = pd.merge(result_frame, item_cls_frame, left_on='item_clsno_1', right_on='item_clsno', how='left',
                            suffixes=('_c1', '_lv1'))

    lv2 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == 4]

    '''
        lv1 item_clsno_lv1 item_clsname_c2
        lv2 item_clsno item_clsname_lv2
    '''
    # s4 - c2
    result_frame = pd.merge(result_frame, lv2, left_on='item_clsno_2', right_on='item_clsno', how='left',
                            suffixes=('_c2', '_lv2'))

    lv3 = item_cls_frame[item_cls_frame['item_clsno'].str.len() == 6]

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
    result_frame['branch_no_cost'] = result_frame['branch_no_cost'].str.slice(0, 2)
    result_frame = result_frame[(result_frame['branch_no_cost'] != '00') & (result_frame['item_no'].notna())]

    result_frame['item_clsno_c3'] = result_frame['item_clsno_c3'].map(lambda x: x if x else '')
    result_frame['item_clsno_lv3'] = result_frame['item_clsno_lv3'].map(lambda x: x if x else '')

    result_frame = result_frame.rename(columns={
        'branch_no_cost': 'foreign_store_id',
        'item_no': 'foreign_item_id',
        'fifo_cost_amt': 'total_cost',
        'item_clsno_lv1': 'foreign_category_lv1',
        'item_clsno_c3': 'foreign_category_lv2',
        'item_clsno_lv3': 'foreign_category_lv3',
    })

    # result_frame['date'] = result_frame['oper_date'].map()
    result_frame['date'] = pd.to_datetime(result_frame['oper_date'], format='%Y-%m-%d')
    # print(result_frame['oper_date'].dtype)
    result_frame['cost_type'] = ''

    def total_qualtity_convert(row):
        return row['so_qty'] + row['pos_qty']

    def total_sale_convert(row):
        return row['so_amt'] + row['pos_amt']

    result_frame['total_quantity'] = result_frame.apply(total_qualtity_convert, axis=1)
    result_frame['total_sale'] = result_frame.apply(total_sale_convert, axis=1)

    result_frame['foreign_category_lv4'] = ''
    result_frame['foreign_category_lv5'] = ''
    result_frame['source_id'] = '72YYYYYYYYYYYYY'
    result_frame['cmid'] = '72'

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
    print(result_frame['foreign_category_lv1'])
    result_frame.to_csv(path_or_buf='cost.csv', index=False)


# cost()
# goodsflow()

store()
category()
goods()
