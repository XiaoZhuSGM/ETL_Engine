# -*- coding: utf-8 -*-
# @Time    : 2018/8/31 11:18
# @Author  : 范佳楠


from datetime import datetime


import pandas as pd

from typing import Dict
from base import Base


class ShangHaiDaoHangCleaner(Base):

    def __init__(self, source_id: str, date, data: Dict[str, pd.DataFrame]) -> None:
        Base.__init__(self, source_id, date, data)



    """
    "origin_table_columns": {
            "item": ['orgcode', 'plucode', 'trantype', 'serialno', 'pluprice', 'pluqty', 'plutotal', 'accdate'],
            "subshop": ['orgcode','orgname',],
            "goods": ['plucode', 'clscode', 'barcode', 'pluname', 'unit'],
            "gclass": ['clscode', 'clslevel', 'clsname']
        },

        "converts": {
            "item": {
                'serialno':'str'
                'orgcode':'str',
                'plucode':'str',
                'trantype':'str',
            },
            "subshop": {
                'orgcode':'str',
            },
            "goods": {
                'plucode':'str',
                'clscode':'str',
                'barcode':'str'
            },
            "gclass": {
                'clscode':'str',
                'clslevel':'int'
            }
        }
    """

    def goodsflow(self):
        flow = self.data['item']
        store = self.data['subshop']
        goods = self.data['goods']
        gclass = self.data['gclass']

        goods['clscode_1'] = goods.apply(lambda row: row['clscode'][:2], axis=1)
        goods['clscode_2'] = goods.apply(lambda row: row['clscode'][:4], axis=1)
        goods['clscode_3'] = goods.apply(lambda row: row['clscode'][:6], axis=1)
        goods['clscode_4'] = goods.apply(lambda row: row['clscode'][:8], axis=1)
        goods['clscode_5'] = goods.apply(lambda row: row['clscode'][:10], axis=1)

        result = flow.merge(
            store,
            on='orgcode',
            how='left',
        ).merge(
            goods,
            on='plucode',
            how='left',
        ).merge(
            gclass,
            left_on='clscode_1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        ).merge(
            gclass,
            left_on='clscode_2',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv2')
        ).merge(
            gclass,
            left_on='clscode_3',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv3')
        ).merge(
            gclass[gclass['clslevel'] == 4],
            left_on='clscode_4',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv4')
        ).merge(
            gclass[gclass['clslevel'] == 5],
            left_on='clscode_5',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv5')
        )

        result = result[(result['trantype'] != 'a')
                        & (result['orgcode'].notna())
                        & (result['plucode'].notna())]

        result['foreign_category_lv1'] = result.apply(lambda row: row['clscode_lv1'] if row['clscode_lv2'] else '',
                                                      axis=1)
        result['foreign_category_lv2'] = result.apply(lambda row: row['clscode_lv2'] if row['clscode_lv2'] else '',
                                                      axis=1)

        result['foreign_category_lv3'] = result.apply(lambda row: row['clscode_lv3'] if row['clscode_lv3'] else '',
                                                      axis=1)
        result['foreign_category_lv4'] = result.apply(lambda row: row['clscode_lv4'] if row['clscode_lv4'] else '',
                                                      axis=1)
        result['foreign_category_lv5'] = result.apply(lambda row: row['clscode_lv5'] if row['clscode_lv5'] else '',
                                                      axis=1)
        result['source_id'] = self.source_id
        result['cmid'] = self.cmid
        result['consumer_id'] = ''
        result['last_updated'] = datetime.now(_TZINFO)
        result['pos_id'] = ''

        result = result.rename(columns={
            'orgcode': 'foreign_store_id',
            'orgname': 'store_name',
            'serialno': 'receipt_id',
            'accdate': 'saletime',
            'plucode': 'foreign_item_id',
            'barcode': 'barcode',
            'pluname': 'item_name',
            'unit': 'item_unit',
            'pluprice': 'saleprice',
            'pluqty': 'quantity',
            'plutotal': 'subtotal',
            'clsname': 'foreign_category_lv1_name',
            'clsname_lv2': 'foreign_category_lv2_name',
            'clsname_lv3': 'foreign_category_lv3_name',
            'clsname_lv4': 'foreign_category_lv4_name',
            'clsname_lv5': 'foreign_category_lv5_name',

        })

        result = result[[
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

        return result

    """
        "origin_table_columns": {
                "goodssale": ['orgcode', 'plucode', 'accdate', 'counts', 'amount', 'cost', 'adjustprofit'],
                "subshop": ['orgcode','orgname',],
                "goods": ['plucode', 'clscode', 'barcode', 'pluname', 'unit'],
                "gclass": ['clscode', 'clslevel', 'clsname']
            },

            "converts": {
                "goodssale": {
                    'orgcode':'str',
                    'plucode':'str',
                    'counts':'float',
                    'amount':'float',
                    'cost':'float',
                    'adjustprofit':'float',
                },
                "subshop": {
                    'orgcode':'str',
                },
                "goods": {
                    'plucode':'str',
                    'clscode':'str',
                },
                "gclass": {
                    'clscode':'str',
                    'clslevel':'int'
                }
            }
        """

    def cost(self):
        cost = self.data['goodssale']
        store = self.data['subshop']
        goods = self.data['goods']
        gclass = self.data['gclass']

        goods['clscode_1'] = goods.apply(lambda row: row['clscode'][:2], axis=1)
        goods['clscode_2'] = goods.apply(lambda row: row['clscode'][:4], axis=1)
        goods['clscode_3'] = goods.apply(lambda row: row['clscode'][:6], axis=1)
        goods['clscode_4'] = goods.apply(lambda row: row['clscode'][:8], axis=1)
        goods['clscode_5'] = goods.apply(lambda row: row['clscode'][:10], axis=1)

        result = cost.merge(
            store,
            on='orgcode',
            how='left'
        ).merge(
            goods,
            on='plucode',
            how='left',
        ).merge(
            gclass,
            left_on='clscode_1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        ).merge(
            gclass,
            left_on='clscode_2',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv2')
        ).merge(
            gclass,
            left_on='clscode_3',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv3')
        ).merge(
            gclass[gclass['clslevel'] == 4],
            left_on='clscode_4',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv4')
        ).merge(
            gclass[gclass['clslevel'] == 5],
            left_on='clscode_5',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv5')
        )

        result['foreign_category_lv1'] = result.apply(lambda row: row['clscode_lv1'] if row['clscode_lv2'] else '',
                                                      axis=1)
        result['foreign_category_lv2'] = result.apply(lambda row: row['clscode_lv2'] if row['clscode_lv2'] else '',
                                                      axis=1)

        result['foreign_category_lv3'] = result.apply(lambda row: row['clscode_lv3'] if row['clscode_lv3'] else '',
                                                      axis=1)
        result['foreign_category_lv4'] = result.apply(lambda row: row['clscode_lv4'] if row['clscode_lv4'] else '',
                                                      axis=1)
        result['foreign_category_lv5'] = result.apply(lambda row: row['clscode_lv5'] if row['clscode_lv5'] else '',
                                                      axis=1)

        result['total_cost'] = result.apply(lambda row: row['cost'] + row['adjustprofit'], axis=1)
        result['source_id'] = self.source_id
        result['cmid'] = self.cmid
        result['cost_type'] = ''

        result = result.rename(columns={
            'orgcode': 'foreign_store_id',
            'plucode': 'foreign_item_id',
            'accdate': 'date',
            'counts': 'total_quantity',
            'amount': 'total_sale',
        })

        result = result[[
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

        return result

    """
    "origin_table_columns": {
                "subshop": ['orgcode','orgname','address', 'telephone','linkman','orgtype','shopareacode',]
                'shoparea':['shopareacode','shopareaname']
            },

            "converts": {
                "subshop": {
                    'orgcode':'str',
                    'shopareacode':'str',
                    'orgtype':'str'
                },
                'shoparea': {'shopareacode':'str'}
            }
    """

    def store(self):
        store = self.data['subshop']
        area = self.data['shoparea']

        result = store.merge(
            area,
            on='shopareacode',
            how='left',
        )

        orgtype_dict = {
            '0': '自营店',
            '1': '加盟店',
            '2': '配送中心'
        }

        result['property'] = result.apply(lambda row: orgtype_dict[row['orgtype']], axis=1)
        result['cmid'] = self.cmid
        result['source_id'] = self.source_id
        result['address_code'] = ''
        result['device_id'] = ''
        result['store_status'] = ''
        result['create_date'] = ''
        result['lat'] = None
        result['lng'] = None
        result['business_area'] = None
        result['last_updated'] = datetime.now(_TZINFO)
        result['show_code'] = result['orgcode']

        result = result.rename(columns={
            'orgcode': 'foreign_store_id',
            'orgname': 'store_name',
            'address': 'store_address',
            'telephone': 'phone_number',
            'linkman': 'contacts',
            'shopareacode': 'area_code',
            'shopareaname': 'area_name',
            'orgtype': 'property_id',
        })

        result = result[[
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

        return result

    """
            "origin_table_columns": {
            "goods": ['clscode',
                      'brand',
                      'vendorcode',
                      'barcode',
                      'plucode',
                      'pluname',
                      'jprice',
                      'price',
                      'unit',
                      'keepdays',
                      'plucode',
                      'plustatus',

                      ],
            "gclass": ['clscode', 'clslevel'],
            "vendor": ['vendorcode', 'delivermode', 'vendorname'],
            "brand": ['brandcode', 'brandname']
        },

        "converts": {
            "goods": {
                'clscode': 'str',
                'brand': 'str',
                'vendorcode': 'str',
                'barcode': 'str',
                'plustatus': 'str',
                'plucode': 'str',
                'keepdays': 'str'

            },
            "gclass": {
                'clscode': 'str',
                'clslevel': 'str'
            },
            "vendor": {
                'vendorcode': 'str'
            },
            "brand": {
                'brandcode': 'str'
            }
        }
            """

    def goods(self):
        goods = self.data['goods']
        gclass = self.data['gclass']
        vendor = self.data['vendor']
        brand = self.data['brand']

        goods['clscode_1'] = goods.apply(lambda row: row['clscode'][:2], axis=1)
        goods['clscode_2'] = goods.apply(lambda row: row['clscode'][:4], axis=1)
        goods['clscode_3'] = goods.apply(lambda row: row['clscode'][:6], axis=1)
        goods['clscode_4'] = goods.apply(lambda row: row['clscode'][:8], axis=1)
        goods['clscode_5'] = goods.apply(lambda row: row['clscode'][:10], axis=1)

        result = goods.merge(
            gclass,
            left_on='clscode_1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        ).merge(
            gclass,
            left_on='clscode_2',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv2')
        ).merge(
            gclass,
            left_on='clscode_3',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv3')
        ).merge(
            gclass[gclass['clslevel'] == 4],
            left_on='clscode_4',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv4')
        ).merge(
            gclass[gclass['clslevel'] == 5],
            left_on='clscode_5',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv5')
        ).merge(
            vendor,
            on='vendorcode',
            how='left'
        ).merge(
            brand,
            left_on='brand',
            right_on='brandcode',
            how='left'
        )

        def status_convert(row):
            if row['plustatus'] == '0':
                return '正常品'
            elif row['plustatus'] == '1':
                return '残次品'
            elif row['plustatus'] == '2':
                return '淘汰品'
            elif row['plustatus'] == '9':
                return '未审核'
            else:
                return '新品'

        result['item_status'] = result.apply(status_convert, axis=1)

        result['foreign_category_lv3'] = result.apply(lambda row: row['clscode_lv3'] if row['clscode_lv3'] else '',
                                                      axis=1)
        result['foreign_category_lv4'] = result.apply(lambda row: row['clscode_lv4'] if row['clscode_lv4'] else '',
                                                      axis=1)
        result['foreign_category_lv5'] = result.apply(lambda row: row['clscode_lv5'] if row['clscode_lv5'] else '',
                                                      axis=1)

        result['storage_time'] = datetime.now(_TZINFO)
        result['last_updated'] = datetime.now(_TZINFO)
        result['isvalid'] = '1'
        result['show_code'] = result['plucode']
        result['cmid'] = self.cmid

        result = result.rename(columns={
            'barcode': 'barcode',
            'plucode': 'foreign_item_id',
            'pluname': 'item_name',
            'jprice': 'lastin_price',
            'price': 'sale_price',
            'unit': 'item_unit',
            'clscode_lv1': 'foreign_category_lv1',
            'clscode_lv2': 'foreign_category_lv2',
            'keepdays': 'warranty',
            'delivermode': 'allot_method',
            'vendorname': 'supplier_name',
            'vendorcode': 'supplier_code',
            'brandname': 'brand_name',

        })

        result = result[[
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

        result['warranty'] = result.apply(lambda row: row['warranty'].split('.')[0] if row['warranty'] else '0', axis=1)
        print(result['warranty'])

        return result

    """
    "origin_table_columns": {
                    "gclass": ['clscode','clslevel', 'uppercode1', 'clsname'],

                },

                "converts": {
                    "gclass": {
                        'clscode':'str',
                        'clslevel':'int',
                        'uppercode1':'str'
                    },

                }
    """

    def category(self):
        gclass = self.data['gclass']

        part1 = gclass[gclass['clslevel'] == 1]
        part1['cmid'] = self.cmid
        part1 = part1.rename(columns={
            'clslevel': 'level',
            'clscode': 'foreign_category_lv1',
            'clsname': 'foreign_category_lv1_name'
        })

        part1['foreign_category_lv2'] = ''
        part1['foreign_category_lv2_name'] = None
        part1['foreign_category_lv3'] = ''
        part1['foreign_category_lv3_name'] = None
        part1['foreign_category_lv4'] = ''
        part1['foreign_category_lv4_name'] = None
        part1['foreign_category_lv5'] = ''
        part1['foreign_category_lv5_name'] = None
        part1['last_updated'] = datetime.now(_TZINFO)

        part2 = gclass.merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        )

        part2 = part2[part2['clslevel'] == 2]
        part2['cmid'] = self.cmid
        part2 = part2.rename(columns={
            'clslevel': 'level',
            'clscode_lv1': 'foreign_category_lv1',
            'clsname_lv1': 'foreign_category_lv1_name',
            'clscode': 'foreign_category_lv2',
            'clsname': 'foreign_category_lv2_name'
        })

        part2['foreign_category_lv3'] = ''
        part2['foreign_category_lv3_name'] = None
        part2['foreign_category_lv4'] = ''
        part2['foreign_category_lv4_name'] = None
        part2['foreign_category_lv5'] = ''
        part2['foreign_category_lv5_name'] = None
        part2['last_updated'] = datetime.now(_TZINFO)

        part3 = gclass.merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv2')
        ).merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        )

        part3 = part3[part3['clslevel'] == 3]

        part3['cmid'] = self.cmid
        part3 = part3.rename(columns={
            'clslevel': 'level',
            'clscode_lv1': 'foreign_category_lv1',
            'clsname_lv1': 'foreign_category_lv1_name',
            'clscode_lv2': 'foreign_category_lv2',
            'clsname_lv2': 'foreign_category_lv2_name',
            'clscode': 'foreign_category_lv3',
            'clsname': 'foreign_category_lv3_name',
        })

        part3['foreign_category_lv4'] = ''
        part3['foreign_category_lv4_name'] = None
        part3['foreign_category_lv5'] = ''
        part3['foreign_category_lv5_name'] = None
        part3['last_updated'] = datetime.now(_TZINFO)

        part4 = gclass.merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv3')
        ).merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv2')
        ).merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        )

        part4 = part4[part4['clslevel'] == 4]

        part4['cmid'] = self.cmid
        part4 = part4.rename(columns={
            'clslevel': 'level',
            'clscode_lv1': 'foreign_category_lv1',
            'clsname_lv1': 'foreign_category_lv1_name',
            'clscode_lv2': 'foreign_category_lv2',
            'clsname_lv2': 'foreign_category_lv2_name',
            'clscode_lv3': 'foreign_category_lv3',
            'clsname_lv3': 'foreign_category_lv3_name',
            'clscode': 'foreign_category_lv4',
            'clsname': 'foreign_category_lv4_name',
        })

        part4['foreign_category_lv5'] = ''
        part4['foreign_category_lv5_name'] = None
        part4['last_updated'] = datetime.now(_TZINFO)

        part5 = gclass.merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv4')
        ).merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv3')
        ).merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv2')
        ).merge(
            gclass,
            left_on='uppercode1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        )

        part5 = part5[part5['clslevel'] == 5]

        part5['cmid'] = self.cmid
        part5 = part5.rename(columns={
            'clslevel': 'level',
            'clscode_lv1': 'foreign_category_lv1',
            'clsname_lv1': 'foreign_category_lv1_name',
            'clscode_lv2': 'foreign_category_lv2',
            'clsname_lv2': 'foreign_category_lv2_name',
            'clscode_lv3': 'foreign_category_lv3',
            'clsname_lv3': 'foreign_category_lv3_name',
            'clscode_lv4': 'foreign_category_lv4',
            'clsname_lv4': 'foreign_category_lv4_name',
            'clscode': 'foreign_category_lv5',
            'clsname': 'foreign_category_lv5_name',
        })

        part5['last_updated'] = datetime.now(_TZINFO)

        filter_columns = [
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

        part1 = part1[filter_columns]
        part2 = part2[filter_columns]
        part3 = part3[filter_columns]
        part4 = part4[filter_columns]
        part5 = part5[filter_columns]

        return pd.concat([part1, part2, part3, part4, part5])

    """
        "origin_table_columns": {
            "differform": ['differno', 'orgcode', 'accdate'],
            "differdetail": ['differno', 'plucode', 'ykcount', 'yktotal'],
            "subshop": ['orgcode', 'orgname'],
            "goods": ['plucode', 'clscode', 'barcode', 'pluname', 'unit']
            'gclass':['clscode', 'clslevel'],
        },

        "converts": {
            "differform": {
                'differno':'str',
                'orgcode':'str',

            },
            "differdetail": {
                'differno':'str',
                'plucode':'str',
                'ykcount':'float'
            },
            "subshop": {
                'orgcode':'str'
            },
            "goods": {
                'plucode':'str',
                'clscode':'str',
                'barcode':'str'
            },
            'gclass':{
                'clscode':'str',
                'clslevel':'int'
            }
        }
    """

    def goods_loss(self):

        header = self.data['differform']

        if not len(header):
            return pd.DataFrame()

        detail = self.data['differdetail']
        store = self.data['subshop']
        item = self.data['goods']
        gclass = self.data['gclass']

        item['clscode_1'] = item.apply(lambda row: row['clscode'][:2], axis=1)
        item['clscode_2'] = item.apply(lambda row: row['clscode'][:4], axis=1)
        item['clscode_3'] = item.apply(lambda row: row['clscode'][:6], axis=1)
        item['clscode_4'] = item.apply(lambda row: row['clscode'][:8], axis=1)
        item['clscode_5'] = item.apply(lambda row: row['clscode'][:10], axis=1)

        result = header.merge(
            detail,
            on='differno',
            how='left'
        ).merge(
            store,
            on='orgcode',
            how='left'
        ).merge(
            item,
            on='plucode',
            how='left'
        ).merge(
            gclass,
            left_on='clscode_1',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv1')
        ).merge(
            gclass,
            left_on='clscode_2',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv2')
        ).merge(
            gclass,
            left_on='clscode_3',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv3')
        ).merge(
            gclass[gclass['clslevel'] == 4],
            left_on='clscode_4',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv4')
        ).merge(
            gclass[gclass['clslevel'] == 5],
            left_on='clscode_5',
            right_on='clscode',
            how='left',
            suffixes=('', '_lv5')
        )

        result = result[result['ykcount'] < 0]

        result['foreign_category_lv3'] = result.apply(lambda row: row['clscode_lv3'] if row['clscode_lv3'] else '',
                                                      axis=1)
        result['foreign_category_lv4'] = result.apply(lambda row: row['clscode_lv4'] if row['clscode_lv4'] else '',
                                                      axis=1)
        result['foreign_category_lv5'] = result.apply(lambda row: row['clscode_lv5'] if row['clscode_lv5'] else '',
                                                      axis=1)
        result['cmid'] = self.cmid
        result['source_id'] = self.source_id
        result['store_show_code'] = result['orgcode']
        result['item_showcode'] = result['plucode']

        result = result.rename(columns={
            'differno':'lossnum',
            'accdate': 'lossdate',
            'orgcode': 'foreign_store_id',
            'orgname': 'store_name',
            'plucode': 'foreign_item_id',
            'barcode': 'barcode',
            'pluname': 'item_name',
            'unit':'item_unit',
            'ykcount':'quantity',
            'yktotal':'subtotal',
            'clscode_lv1':'foreign_category_lv1',
            'clscode_lv2':'foreign_category_lv2'
        })

        result = result[[
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
        ]]

        return result
