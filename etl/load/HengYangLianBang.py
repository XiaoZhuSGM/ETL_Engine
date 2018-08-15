# -*- coding: utf-8 -*-
# @Time    : 2018/8/15 下午2:13
# @Author  : 范佳楠

import boto3
import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context



boto_client = boto3.client('s3')


'''
    先处理goodsflow表 -- 商品销售表
    1. 从s3上面将数据读取下来，文件是gz文件,
'''
branch_info_path = 'https://s3.cn-north-1.amazonaws.com.cn/ext-etl-data/datapipeline/source_id%3D72YYYYYYYYYYYYY/ext_date%3D2018-08-13/table%3Dt_bd_branch_info/dump%3D2018-08-15+14%3A09%3A11.610040%2B08%3A00%26rowcount%3D24.csv.gz'

branch_info_frame = pd.read_csv(filepath_or_buffer=branch_info_path)
print(branch_info_frame)