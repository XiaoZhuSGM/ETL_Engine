# -*- coding: utf-8 -*-
# @Time    : 2018/8/8 上午10:40
# @Author  : 范佳楠

import boto3
from flask import request
from . import etl_admin_api
from ...service.extract import generator_sql_service

S3_BUCKET = 'fanjianan'
EXTRACT_API_GET = '/generator/<string:source_id>'


@etl_admin_api.route(EXTRACT_API_GET, methods=['GET'])
def generator_sql(source_id):
    """
    作用: 生成对应的sql，返回给client,同时也要将当前生成的上传到S3中进行存储，
    所以这个方法最终返回的是json<sql>
    :param source_id: 对当前source_id进行操作
    :return:
    1. 首先我们需要判断当前source_id在我们抓取的这一天是否有数据，这个可以判别出全量和增量
    2. 找出全量和增量之后，
    """
    print('进来了')
    sql_json = generator_sql_service(source_id)
    from .. import jsonify_with_error, jsonify_with_data, APIError
    return jsonify_with_data(APIError.OK, data=sql_json)
