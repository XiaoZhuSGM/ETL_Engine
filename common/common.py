# -*- coding: utf-8 -*-
"""
common function for all app
"""
from datetime import datetime
import hashlib
import random
import time
import json

import boto3
from botocore.client import Config

S3 = boto3.resource('s3')
LAMBDA = boto3.client("lambda", config=Config(connect_timeout=910, read_timeout=910, retries=dict(max_attempts=0)))


def timestamp2format_time(timestamp):
    time_tuple = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S', time_tuple)


def generate_random():
    return random.randint(0, 1000)


# datetime转成字符串
def datetime_to_string(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def string_to_datetime(time_string):
    return datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")


def generate_password(password):
    return hashlib.sha1(password).hexdigest()


def md5sum(file_content):
    fmd5 = hashlib.md5(file_content)
    return fmd5.hexdigest()


def now_timestamp():
    _timestamp = datetime.fromtimestamp(time.time())
    return _timestamp


def upload_body_to_s3(bucket, key, body):
    """
    upload body (xml,json,binary) to s3 key
    :param bucket:
    :param key:
    :param body:
    :return:
    """
    S3.Object(bucket_name=bucket, key=key).put(
        Body=body)


def upload_file_to_s3(bucket, key, file):
    """
    upload a file to s3
    :param bucket:
    :param key:
    :param file:
    :return:
    """
    S3.Bucket(bucket).upload_file(file, key)


def get_content(bucket, key):
    """
    get body from s3 key
    :param bucket:
    :param key:
    :return:
    """
    content = S3.Object(bucket, key).get()
    return json.loads(content["Body"].read().decode("utf-8"))


def get_all_s3_keys(bucket):
    """Get a list of all keys in an S3 bucket."""
    s3 = boto3.client('s3')
    keys = []
    kwargs = {'Bucket': bucket}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    return keys


def get_s3_keys_as_generator(bucket):
    """Generate all the keys in an S3 bucket."""
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            yield obj['Key']

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    s3 = boto3.client('s3')
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


# 分页模版
PAGE_SQL = {
    "oracle": "SELECT * FROM (SELECT RPT.*, ROWNUM RN FROM (SELECT {special_column}  FROM {table}  {wheres} {order_by} ) RPT WHERE  ROWNUM <= {large} )  temp_rpt WHERE RN > {small}",
    "sqlserver": "SELECT * FROM ( SELECT  ROW_NUMBER() OVER ({order_by} ) AS rownum ,{special_column} FROM {table} {wheres} ) AS temp WHERE temp.rownum between {small} and {large}",
}
PAGE_LAST_SQL = {
    "oracle": "SELECT * FROM (SELECT RPT.*, ROWNUM RN FROM (SELECT {special_column} FROM {table}  {wheres} {order_by} ) RPT )  "
              "temp_rpt WHERE RN > {small}",
    "sqlserver": "SELECT * FROM ( SELECT  ROW_NUMBER() OVER ({order_by} ) AS rownum ,{special_column} FROM {table} {wheres} ) AS "
                 "temp WHERE temp.rownum>{small} "
}


S3_BUCKET = 'ext-etl-data'

SQL_PREFIX = 'sql/source_id={source_id}/{date}/'
SQL_INV_PREFIX = 'sql/source_id={source_id}/{date}/inventory/'

TARGET_TABLE_KEY = "target_table/target_table.json"

ALLOWED_EXTENSIONS = set(['xlsx', 'xltx', 'xls', 'xlt', 'et', 'ett', 'csv'])
