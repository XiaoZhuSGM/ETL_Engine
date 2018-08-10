# -*- coding: utf-8 -*-
"""
common function for all app
"""
import datetime
import hashlib
import random
import time

import boto3

s3 = boto3.client('s3')


def timestamp2format_time(timestamp):
    time_tuple = time.localtime(timestamp)
    return time.strftime('%Y-%m-%d %H:%M:%S', time_tuple)


def generate_random():
    return random.randint(0, 1000)


# datetime转成字符串
def datetime_to_string(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def string_to_datetime(time_string):
    return datetime.datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")


def generate_password(password):
    return hashlib.sha1(password).hexdigest()


def md5sum(file_content):
    fmd5 = hashlib.md5(file_content)
    return fmd5.hexdigest()


def get_s3_keys(bucket):
    """Get a list of keys in an S3 bucket.
        This call only returns the first 1000 keys
    """
    keys = []
    resp = s3.list_objects_v2(Bucket=bucket)
    for obj in resp['Contents']:
        keys.append(obj['Key'])
    return keys


def get_all_s3_keys(bucket):
    """Get a list of all keys in an S3 bucket."""
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
    "oracle": "SELECT * FROM (SELECT RPT.*, ROWNUM RN FROM (SELECT * FROM {table}  {wheres} order by  {order_rows} desc ) RPT WHERE  ROWNUM <= {large} )  temp_rpt WHERE RN > {small}",
    "sqlserver": "SELECT * FROM ( SELECT  ROW_NUMBER() OVER ( ORDER BY {order_rows} desc ) AS rownum ,* FROM {table} {wheres} ) AS temp WHERE temp.rownum between {small} and {large}",
}
