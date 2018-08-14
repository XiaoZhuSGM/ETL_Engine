# -*- coding: utf-8 -*-
import json
import time
import traceback
import boto3
import pandas as pd
import pytz

S3_BUCKET = "ext-etl-data"

HISTORY_HUMP_JSON = 'datapipeline/source_id={source_id}/ext_date={date}/history_dump_json/'

_TZINFO = pytz.timezone('Asia/Shanghai')
S3 = boto3.resource("s3")


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

        resp = S3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield key

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def handler(event, context):
    # Check if the incoming message was sent by SNS
    if 'Records' in event:
        message = json.loads(event['Records'][0]['Sns']['Message'])
    else:
        message = event

    source_id = message["source_id"]
    erp_name = message["erp_name"]
    date = message["date"]
    target_table = message["target_table"]
    origin_table = message["origin_table"].split(',')

    prefix = HISTORY_HUMP_JSON.format(source_id=source_id, date=date)

    json_file = get_matching_s3_keys(S3_BUCKET, prefix=prefix, suffix=".json")

    if erp_name == "科脉云鼎":
        clean_kemaiyunding(source_id, date, target_table, origin_table, json_file)
        pass
    elif erp_name == "海鼎":
        pass


def clean_kemaiyunding(source_id, date, target_table, origin_table, json_file):
    pass
