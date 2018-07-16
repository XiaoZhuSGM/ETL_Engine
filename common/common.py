# -*- coding: utf-8 -*-
"""
common function for all app 
"""
import datetime
import hashlib
import random
import time


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
