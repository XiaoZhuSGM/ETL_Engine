# -*- coding: utf8 -*-

from flask import Blueprint

etl_admin_api = Blueprint('admin_api', __name__)

from .ext_table_info import *  # noqa
