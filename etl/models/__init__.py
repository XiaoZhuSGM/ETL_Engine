# -*- coding: utf-8 -*-

import functools

from etl.etl import db
from .datasource import ExtDatasource
from .etl_table import ExtChainStoreOnline, ExtErpEnterprise, ExtStoreDetail
from .ext_datasource_con import ExtDatasourceCon
from .ext_table_info import ExtTableInfo


def session_scope(func):
    """
    装饰器使用session事务
    :param func:
    :return:
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        session = db.session
        try:
            data = func(self, *args, **kwargs)
            session.commit()
            return data
        except Exception as e:
            session.rollback()
            raise
            # return e.message
        finally:
            session.close()

    return wrapper
