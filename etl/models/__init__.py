# -*- coding: utf-8 -*-

import functools

from etl.etl import db


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
