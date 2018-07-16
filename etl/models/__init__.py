# -*- coding: utf-8 -*-

from etl.etl import db
from artist import Artist
from collection import Collection
from internal import Internal
import functools


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
        except Exception, e:
            session.rollback()
            print
            e
            raise e
            # return e.message
        finally:
            session.close()

    return wrapper
