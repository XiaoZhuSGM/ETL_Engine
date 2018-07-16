# -*- coding: utf-8 -*-

from etl.etl import db
from score import Score, ScoreXml, ScoreEval, RushStrategy, Rush, Video, Kara, ScoreImage
from artist import Artist
from collection import Collection
from external import AppImage, AppResource, App
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
            print e
            raise e
            # return e.message
        finally:
            session.close()

    return wrapper
