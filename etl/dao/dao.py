# -*- coding: utf-8 -*-
import functools
from contextlib import contextmanager

from etl.etl import db


class Dao(object):

    def __init__(self, model):
        self.session = db.session
        self.model = model

    def get_model_by_id(self, model_id):
        return self.model.query.filter_by(id=model_id).one_or_none()

    def flush(self):
        self.session.flush()

    def commit(self):
        try:
            self.session.commit()
        except Exception as e:
            self.rollback()
            raise e

    def rollback(self):
        self.session.rollback()

    def connect(self):
        return self.session.connection()

    def close(self):
        self.session.close()


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
            func(self, *args, **kwargs)
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    return wrapper


@contextmanager
def session_scope_context():
    """
    上下文管理器方式使用session
    :return:
    """
    session = db.session
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
