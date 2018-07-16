# -*- coding: utf-8 -*-
import logging
import os
from logging.handlers import TimedRotatingFileHandler


class LogManager(object):
    def __init__(self, app):
        self.logger = logging.getLogger(app)
        self.logger.setLevel(logging.INFO)
        self.formatter = logging.Formatter(
            "%(asctime)s - %(funcName)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")


class FileLogManager(LogManager):
    """
    文件记录日志信息,每个一天生产一个日志文件，保留最近30天。
    """

    def __init__(self, app):
        super(FileLogManager, self).__init__(app)
        file_handler = TimedRotatingFileHandler(os.path.join("/", "tmp", app), when='D', interval=1, backupCount=30)
        file_handler.setLevel(logging.DEBUG)
        # file_handler.suffix = "%Y-%m-%d_%H-%M.log"
        # file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}.log$")

        file_handler.setFormatter(self.formatter)

        self.logger.addHandler(file_handler)

    def __call__(self, *args, **kwargs):
        return self.logger


class ConsoleLogManager(LogManager):
    """
    控制台打印日志，用于开发阶段
    """

    def __init__(self, app):
        super(ConsoleLogManager, self).__init__(app)
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.DEBUG)

        stream_handler.setFormatter(self.formatter)

        self.logger.addHandler(stream_handler)

    def __call__(self, *args, **kwargs):
        return self.logger
