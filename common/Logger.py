# -*- coding: UTF-8 -*-
"""
Description: MultiTool
@author: Men Luyao
@date: 2019/9/12
"""
import sys, re, datetime
import logging
import ctypes
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler


fmt = '%(asctime)s\tFile \"%(filename)s\", line %(lineno)s\t%(levelname)s: %(message)s'
logger = logging.getLogger(__name__)
formatter = logging.Formatter(fmt)
logger.setLevel(logging.INFO)

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)
