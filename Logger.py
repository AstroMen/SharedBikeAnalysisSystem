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


# basic_log_path = '/opt/monitor/log' if '-o' in sys.argv else 'log'
task_name = '_'.join(sys.argv[1:]).replace('-', '')
# fl_name = '{}/{}_{}.log'.format(basic_log_path, task_name, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
fmt = '%(asctime)s\tFile \"%(filename)s\", line %(lineno)s\t%(levelname)s: %(message)s'
logger = logging.getLogger(__name__)
formatter = logging.Formatter(fmt)
logger.setLevel(logging.INFO)
# fh = TimedRotatingFileHandler(fl_name, when='MIDNIGHT', interval=1, backupCount=7, encoding="utf-8")  # MIDNIGHT , atTime=datetime.time(0, 0, 0, 0)

# # delete settings
# fh.suffix = '_%Y%-m-%d_%H-%M'
# fh.extMatch = re.compile(r"^_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$")
# fh.setFormatter(formatter)
# logger.addHandler(fh)

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)
