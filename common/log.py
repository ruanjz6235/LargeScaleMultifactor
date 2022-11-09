#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
from cloghandler import ConcurrentRotatingFileHandler
from logging.handlers import RotatingFileHandler
import platform

log_file_path = 'logs'
if not os.path.exists(log_file_path):
    os.makedirs(log_file_path)

# 用于输出到控制台
console_log = logging.StreamHandler()
console_log.setLevel(logging.INFO)

code_log_file = os.path.join(log_file_path, 'orca.log')

if platform.system() == 'Windows':
    _code_log_handler = RotatingFileHandler(code_log_file, maxBytes=100*1024*1024, backupCount=10)
else:
    _code_log_handler = ConcurrentRotatingFileHandler(code_log_file, "a", 50 * 1024 * 1024, 10)

_code_log_formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(thread)d][%(filename)s:%(lineno)d]- %('
                                        'message)s ')

console_log.setFormatter(_code_log_formatter)
_code_log_handler.setFormatter(_code_log_formatter)
# 此处设置logger名称
logger = logging.getLogger('orca')
logger.setLevel(logging.INFO)
logger.addHandler(_code_log_handler)
logger.addHandler(console_log)
