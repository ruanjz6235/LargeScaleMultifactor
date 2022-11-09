#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/4/28 上午10:41  
# @author: ctp
from flask import current_app
from pyhive import hive

from app import extensions
from app.test.local import LocalHive


class HiveUtil:

    @classmethod
    def get_conn(cls):
        if extensions.start_app:
            hive_config = current_app.config["HIVE_URI"]
            conn = hive.Connection(**hive_config)
        else:
            conn = LocalHive.get_conn()
        return conn
