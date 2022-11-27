#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/2/15 8:11 下午
# @Author : ctp

from pyhive import hive
from sqlalchemy import create_engine
from common.log import logger
from redis import StrictRedis


class LocalDb:

    @classmethod
    def get_engine(cls, bind=None):
        if bind == 'jydb':
            engine = create_engine('mysql+mysqldb://devuser:hcy6YJF123@139.224.238.162/JYDB?charset=utf8')
        elif bind == 'zj_data':
            engine = create_engine('mysql+mysqldb://devuser:hcy6YJF123@139.224.238.162/zj_data?charset=utf8')
        elif bind == 'zhijunfund':
            engine = create_engine('mysql+mysqldb://zhijunfund:zsfdcd82sf2dmd6a@10.56.36.145/zhijunfund?charset=utf8')
        else:
            engine = create_engine('mysql+mysqldb://devuser:hcy6YJF123@139.224.238.162/zhijunfund?charset=utf8')
        return engine


class LocalRedis:

    @classmethod
    def get_redis_client(cls):
        redis_client = StrictRedis(host='139.224.238.162', password='3R13jPUwdSYNvtPD', port=14147,
                                   db=0, decode_responses=False)
        return redis_client


class LocalHive:

    @classmethod
    def get_conn(cls):
        # host主机ip,port：端口号，username:用户名，database:使用的数据库名称
        conn = hive.Connection(host='192.168.0.130', port=10000, username='root',
                               database='default')
        return conn