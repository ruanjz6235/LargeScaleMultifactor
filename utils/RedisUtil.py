#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/9/10 8:42 上午
from app import extensions
from app.test.local import LocalRedis


class RedisUtil:

    @classmethod
    def get_redis_client(cls):
        if extensions.start_app:
            redis_client = extensions.redis_client
        else:
            redis_client = LocalRedis.get_redis_client()
        return redis_client
