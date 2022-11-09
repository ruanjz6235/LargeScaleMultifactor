#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/12/22 上午10:46  
# @author: ctp

import os

from celery.signals import task_postrun
from flask import current_app

from app import model
from app.extensions import celery, db
from app.common.log import logger


# 通过celery.task装饰耗时任务函数,bind为True会传入self给被装饰的函数,用于记录和更新任务状态
@celery.task(bind=True)
def model_task(self, **request_data):
    try:
        logger.info("[request_id:%(request_id)s]exec type:%(type)s" % request_data)
        request_task = getattr(model, request_data['type'])
        result = request_task(**request_data)
        logger.info("[request_id:%(request_id)s]exec type:%(type)s success" % request_data)
    except Exception as e:
        logger.exception("[request_id:%(request_id)s]exec type:%(type)s error" % request_data)

    return 'success'


@task_postrun.connect
def close_session(*args, **kwargs):
    # Flask SQLAlchemy will automatically create new sessions for you from
    # a scoped session factory, given that we are maintaining the same app
    # context, this ensures tasks have a fresh session (e.g. session errors
    # won't propagate across tasks)
    db.session.remove()
