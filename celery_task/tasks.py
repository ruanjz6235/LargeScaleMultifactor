#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/8/11 2:18 下午  
# @author: ctp


import os
from flask import current_app

from app.celery_task import celery
from app.common.log import logger


# 通过celery.task装饰耗时任务函数,bind为True会传入self给被装饰的函数,用于记录和更新任务状态
@celery.task(bind=True)
def apptask(self):
    logger.info(current_app.config)
    logger.info("==============%s " % current_app.config["SQLALCHEMY_DATABASE_URI"])
    logger.info("++++++++++++++%s " % os.getenv("DATABASE_URL"))

    try:
        logger.info("exec test task")


    except Exception as e:
        logger.exception("exec test task")

    return 'success'


