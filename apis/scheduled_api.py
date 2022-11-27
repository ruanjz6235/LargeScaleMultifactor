#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/1/19 10:50 上午
import time
from datetime import datetime

from flask_restplus import api

from celery_task import scheduled_tasks
import extensions
from base import *

ns = api.namespace('scheduled', description='定时调度接口')

scheduledRequestParams = api.model('scheduledRequestParams', {
    'request_id': fields.String(description='请求ID', example='test'),
    'type': fields.String()
})

scheduledResultData = api.model('scheduledResultData', {
    'taskId': fields.String(description='任务ID'),
})
scheduledResponse = api.clone('scheduledResponse', baseResponse, {
    'data': fields.Nested(scheduledResultData),
})


@ns.route('/exec_model_task', doc={"description": "定时调度模型任务"})
class ScheduleTopsisMothly(BaseResource):
    """
      定时调度模型计算
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('name', required=True, type=str,
                                 location=['form', 'json', 'args', 'files', 'values', 'headers'])

    @ns.expect(scheduledRequestParams)
    @ns.marshal_with(scheduledResponse)
    def post(self):
        request_data = api.payload
        request_id = request_data['request_id']
        type = request_data['type']
        logger.info("[request_id:%(request_id)s]定时调度计算type:%(type)s" % request_data)
        try:
            scheduled_tasks.model_task.apply_async(kwargs=request_data)
            return ResultSuccess(data={"taskId": request_id})
        except Exception as e:
            logger.exception('[request_id:%(request_id)s]定时调度type:%(type)s计算,error' % request_data)
            return ResultError(msg='定时调度type:%s计算出现错误' % type)
        finally:
            pass
