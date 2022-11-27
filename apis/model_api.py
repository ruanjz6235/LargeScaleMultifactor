#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/4/9 7:13 下午
# @author: ctp
import inspect

import numpy
import pandas
from flask_restplus import api
import model
from base import *
from utils.RedisUtil import RedisUtil
from util import np_array_to_json_dict, structured_array_to_json_dict

ns = api.namespace('api', description='模型api接口')

modelRequestParams = api.model('modelRequestParams', {
    'request_id': fields.String(description='任务type', example='test'),
    'type': fields.String(description='任务type', example='test'),
    'fund_innercode': fields.Integer(description='任务type', example=1),
    'start_date': fields.String(description='任务type', example='test'),
    'end_date': fields.String(description='任务type', example='test'),
    'nv_type': fields.String(description='任务type', example='test'),
    'gzb_authority': fields.Integer(description='任务type', example=1),
    'fund_zscode': fields.Integer(description='任务type', example=1),
    'index_code': fields.String(description='任务type', example='test'),
    'freq': fields.String(description='任务type', example='test'),
    'industry_standard': fields.String(description='任务type', example='test'),
    'selected_date': fields.String(description='任务type', example='test'),
    'cul_type': fields.Integer(description='任务type', example=1),
    'ifmodified': fields.Integer(description='任务type', example=1),
    'fund_innercodes': fields.List(CusObj()),
    'fund_zscodes': fields.List(CusObj()),
    'gzb_authorities': fields.List(CusObj()),
    'data_source': fields.String(description='任务type', example='test'),
    'show': fields.Integer(description='任务type', example=1),
    'client_id': fields.String(description='任务type', example='test'),
    'data_type': fields.String(description='任务type', example='test'),
    'time_type': fields.String(description='任务type', example='test'),
})
modelResponseResult = api.clone('scheduledResponse', baseResponse, {
    'data': CusObj(),
})


@ns.route('/modelApi', doc={"description": "模型api接口"})
class ModelApi(BaseResource):
    """
      定时调度factor_realized_vol计算
    """
    queue_name = 'json-oracle-queue'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('name', required=True, type=str,
                                 location=['form', 'json', 'args', 'files', 'values', 'headers'])

    @ns.expect(modelRequestParams)
    @ns.marshal_with(modelResponseResult)
    def post(self):
        request_dict = None
        request_id = None
        redis_client = RedisUtil.get_redis_client()
        try:
            # request_data is a list, and first element is the queue_name
            request_dict = api.payload
            request_id = request_dict['request_id']
            logger.info('start processing request %s', request_dict)

            # get model method
            request_task = getattr(model, request_dict['type'])

            result = {'request_id': request_id, 'status': 'Result.PROCESSING'}
            self._set_result(redis_client, result)

            # exec model method
            result = request_task(**request_dict)
            result_dict = request_dict.copy()
            result = self._to_json_dict(result)
            result_dict['result'] = result
            result_dict['status'] = Result.FINISHED
            result_dict['source'] = 'source_python'

            self._set_result(redis_client, result_dict)

            logger.info('finish processing request %s', request_dict)

            return ResultSuccess(data=result)
        except Exception as e:
            logger.error('Failed to process request %s, due to error %s', request_dict, e)
            logger.exception(e)
            result = {'request_id': request_id, 'status': Result.ERROR}
            redis_client.set(self._get_result_key(request_id), json.dumps(result), 3600)
            return ResultError(data=result)
        finally:
            pass

    def get_result(self, redis, request_id):
        logger.debug('before retrieving result of request %s', request_id)
        result_bytes = redis.get(self._get_result_key(request_id))
        logger.debug('after retrieving result of request %s', request_id)
        if not result_bytes:
            return None
        return json.loads(result_bytes)

    def _get_result_key(self, request_id):
        return '{queue_name}-result-{request_id}'.format(queue_name=self.queue_name, request_id=str(request_id))

    def _set_result(self, redis, result, result_expire_in=3600):
        key = self._get_result_key(result['request_id'])
        redis.set(key, json.dumps(result), result_expire_in)

    def _to_json_dict(self, result):
        if type(result) in (int, float, str):
            return result
        if type(result) == numpy.float64:
            return result 
        if type(result) == numpy.int64:
            return int(result)            
        # list, set, dict
        if type(result) in (list, set):
            return [self._to_json_dict(item) for item in result]

        if type(result) == dict:
            return {key: self._to_json_dict(value) for key, value in result.items()}

        if type(result) == pandas.DataFrame:
            return {
                column: np_array_to_json_dict(result[column].values) for column in result.columns
            }

        if type(result) == numpy.ndarray:
            return structured_array_to_json_dict(result)

        return result.__dict__
