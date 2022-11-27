#! /usr/bin/env python
# -*- coding: utf-8 -*-
import uuid

from flask_restplus import Resource, reqparse
from flask import request
import json
from common.log import logger
import flask_restplus
from flask_restplus import fields
from extensions import api


class MyRequestParser(reqparse.RequestParser):
    """自定义参数请求"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class BaseResource(Resource):
    default_help = '不是一个有效值'

    # tid_file_dir = Config._REPORT_FILES_DIR

    def __init__(self, *args, **kwargs):
        self.parser = MyRequestParser()
        logger.info(request.remote_addr + '请求参数：' + str(request.json))
        super().__init__(*args, **kwargs)


# 自定义请求体检验
class MyBaseResource(Resource):
    def __init__(self, *args, **kwargs):
        pass

    def get_params(self):
        try:
            params = request.form
            if not params:
                params = json.loads(request.data.decode())
        except:
            flask_restplus.abort(400, message='请求体格式有误')
        logger.info(request.remote_addr + '请求参数：' + str(params))
        return params


class ResponseResult:

    @staticmethod
    def success(msg, data):
        return {"code": 200, "status": 'true', "msg": msg, "data": data}

    @staticmethod
    def error(code=500, msg=None):
        return {"code": code, "status": 'false', "msg": msg}


baseResponse = api.model('baseResponse', {
    'code': fields.Integer(example=200, description='错误码'),
    'status': fields.Boolean(description='请求是否成功标识'),
    'msg': fields.String(description='错误信息'),
})


class CusObj(fields.Raw):
    def format(self, value):
        return value


class ResultSuccess:
    def __init__(self, code=200, msg='success', data=None):
        self.code = code
        self.success = True
        self.msg = msg
        self.data = data


class ResultError:
    def __init__(self, code=500, msg='error', data=None):
        self.code = code
        self.success = False
        self.msg = msg
        self.data = data


class Result:
    NEW = 'NEW'

    SUBMITTED = 'SUBMITTED'

    PROCESSING = 'PROCESSING'

    FINISHED = 'FINISHED'

    ERROR = 'ERROR'

    def __init__(self, request_id, status=None):
        self.request_id = request_id
        self.status = status if status else type(self).NEW

    def to_json_dict(self):
        return {
            'request_id': str(self.request_id),
            'status': self.status
        }

    def __repr__(self):
        return '<Result[request_id: %s, status: %s]>' % (self.request_id, self.status)

    def __str__(self):
        return self.__repr__()

    def __eq__(self, other):
        if type(self) != type(other):
            return False

        return self.request_id == other.request_id and self.status == other.status


class GetUUID:
    @staticmethod
    def getTaskId(data):
        if data.get('request_id'):
            task_id = data.get('request_id')
        else:
            uid = str(uuid.uuid1())
            task_id = ''.join(uid.split('-'))
        return task_id


def get_fun(fn):
    return '.'.join([fn.__module__, fn.__name__])


model_type_request_params = ['manager_index_curve_time_period', 'manager_index_curve',
                             'represent_fund_select', 'represent_fund_curve', 'return_and_distribution',
                             'get_end_date', 'manager_interval_return',
                             'manager_interval_draw_down', 'manager_index_curve_time_period',
                             'return_and_risk_indicators', 'manager_size_time_period', 'manager_size',
                             'asset_distribution_end_date', 'asset_distribution',
                             'asset_distribution_change', 'stock_distribution_end_date',
                             'stock_distribution_time_period',
                             'max_industry_distribution', 'industry_distribution_change', 'max_stocks',
                             'max_stocks_changes',
                             'stocks_details_time_period', 'stocks_details', 'stocks_details_curves',
                             'bond_distribution_end_date', 'bond_distribution_time_period',
                             'bond_distribution', 'bond_distribution_changes', 'asset_distribution_end_date',
                             'manager_size_time_period', 'max_bonds', 'max_bonds_changes']
