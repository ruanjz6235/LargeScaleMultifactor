#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask_restplus import api

from base import *
from utils.HiveUtil import HiveUtil

ns = api.namespace('sidecar', description='sidecar接口')

healthResponseResult = api.model('healthResponseResult', {
    'status': fields.String(description='status', example='UP'),
})


@ns.route('/health', doc={"description": "health接口"})
class Health(BaseResource):
    """
      health
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parser.add_argument('name', required=True, type=str,
                                 location=['form', 'json', 'args', 'files', 'values', 'headers'])

    @ns.marshal_with(healthResponseResult)
    def post(self):
        return {"status": "UP"}

    @ns.marshal_with(healthResponseResult)
    def get(self):
        return {"status": "UP"}