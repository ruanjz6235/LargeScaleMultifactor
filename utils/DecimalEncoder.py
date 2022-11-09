#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/9/10 9:17 上午
import decimal
import json
import datetime


class DecimalEncoder(json.JSONEncoder):

    def default(self, o):

        if isinstance(o, decimal.Decimal):
            return float(o)

        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')

        super(DecimalEncoder, self).default(o)