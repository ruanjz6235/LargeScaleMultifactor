#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/30 10:59 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : __init__.py.py
# @IDE     : PyCharm
# @REMARKS : 说明文字
import pandas as pd
import numpy as np
from backtest.backtest_vec import VecBackTest
from backtest.backtest_gpu import VecBackTest


# metric
class MetricEngine:
    def __init__(self,
                 method='topsis',
                 weight_method='mean',
                 obj_num=600,
                 cand=True,
                 ls=True,
                 query_name=['st', 'suspend'],
                 mask_rule=[{0: 0}, {0: 0}],
                 dummy=[True, True],
                 diy=True,
                 const='weight',
                 hold_days=20,
                 **kwargs
                 ):
        self.method = method
        self.weight_method = weight_method
        self.obj_num = obj_num
        self.cand = cand
        self.ls = ls
        self.query_name = query_name
        self.mask_rule = mask_rule
        self.dummy = dummy
        self.diy = diy
        self.const = const
        self.hold_days = hold_days
        self.kwargs = kwargs

    def __call__(self, score, ret, index_return):
        bt = VecBackTest(ret, self.obj_num, self.cand, self.ls, **self.kwargs)
        bt.get_mask(self.query_name, self.mask_rule, self.dummy)
        bt.backtest_layer(score, self.diy, self.const, self.hold_days, **bt.kwargs)
        self.asset_return = bt.factor_perform
        if self.method == 'topsis':
            from evaluate.topsis import TopsisEngine as EvaluateEngine
            evaluate_kwargs = {'weight_method': weight_method}
        elif self.method == 'ret':
            from evaluate.ret import VwapEngine as EvaluateEngine
            evaluate_kwargs = {'weight_method': weight_method}
        else:
            from evaluate.topsis import CalIndex as EvaluateEngine
            func = getattr(EvaluateEngine, 'cal_'+method)
            setattr(EvaluateEngine, 'evaluate', func)
            evaluate_kwargs = {}
        port_score = EvaluateEngine.evaluate(self.asset_return, index_return, **evaluate_kwargs)
        return port_score


# %%ret_attribution
from evaluate.ret_attribution import RetAttr
