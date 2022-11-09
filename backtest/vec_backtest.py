#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/1 11:35 上午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : vec_backtest.py
# @IDE     : PyCharm
# @REMARKS : 说明文字
import numpy as np
import pandas as pd
import feather
from itertools import product
from query.backtest import ProcessSelect
from data_transform import DataTransform
from const import nc


class VecBackTest:
    def __init__(self,
                 ret,
                 obj_num=200,
                 cand=True,
                 ls=True,
                 **kwargs):
        """
        obj_num代表标的筛选数量，cand代表是否候补满筛选数量，**kwargs代表其他筛选标准
        :param ret:
        :param obj_num:
        :param cand:
        :param ls:
        :param kwargs:
        """
        self.obj_num = obj_num
        self.cand = cand
        self.ls = ls
        self.kwargs = kwargs

        self.ret = ret
        self.mask = pd.DataFrame()
        self.score = None
        self.weight = None

        self.params = None
        self.dates = None
        self.codes = None

    def no_diy_params(self):
        obj_num = list(range(100, 600, 100))
        cand = [True, False]
        ls = [True, False]
        self.params = list(product(obj_num, cand, ls))

    @classmethod
    def add(cls, df1, df2):
        column1 = df1.columns[~df1.columns.isin(df2.columns)]
        column2 = df2.columns[~df2.columns.isin(df1.columns)]
        new1 = pd.DataFrame(np.ones(shape=(len(df1), len(column2))), columns=column2, index=df1.index)
        new2 = pd.DataFrame(np.ones(shape=(len(df2), len(column1))), columns=column1, index=df2.index)
        df1_new = pd.concat([df1, new1], axis=1)
        df2_new = pd.concat([df2, new2], axis=1)
        return df1_new + df2_new

    @classmethod
    def trans_with_last(cls, array, dim=0):
        if dim < 0:
            dim = dim - len(array.shape)
        list_new = np.array(range(len(array.shape)))
        list_new[dim] = len(array.shape) - 1
        list_new[len(array.shape) - 1] = dim
        return list_new

    @classmethod
    def insert_from_last(cls, array, dim=0):
        if dim < 0:
            dim = dim - len(array.shape)
        list_new = np.array(range(len(array.shape)))
        list_new = np.insert(list_new, dim, list_new[-1])[:-1]
        return list_new

    @classmethod
    def pull_to_last(cls, array, dim=0):
        if dim < 0:
            dim = dim - len(array.shape)
        list_new = np.array(range(len(array.shape)))
        list_new = np.delete(np.append(list_new, dim), dim)
        return list_new

    @classmethod
    def rolling_window(cls, array: np.array, window, dim=0):

        array = array.transpose(cls.pull_to_last(array, dim))

        shape = array.shape[:-1] + (array.shape[-1] - window + 1, window)
        strides = array.strides + (array.strides[-1],)
        rolling_array = np.lib.stride_tricks.as_strided(array, shape=shape, strides=strides)

        return rolling_array.transpose(cls.insert_from_last(rolling_array, dim)).transpose(
            cls.insert_from_last(rolling_array, dim))

    @classmethod
    def compute_mask(cls, query_name, mask_rule={0: 0, 1: 1}, dummy=True, **kwargs):
        # tmp = ProcessSelect.get_data(getattr(ProcessSelect, query_name), **kwargs)
        tmp = feather.read_dataframe(query_name.format(**kwargs))
        if dummy:
            tmp = DataTransform(tmp.set_index([nc.date_name, nc.code_name])).get_dummy().get_df()
        mask = tmp.where(tmp.isin(mask_rule[1]), 1)
        mask = mask.where(~mask.isna(), 0)
        return mask

    def get_mask(self, query_name=['st', 'suspend'], mask_rule=[{0: 0}, {0: 0}], dummy=[True, True], **kwargs):

        for i in len(query_name):
            sub_mask = self.compute_mask(query_name[i], mask_rule[i], dummy[i], **kwargs)
            self.mask = self.add(self.mask, sub_mask)

        self.mask = np.exp(self.mask.where(self.mask > 0, - np.inf)).T

    def _get_weight(self, score: pd.DataFrame, obj_num=200, cand=False, ls=False, **kwargs):
        mask = self.mask.where(self.mask == 0, np.nan).where(self.mask == 1, 0)
        score, mask = DataTransform(score).align(mask)
        # 其他方法
        if len(kwargs) == 0:
            # 有候补
            if cand:
                score = score + mask
            # 多头
            self.score = score.where(score.quantile(obj_num/len(score))).where(~score.isna(), 0).where(score.isna(), 1)
            # 是否有多空
            if ls:
                score1 = score.where(score.quantile(1 - obj_num/len(score))).where(~score.isna(), 0).where(score.isna(), - 1)
                self.score = self.score + score1
            # 若无候选，相关mask掉的股票后面得去掉
            if not cand:
                self.score = (self.score + mask).fillna(0)
        else:
            func = kwargs.pop('func')
            if cand:
                score = score + mask
            self.score = func(score, **kwargs)
            if not cand:
                self.score = (self.score + mask).fillna(0)
        # 归一化
        self.sub_weight = self.score / self.score.sum()

    def sell_limit(self, weight, sell_type=1):
        # todo: sell_suspend, 卖出停牌
        if sell_type == 1:
            self.weight = self.weight.copy()
        elif sell_type == 2:
            self.weight = self.weight.copy()

    def get_weight(self, score, diy=False, **kwargs):
        # 网格法
        if not diy:
            self.no_diy_params()
            assert len(self.params) > 0
            for i, o, c, l in enumerate(self.params):
                self._get_weight(score, obj_num=o, cand=c, ls=l, **kwargs)
                if i == 0:
                    self.weight = self.sub_weight.values
                    continue
                self.weight = np.hstack([self.weight, self.sub_weight.values])

            self.weight = pd.DataFrame(self.weight,
                                       columns=np.hstack([self.sub_weight.columns] * (i + 1)),
                                       index=self.sub_weight.index)

        else:
            self._get_weight(score, obj_num=self.obj_num, cand=self.cand, ls=self.ls, **self.kwargs)
            self.weight = self.sub_weight.copy()
            self.params = [[self.obj_num, self.cand, self.ls]]

        self.dates, self.codes = self.ret.index, self.sub_weight.columns

    def _backtest_period(self, score, diy=False, const='weight', **kwargs):
        """
        打分回测1，单期回测，红利再投
        :param score: 打分
        :param diy: 是否批量输出，False表示批量输出
        :param const: 权重不变(const='weight')还是份额不变(const='share')
        :param kwargs: 权重自定义
        :return:
        """
        self.get_weight(score, diy=diy, **kwargs)

        self.sell_limit(self.weight, sell_type=1)

        self._backtest_port(self.weight, const)

    def _backtest_layer(self, score, diy=False, const='weight', hold_days=20, **kwargs):
        """
        打分回测2，分层回测，红利不投
        资金分为20等份(hold_days等份)滚动投资，计算动态组合收益率
        :param score: 打分
        :param hold_days: 持有天数20天
        :param diy: 是否批量输出，False表示批量输出
        :param const: 权重不变(const='weight')还是份额不变(const='share')
        :param kwargs: 权重自定义
        :return:
        """
        self.get_weight(score, diy=diy, **kwargs)

        # ret.shape = (codes, dates), rolling_ret = (codes*params, window, dates - window + 1)
        rolling_ret = self.rolling_window(self.ret[codes].values, window=hold_days, dim=0)
        rolling_ret = np.stack([rolling_ret] * len(self.params), axis=-1)

        # todo: 1. 保持资金权重不变; 2. 保持份额不变.
        if const == 'weight':
            rolling_weight = np.stack([self.weight.values] * (len(self.dates)-hold_days+1), axis=-2)
            self.sell_limit(rolling_weight, sell_type=1)
            ret_attr = (rolling_weight * rolling_ret).reshape(
                len(self.dates)-hold_days+1, hold_days, len(self.params), len(self.codes)).sum(axis=-1) / hold_days
            ret_ts = []
            for i in range(len(self.dates)-hold_days+1):
                ret_ts.append(ret_attr[ret_attr[..., ::-1]].diagonal(axis1=1, axis2=2, offset=-i).sum(axis=-1))
            self.factor_perform = (1 + ret_ts).cumprod(dim=1) - 1
        else:
            pass

    def _backtest_port(self, portfolio, const='weight'):
        """组合回测，给定组合(金额or权重)"""
        self.weight = portfolio.copy()

        self.weight_ret = pd.concat([self.weight, self.ret[self.codes]], axis=1).sort_index().ffill()
        # weight.shape == (dates, codes * params) == (len(weight), len(sub_weight.columns) * len(params))
        weight = self.weight_ret.T.iloc[:-len(self.codes)].T
        ret = self.weight_ret.T.iloc[-len(self.codes):].T
        # todo: 1. 保持资金权重不变; 2. 保持份额不变.
        if const == 'weight':
            ret_attr = weight.mul(ret).values
            ret_ts = ret_attr.reshape(size=(len(self.dates), len(self.codes), len(self.params))).sum(axis=1)
            self.factor_perform = (1 + ret_ts).cumprod(dim=1) - 1
        else:
            pass


def backtest(data, func):
    pass


