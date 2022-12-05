#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/12/1 8:15 上午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : ret_attribution.py
# @IDE     : PyCharm
# @REMARKS : 说明文字


import pandas as pd
import numpy as np
from functools import reduce

from const import nc
from data_transform import DataTransform

from query.ret_attribution import (get_tradingdays,
                                   StyleSelect,
                                   RetSelect)


class RetAttr:
    """
    a class to calculate position and attribution
    portfolio.columns = ['fund', 'date', 'code', 'weight', 'style']
    'style' here means asset type
    """
    def __init__(self, portfolio: pd.DataFrame, start: str = '', end: str = ''):
        self.portfolio = portfolio
        self.codes = portfolio[nc.code_name].drop_duplicates().tolist()

        start = portfolio[nc.date_name].min() if len(start) == 0 else start
        end = portfolio[nc.date_name].max() if len(end) == 0 else end
        self.tradingdays = get_tradingdays(start, end)
        self.style_list = None

    def get_style(self, style, **kwargs):
        """
        Default variable cols exist in kwargs when style == 'asset'
        """
        cols = kwargs.get('cols')
        if cols:
            kwargs.pop('cols')
        else:
            if style == 'asset':
                raise KeyError("Default variable cols exist in kwargs when style == 'asset'")
            else:
                cols = self.portfolio.columns[5:]
                cols = dict(zip(cols, cols))

        if style != 'asset':

            if f'{style}_style_data' not in self.__dict__.keys():

                nm = [x for x in kwargs.keys() if len(kwargs[x].split(', ')) > 1 and x != 'dates']
                # one = {k: v for k, v in kwargs.items() if k in nm}
                # other = {k: v for k, v in kwargs.items() if k not in nm}
                # new_object = pd.MultiIndex.from_product(one.values()).to_frame(index=False, name=one.keys())
                # new_object = list(new_object.T.to_dict().values())

                style_data = StyleSelect.get_data(query_name=f'{style}_style',
                                                  schema='funddata', **kwargs)
                style_data[nc.style_name] = reduce(lambda x, y: x + '--' + y,
                                                   [style_data[i] for i in nm+[nc.style_name]])

                setattr(self, f'{style}_style_data', style_data.drop(nm, axis=1))

            style_data = getattr(self, f'{style}_style_data')
            merge_on = [nc.code_name] + ([nc.date_name] if nc.date_name in style_data.columns else [])

            self.portfolio = self.portfolio.drop(self.portfolio.columns[5:], axis=1).merge(style_data, on=merge_on, how='left')
            self.portfolio[nc.style_name] = self.portfolio[nc.style_name].fillna('其他')

        if style not in ['barra']:
            self.portfolio = DataTransform(self.portfolio).get_dummy(cols).get_df()
        self.style_list = self.portfolio.columns[5:]

    def fill_portfolio(self):
        dates = self.portfolio[nc.date_name].unique()
        miss_dates = self.tradingdays[~np.isin(self.tradingdays, dates)]
        port = self.portfolio.set_index([nc.date_name, nc.code_name]).unstack().reset_index()
        port = port.append(pd.DataFrame(dates, columns=[nc.date_name])).sort_values(nc.date_name)
        port.loc[port.index.isin(miss_dates), 'miss'] = 1
        first_dates = port.loc[(~port['miss'].isna()) & (port['miss'].shift(1).isna())][nc.date_name].tolist()
        port['miss'] = pd.cut(port[nc.date_name], first_dates)
        for i in port['miss'].drop_duplicates().tolist():
            port.loc[port['miss'] == i, port.columns[:-1]] = port[port.columns[:-1]].ffill()
        del port['miss']
        self.portfolio = port.copy()
        return port.stack().reset_index()

    def get_stock_ret(self, asset, asset_ret=None, **kwargs):
        if asset_ret is not None:
            setattr(self, f'{asset}_ret', asset_ret)
        else:
            if asset == 'stock':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='stock_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)
            elif asset == 'asset':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='asset_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)
            elif asset == 'bond':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='bond_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)
            elif asset == 'future':
                if f'{asset}_ret' not in self.__dict__.keys():
                    asset_ret = RetSelect.get_data(query_name='future_ret', schema='funddata', **kwargs)
                    setattr(self, f'{asset}_ret', asset_ret)

        self.portfolio = self.portfolio.merge(getattr(self, f'{asset}_ret'),
                                              on=[nc.code_name, nc.date_name],
                                              how='inner')

    def get_daily_attr(self, port, lst=[]):
        if len(lst) == 0:
            lst = self.style_list
        port1, port2 = port.copy(), port.copy()
        port1[lst] = port1[lst].mul(port1[nc.weight_name], axis=0).mul(port1[nc.ret_name], axis=0)
        port1 = port1.groupby([nc.fund_name, nc.date_name])[lst].sum()
        port1_new = port1.stack().reset_index()
        port1_new.columns = [nc.fund_name, nc.date_name, nc.style_name, nc.va_name]

        port2[lst] = port2[lst].mul(port2[nc.weight_name], axis=0)
        port2 = port2.groupby([nc.fund_name, nc.date_name])[lst].sum()
        port2_new = port2.stack().reset_index()
        port2_new.columns = [nc.fund_name, nc.date_name, nc.style_name, nc.weight_name]

        port_new = port1_new.merge(port2_new, on=port1_new.columns[:3].to_list(), how='outer')
        return port1, port2, port_new

    def get_daily_style(self, port, lst=[]):
        if len(lst) == 0:
            lst = self.style_list
        port[lst] = port[lst].mul(port[nc.weight_name], axis=0)
        return port[lst].sum()

    def get_cum_attr(self, port, lst=[]):
        """this is just one method of multi-period of decomposition of portfolio return"""
        if len(lst) == 0:
            lst = self.style_list
        port['va_all'] = port[lst].sum(axis=1)
        port = port.groupby(nc.fund_name).apply(lambda x: x[lst] * (1 + x['va_all']))
        return port
