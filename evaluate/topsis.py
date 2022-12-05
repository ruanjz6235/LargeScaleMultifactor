#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/12/1 8:04 上午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : topsis.py
# @IDE     : PyCharm
# @REMARKS : 说明文字


import numpy as np
import pandas as pd
import torch.nn as nn


class CalIndex:
    @classmethod
    def cal_alpha_beta(cls, asset_return, index_return, freq):
        if len(asset_return) > 1 and len(index_return) > 1:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            rf = 0.03 / multiplier
            y = asset_return - rf
            x = index_return - rf
            x = sm.add_constant(x)
            model = sm.OLS(y, x).fit()
            alpha = model.params[0]
            alpha = np.exp(multiplier * alpha) - 1
            if len(y) > 1:
                beta = model.params[1]
            else:
                beta = np.nan
        else:
            alpha = np.nan
            beta = np.nan
        return alpha, beta

    @classmethod
    def cal_sharpe(cls, asset_return, freq):
        if not asset_return.empty:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            annualized_return = multiplier * asset_return.mean()
            annualized_return = np.exp(annualized_return) - 1
            annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
            sharpe_ratio = (annualized_return - 0.03) / annualized_vol
        else:
            sharpe_ratio = np.nan
        return sharpe_ratio

    @classmethod
    def cal_calmar(cls, asset_return, mdd):
        if not np.isnan(mdd):
            sharpe_ratio = (asset_return - 0.03) / mdd
        else:
            sharpe_ratio = np.nan
        return sharpe_ratio

    @classmethod
    def cal_ir(cls, asset_return, index_return, freq):
        if len(asset_return) > 0 and len(index_return) > 0:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            active_return = asset_return - index_return
            tracking_error = (active_return.std(ddof=1)) * np.sqrt(multiplier)
            asset_annualized_return = multiplier * asset_return.mean()
            index_annualized_return = multiplier * index_return.mean()
            information_ratio = (asset_annualized_return
                                 - index_annualized_return) / tracking_error
        else:
            information_ratio = np.nan
        return information_ratio

    @classmethod
    def cal_sortino(cls, asset_return, freq):
        if not asset_return.empty:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            target_return = 0.03 / multiplier
            downside_return = asset_return - target_return
            downside_return[downside_return > 0] = 0
            downside_volatility = downside_return.std(ddof=1) * np.sqrt(multiplier)
            annualized_return = multiplier * asset_return.mean()
            sortino_ratio = (annualized_return - 0.03) / downside_volatility
        else:
            sortino_ratio = np.nan
        return sortino_ratio

    @classmethod
    def cal_treynor(cls, asset_return, index_return, freq):
        if len(asset_return) > 1 and len(index_return) > 1:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            rf = 0.03 / multiplier
            if len(asset_return[asset_return.values != 0]) == 0:
                return np.nan
            y = asset_return - rf
            x = index_return - rf
            x = sm.add_constant(x)
            model = sm.OLS(y, x).fit()
            beta = model.params[1]
            annualized_return = multiplier * asset_return.mean()
            treynor_ratio = (annualized_return - 0.03) / beta
        else:
            treynor_ratio = np.nan
        return treynor_ratio

    @classmethod
    def cal_jensen(cls, asset_return, index_return, freq):
        if len(asset_return) > 1 and len(index_return) > 1:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            rf = 0.03 / multiplier
            y = asset_return - rf
            x = index_return - rf
            x = sm.add_constant(x)
            model = sm.OLS(y, x).fit()
            beta = model.params[1]
            asset_annualized_return = multiplier * asset_return.mean()
            index_annualized_return = multiplier * index_return.mean()
            jensens_alpha = asset_annualized_return \
                            - (0.03 + beta * (index_annualized_return - 0.03))
        else:
            jensens_alpha = np.nan
        return jensens_alpha

    @classmethod
    def cal_m2(cls, asset_return, index_return, freq):
        if len(asset_return) > 0 and len(index_return) > 0:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            asset_annualized_return = multiplier * asset_return.mean()
            asset_annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
            index_annualized_vol = index_return.std(ddof=1) * np.sqrt(multiplier)
            m2 = 0.03 + (asset_annualized_return - 0.03) * index_annualized_vol \
                 / asset_annualized_vol
        else:
            m2 = np.nan
        return m2

    @classmethod
    def cal_winning(cls, asset_return, index_return):
        if not asset_return.empty:
            return_diff = asset_return - index_return
            winning_rate = len(return_diff[return_diff > 0]) / len(return_diff)
        else:
            winning_rate = np.nan
        return winning_rate

    @classmethod
    def cal_dynamic_drawdown(cls, asset_return):
        if not asset_return.empty:
            running_max = np.maximum.accumulate(asset_return.cumsum())
            underwater = asset_return.cumsum() - running_max
            underwater = np.exp(underwater) - 1
        else:
            underwater = np.array([])
        return - underwater

    @classmethod
    def cal_mdd(cls, asset_return):
        if not asset_return.empty:
            dynamic_drawdown = cls.get_dynamic_drawdown(asset_return)
            mdd = dynamic_drawdown.min()
        else:
            mdd = np.nan
        return mdd

    @classmethod
    def cal_mdd_month_number(cls, asset_return):
        if len(asset_return) > 1:
            asset_return = asset_return.set_index('init_date')
            asset_return = asset_return.dropna(axis=0, how='any')
            running_max = np.maximum.accumulate(asset_return.cumsum())
            underwater = asset_return.cumsum() - running_max
            underwater = np.exp(underwater) - 1
            valley = underwater.idxmin().iloc[0]
            peak = underwater[:valley][underwater[:valley] == 0].dropna().index[-1]
            month_diff = valley.month - peak.month
            year_diff = valley.year - peak.year
            period_diff = max(12 * year_diff + month_diff, 0)
        else:
            period_diff = np.nan
        return period_diff

    @classmethod
    def cal_mdd_recovery_date(cls, asset_return):
        if len(asset_return) > 1:
            asset_return = asset_return.set_index('init_date')
            asset_return = asset_return.dropna(axis=0, how='any')
            running_max = np.maximum.accumulate(asset_return.cumsum())
            underwater = asset_return.cumsum() - running_max
            underwater = np.exp(underwater) - 1
            valley = underwater[underwater.columns[-1]].idxmin()
            try:
                recovery_date = underwater[valley:][underwater[valley:] == 0].dropna().index[0]
            except IndexError:
                recovery_date = np.nan
        else:
            recovery_date = np.nan
        return recovery_date

    @classmethod
    def cal_mdd_recovery_months_number(cls, asset_return):
        if len(asset_return) > 1:
            asset_return = asset_return.set_index('init_date')
            asset_return = asset_return.dropna(axis=0, how='any')
            running_max = np.maximum.accumulate(asset_return.cumsum())
            underwater = asset_return.cumsum() - running_max
            underwater = np.exp(underwater) - 1
            valley = underwater.idxmin().iloc[0]
            try:
                recovery_date = underwater[valley:][underwater[valley:] == 0].dropna().index[0]
            except IndexError:
                recovery_date = None
            if recovery_date is not None:
                month_diff = recovery_date.month - valley.month
                year_diff = recovery_date.year - valley.year
                period_diff = max(12 * year_diff + month_diff, 0)
            else:
                period_diff = np.nan
        else:
            period_diff = np.nan
        return period_diff

    @classmethod
    def cal_downside_vol(cls, asset_return, freq):
        if freq == 'weekly':
            multiplier = 52
        elif freq == 'monthly':
            multiplier = 12
        else:  # freq == 'daily'
            multiplier = 252
        target_return = 0.03 / multiplier
        if not asset_return.empty:
            downside_return = asset_return - target_return
            downside_return[downside_return > 0] = 0
            downside_volatility = downside_return.std(ddof=1) * np.sqrt(multiplier)
        else:
            downside_volatility = np.nan
        return downside_volatility

    @classmethod
    def cal_var(cls, asset_return, cutoff=0.05):
        if len(asset_return) > 24:
            hist_var = np.quantile(asset_return, cutoff, interpolation='lower')
        else:
            hist_var = np.nan
        return -hist_var

    @classmethod
    def cal_cvar(cls, asset_return, cutoff=0.05):
        if len(asset_return) > 24:
            var = np.quantile(asset_return, cutoff, interpolation='lower')
            conditional_var = asset_return[asset_return <= var].mean()
        else:
            conditional_var = np.nan
        return -conditional_var

    @classmethod
    def cal_annual_vol(cls, asset_return, freq):
        if not asset_return.empty:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            annualized_vol = asset_return.std(ddof=1) * np.sqrt(multiplier)
        else:
            annualized_vol = np.nan
        return annualized_vol

    @classmethod
    def cal_tracking_error(cls, asset_return, index_return, freq):
        if len(asset_return) > 0 and len(index_return) > 0:
            if freq == 'weekly':
                multiplier = 52
            elif freq == 'monthly':
                multiplier = 12
            else:  # freq == 'daily'
                multiplier = 252
            active_return = asset_return - index_return
            tracking_error = (active_return.std(ddof=1)) * np.sqrt(multiplier)
        else:
            tracking_error = np.nan
        return tracking_error

    @classmethod
    def cal_benchmark_corr(cls, asset_return, index_return):
        if len(asset_return) > 0 and len(index_return) > 0:
            benchmark_correlation = asset_return.corr(index_return)
        else:
            benchmark_correlation = np.nan
        return benchmark_correlation

    @classmethod
    def cal_index(cls,
                  asset_return,
                  index_return='000300',
                  freq='daily',
                  cutoff=0.05):
        alpha, beta = cls.cal_alpha_beta(asset_return, index_return, freq)
        sharpe = cls.cal_sharpe(asset_return, freq)
        mdd = cls.cal_mdd(asset_return)
        calmar = cls.cal_calmar(asset_return, mdd)
        ir = cls.cal_ir(asset_return, index_return, freq)
        sortino = cls.cal_sortino(asset_return, freq)
        treynor = cls.cal_treynor(asset_return, index_return, freq)
        jensen = cls.cal_jensen(asset_return, index_return, freq)
        m2 = cls.cal_m2(asset_return, index_return, freq)
        winning = cls.cal_winning(asset_return, index_return)
        mdd_month_number = cls.cal_mdd_month_number(asset_return)
        mdd_recovery_date = cls.cal_mdd_recovery_date(asset_return)
        mdd_recovery_months_number = cls.cal_mdd_recovery_months_number(asset_return)
        downside_vol = cls.cal_downside_vol(asset_return, freq)
        var = cls.cal_var(asset_return, cutoff)
        cvar = cls.cal_cvar(asset_return, cutoff)
        vol = cls.cal_annual_vol(asset_return, freq)
        tracking_error = cls.cal_tracking_error(asset_return, index_return, freq)
        benchmark_corr = cls.cal_benchmark_corr(asset_return, index_return)
        return np.stack([alpha, beta, sharpe, mdd, calmar, ir, sortino, treynor, jensen, m2, winning,
                         mdd_month_number, mdd_recovery_date, mdd_recovery_months_number, downside_vol,
                         var, cvar, vol, tracking_error, benchmark_corr], axis=-1)


class TopsisEngine(CalIndex):

    @classmethod
    def _topsis(cls, data, weight):
        """topsis计算引擎封装.

        :param data: DataFrame        e.g. \n
                                    a  b  c
                                0  1.0  2  3
                                1  1.0  4  6
                                2  1.0  1  1
        :param weight: 计算数据的"columns"的权重  没有权重的列不会被计算进最终结果  e.g. \n
                      pd.Series({"a":0.2,"b":0.2,"c":0.6})
        :return: 反应每行的数值大小情况的汇总  结果 0~1  单行每个数值都是最小时为"0" 最大为"1" e .g. \n
                 pd.Series({0: 0.3864744659301341, 1: 1.0, 2: 0.0})
        """
        square_sum = (data ** 2).sum(axis=0) ** 0.5
        norm_data = data / square_sum
        A = norm_data.max(axis=0)
        D = norm_data.min(axis=0)
        a = (weight * ((A - norm_data) ** 2)).sum(axis=1) ** 0.5
        b = (weight * ((D - norm_data) ** 2)).sum(axis=1) ** 0.5
        topsis_score = b / (a + b)
        return topsis_score

    @classmethod
    def _topsis_weight(cls, index_data, asset_return, method='mean'):
        weight = index_data.add(asset_return)
        return weight

    @classmethod
    def evaluate(cls, asset_return, index_return, weight_method='mean'):
        index_data = cls.cal_index(asset_return, index_return)
        index_weight = cls._topsis_weight(index_data, asset_return, weight_method)
        return cls._topsis(index_data, index_weight)





