#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/12/2 9:30 上午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : ret_attribution.py
# @IDE     : PyCharm
# @REMARKS : 说明文字


import pandas as pd
import numpy as np

from utils.DbUtil import DbUtil
from const import nc
from utils.SelectUtil import BaseSelect


def get_tradingdays(start_date, end_date):
    query = f"""select TradingDate from QT_TradingDayNew where IfTradingDay = 1 and SecuMarket = 83
    and TradingDate >= '{start_date}' and TradingDate >= '{end_date}'"""
    tradingdays = pd.read_sql(query, DbUtil.get_conn('zhijunfund'))
    tradingdays['TradingDate'] = tradingdays['TradingDate'].apply(lambda x: x.strftime('%Y%m%d'))
    return tradingdays['TradingDate'].unique()


class StyleSelect(BaseSelect):
    barra_style = """select * from FM_FactorExposure where date = {date}"""
    ind_style = """select secucode code, first_industry style from stock_industry where date = {date}
    and standard in ({standard})"""
    ms_style = """select * from stock_industry where date = {date} and standard in ({standard})"""


class RetSelect(BaseSelect):
    index_price = """
    """
    a_ret = """select code, date, simple_return ret from stock_daily_quote where date = '{date}'"""
    h_ret = """select secucode code, date, simple_return ret from stock_hk_daily_quote where date = '{date}'"""
    stock_ret = a_ret + ' union ' + h_ret
    index_ret = """
    """
    fund_ret = """select fund_id, price_date, cumulative_nav_withdrawal from demo01_nav
    where fund_id in ({codes})"""
    asset_ret = """
    """
    bond_ret = """
    """
    future_ret = """
    """


class PortSelect(BaseSelect):
    asset_port_jy_pub = """select secucode fund, report_date date, stock_code code, ratio_nv weight from fund_stockdetail
    where report_date = '{date}'"""
    asset_port_gzb = """select ZSCode, EndDate, SecuCode, MarketInNV, (
    case when FirstClassCode = '1102' then '股票'
         when FirstClassCode = '1103' then '债券'

         when FirstClassCode = '1105' and (SecuCode not in ({monetary_funds}) then '基金' else '现金-货币型基金')

         when FirstClassCode in ('3102', '3201') and ThirdClassCode = '01' and (SecondClassCode in (
         '01', '02', '03', '04', '05', '06', '07', '08', '31', '32') then '衍生品-期货' else '衍生品')
         when FirstClassCode in ('3102', '3201') and ThirdClassCode = '01' then '衍生品'
    ) style
    from ZS_FundValuation where EndDate = to_date('%s', 'yyyymmdd') and SecuCode is not null
    and FirstClassCode in ('1102', '1103', '1105', '3102', '3201')
    union  # 无需分辨个券资产
    (select ZSCode, EndDate, SecuCode, MarketInNV, (
    case when FirstClassCode = '1203' and SecondClassCode in ('01', '03', '06',
         '09', '11', '13', '14', '15', '17', '18', '23', '24') then '股票-应收股利'
         when FirstClassCode = '1203' and SecondClassCode = '05' and (
         SecuCode not in ({monetary_funds}) then '基金-分红' else '现金-货币型基金-分红')
         when FirstClassCode = '1204' and SecondClassCode = '10' then '债券-应收利息'
         when FirstClassCode = '1031' and SecondClassCode in ('13', '30', '32') then '衍生品-保证金'
         when FirstClassCode = '1204' and SecondClassCode = '14' then '衍生品-保证金-应收利息'
         when FirstClassCode = '1109' then '理财产品'
         when FirstClassCode = '1204' and SecondClassCode = '24' then '理财产品-应收利息'
         when FirstClassCode = '1002' then '现金-存款'
         when FirstClassCode = '1204' and SecondClassCode = '01' then '现金-存款-应收利息'
         when FirstClassCode = '1202' then '现金-逆回购'
         when FirstClassCode = '1204' and SecondClassCode = '91' then '现金-逆回购-应收利息'
         when FirstClassCode = '1021' then '现金-清算备付金'
         when FirstClassCode = '1204' and SecondClassCode = '02' then '现金-清算备付金-应收利息'
         when FirstClassCode = '1031' and SecondClassCode = '06' then '现金-券商保证金'
         when FirstClassCode = '1204' and SecondClassCode = '06' then '现金-券商保证金-应收利息'
         when FirstClassCode = '1031' and SecondClassCode = '51' then '现金-存出保证金信用账户'
         when FirstClassCode = '1204' and SecondClassCode = '15' then '现金-存出保证金信用账户-应收利息'
         when FirstClassCode = '1031' and SecondClassCode not in ('06', '13', '31', '32', '51') then '现金-其他保证金'
         when FirstClassCode = '3003' then '现金-证券清算款'
         when FirstClassCode = '1022' then '现金-证券清算款信用账户'
         when FirstClassCode = '1204' and SecondClassCode not in (
         '01', '02', '06', '10', '14', '15', '24', '91') then '现金-其他应收利息'
         else '其他'
         end) style
    )
    """
    funds_port = """select fund, date, code, weight, style from fund_portfolio where fund in ({funds})"""
    asset_port = """select fund, date, code, weight, style from fund_portfolio where date = '{date}'"""
    base_port = """select fund, date, code, weight, style from base_portfolio where date = '{date}'"""
    base_attr = """select code, date, code, weight, style from base_portfolio where date = '{date}'"""


class InfoSelect(BaseSelect):
    fund_codes = """select pfi.fund_id fund from pvn_fund_info pfi inner join pvn_fund_status pfs
    on pfi.fund_id = pfs.fund_id where pfi.raise_type = 1 and pfi.base_currency = 1 and pfi.isvalid = 1
    and pfs.fund_status in (1, 2, 3)"""
