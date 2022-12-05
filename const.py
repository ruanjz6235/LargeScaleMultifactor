import pandas as pd
import numpy as np
from dataclasses import dataclass


class NameConst:
    fund_name = 'fund'
    code_name = 'code'
    flag_name = 'flag'
    type_name = 'type'
    hold_type_name = 'hold_type'

    date_name = 'date'
    time_name = 'time'
    price_name = 'price'
    close_name = 'close'
    preclose_name = 'preclose'
    volume_name = 'volume'

    hold_name = 'holding'
    days_name = 'days'
    dur_name = 'duration'
    hold_mv_name = 'holding_mv'
    count_name = 'count'
    codes_nm = ['fund', 'code']
    fund_date_nm = ['fund', 'date']
    realized_nm = ['gx', 'rn', 'dx', 'hg', 'pg', 'qz', 'pt']
    realize_nm = ['pt_realize', 'hg_realize', 'dx_realize', 'qz_realize', 'pg_realize']
    hold_nm = ['holding', 'duration']
    turn_nm = ['buy_turn', 'sell_turn', 'turnover']

    style_name = 'style'
    style_type_name = 'style_type'
    weight_name = 'weight'
    ret_name = 'ret'
    va_name = 'va'
    nv_name = 'nv'
    cum_nv_name = 'cum_nv'
    complex_nv_name = 'complex_nv'
    label_name = 'label'
    mul_name = 'mul'

    today = pd.to_datetime('today').normalize()
    date_dict = {'since_found': '1970-01-01',
                 'five_years': (today - pd.DateOffset(years=5)).strftime('%Y-%m-%d'),
                 'three_years': (today - pd.DateOffset(years=3)).strftime('%Y-%m-%d'),
                 'two_years': (today - pd.DateOffset(years=2)).strftime('%Y-%m-%d'),
                 'one_year': (today - pd.DateOffset(years=1)).strftime('%Y-%m-%d'),
                 'year_start': str(today.year - 1) + '-12-31'}
    date_label = list(date_dict.keys())


nc = NameConst()
