#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/1 10:10 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : SelectUtil.py
# @IDE     : PyCharm
# @REMARKS :

from utils.DbUtil import DbUtil


class BaseSelect(DbUtil):

    @classmethod
    def get_dates(cls, **kwargs):
        last_date = kwargs.get('last_date')
        today = pd.to_datetime('today').normalize()
        if not kwargs.get('if_source'):
            return np.array([int(x.strftime('%Y%m%d')) for x in pd.date_range(last_date, today)])
        zjfund = cls.get_conn('zhijunfund')
        tar_con, sou_con = kwargs.get('tar_con', zjfund), kwargs.get('sou_con', zjfund)
        target, source = kwargs.get('target', 'T_CUST_D_STK_TRD_IDX'), kwargs.get('source', 'T_EVT_SEC_DLV_JOUR')
        date_name = kwargs.get('dt', 'etl_date')
        tar_dt, sou_dt = kwargs.get('tar_dt', date_name), kwargs.get('sou_dt', date_name)
        if not last_date:
            query = f"""select max({tar_dt}) from {target}"""
            last_date = pd.read_sql(query, tar_con)
            last_date = last_date.iloc[0].iloc[0]
            if last_date is None:
                last_date = '20150101'
            else:
                last_date = str(int(last_date))
        query = f"""select distinct {sou_dt} from {source} where {sou_dt} >= {last_date} order by {sou_dt} asc"""
        dates = pd.read_sql(query, sou_con)[sou_dt].astype('int').values
        return dates

    @classmethod
    # select data from table partitioned by date
    def get_days_data(cls, dates, query, **kwargs):
        days_data = []
        for date in dates:
            print(date)
            day_data = pd.read_sql(query.format(date=date, **kwargs), cls.get_conn())
            days_data.append(day_data)
        days_data = pd.concat(days_data)
        return days_data

    @classmethod
    def get_data(cls, query_name, **kwargs):
        """
        if select portfolio data by date，kwargs = {'query': 'fund_port', 'dates': dates,...}
        the key 'query' is required, but 'dates' is not necessary
        """
        dates = kwargs.get('dates')
        query = getattr(cls, query_name)
        # we need keep the return form consistent
        if not dates:
            return pd.read_sql(query.format(**kwargs), cls.get_conn())
        else:
            kwargs.pop('dates')
            return cls.get_days_data(dates, query, **kwargs)

    @classmethod
    def complete_df(cls, df, **kwargs):
        # qs是一个对象，columns=['query', 'conn', 'merge_on', 'merge_how']
        qs = kwargs.get('query')
        for query in qs:
            conn = cls.get_conn(query.get('conn', 'zhijunfund'))
            merge_on = query.get('merge_on', ['src_cust_no'])
            merge_how = query.get('merge_how', 'outer')
            cols = query.get('cols', {})
            data = pd.read_sql(query['query'], conn).rename(columns=cols)
            columns = data.columns
            df = df.merge(data, on=merge_on, how=merge_how)
            df[columns] = df[columns].ffill().bfill()
        return df
