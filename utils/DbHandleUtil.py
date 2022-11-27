#!/usr/bin/env python
# -*- coding: utf-8 -*-
from utils import DbUtil
import pandas as pd


class DbHandleUtil:

    @classmethod
    def save(cls, table_name=None, df=None):
        conn = DbUtil.get_conn()
        conn.autocommit(False)
        cursor = conn.cursor()
        try:
            df = df.where(pd.notnull(df), None)
            values = [tuple(xi) for xi in df.values]
            columns_arr = list(df)
            table_columns = ','.join(columns_arr)
            columns_value = ','.join(['%s'] * len(columns_arr))
            sql = 'replace {table_name} ({table_columns}) value ({columns_value})' \
                .format(table_name=table_name, table_columns=table_columns, columns_value=columns_value)
            cursor.executemany(sql, values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()