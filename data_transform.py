import pandas as pd
import numpy as np
from const import nc


class DataTransform:
    def __init__(self, df: pd.DataFrame):
        super().__init__()
        self.df = df

    def __getattr__(self, item):
        if item not in self.__dict__.keys():
            return getattr(self.df, item)
        else:
            return self.__dict__[item]

    def __getitem__(self, item):
        try:
            return self.__dict__[item]
        except:
            return self.df[item]

    def __repr__(self):
        return self.df.__repr__()

    def get_dummy(self, columns=None):
        if not columns:
            columns = self.df[nc.style_name].drop_duplicates().tolist()
            columns = dict(zip(columns, columns))
        if self.df.empty:
            return pd.DataFrame(columns=[nc.code_name] + list(dict(columns).values()))
        self.df[nc.style_name] = self.df[nc.style_name].apply(lambda x: dict(columns)[x])
        df_ = pd.get_dummies(self.df[nc.style_name])
        self.df = pd.concat([self.df.drop(nc.style_name, axis=1), df_], axis=1)
        return self

    def rename(self, columns):
        self.df = self.df.rename(columns=columns)
        return self

    def clear_data(self, *conds):
        for cond in conds:
            self.df = self.df.query(cond)
        return self

    def get_df(self):
        return self.df

    def align(self, dfs: list, align_type='intersection', sort='nature'):
        dfs = [self.df] + dfs
        if any(len(df.shape) == 1 or 1 in df.shape for df in dfs):
            dims = 1
        else:
            dims = 2
        mut_index = sorted(reduce(lambda x, y: getattr(x, align_type)(y), (df.index for df in dfs)))
        mut_columns = sorted(reduce(lambda x, y: getattr(x, align_type)(y), (df.columns for df in dfs)))
        if align_type == 'intersection':
            if dims == 2:
                dfs = [df.loc[mut_index, mut_columns] for df in dfs]
            else:
                dfs = [df.loc[mut_index, :] for df in dfs]
        else:    # align_type = 'union' or 'left'
            if align_type == 'left':
                mut_columns, mut_index = dfs[0].columns, dfs[0].index
            if dims == 2:
                dfs = [
                    pd.concat(
                        [df, pd.DataFrame(columns=mut_columns[~mut_columns.isin(df.columns)],
                                          index=mut_index[~mut_index.isin(df.index)])
                         ]
                    ).loc[mut_index, mut_columns].fillna(0) for df in dfs
                ]
            else:
                dfs = [
                    pd.concat(
                        [pd.DataFrame(df), pd.DataFrame(columns=pd.DataFrame(df).columns,
                                                        index=mut_index[~mut_index.isin(df.index)])
                         ]
                    ).loc[mut_index, :].fillna(0) for df in dfs
                ]
        return dfs
