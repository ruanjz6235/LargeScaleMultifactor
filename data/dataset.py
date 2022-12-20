#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/24 4:06 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : dataset.py
# @IDE     : PyCharm
# @REMARKS : 说明文字
from torch.utils.data.dataset import T_co
from tqdm import tqdm
from pathlib import Path
from os.path import join, getsize
from joblib import Parallel, delayed
from torch.utils.data import Dataset
import torch
import feather
import pandas as pd
import numpy as np
import random
from sklearn.utils.random import check_random_state
from data_transform import DataTransform
from pyarrow.filesystem import FileSystem as fs
spark = fs()


# Additional (official) text src provided
OFFICIAL_TXT_SRC = ['librispeech-lm-norm.txt']
# Remove longest N sentence in librispeech-lm-norm.txt
REMOVE_TOP_N_TXT = 5000000
# Default num. of threads used for loading LibriSpeech
READ_FILE_THREADS = 4


def read_data(path):
    if path.split('.')[-1] == 'csv':
        return pd.read_csv(path, encoding='utf-8')
    elif path.split('.')[-1] == 'feather':
        return feather.read_dataframe(path)
    elif path.split('.')[-1] == 'parquet':
        return spark.read_parquet(path)
    else:
        with open(path, encoding='utf-8') as f:
            lst = f.readlines()
            data = pd.DataFrame(lst[1:], columns=lst[0])
            return data


def mix_wrapper(*classes, **kwargs):
    class MixClass(*classes):
        def __init__(self, **key_words):
            for sub_class in classes:
                try:
                    sub_class.__init__(**key_words)
                    # super(MixClass, self).__init__(**kwargs)
                except KeyError:
                    raise KeyError(f"""classes in mix fund has different params,
                        key words has no {sub_class.__name__} params""")

        def __getitem__(self, item):
            super(MixClass, self).__getitem__(item)

    mix_class = MixClass(**kwargs)
    return mix_class


class PadDataset(Dataset):
    def __init__(self,
                 tokenizer,
                 ret_path,
                 bucket_size,
                 ret=None,
                 *args,
                 **kwargs):
        self.tokenizer = tokenizer
        self.bucket_size = bucket_size

        for origin in origins:
            if not isinstance(origin, pd.DataFrame):
                raise TypeError('origin data is not a dataframe, please check out origin data')

        if ret is None:
            ret = read_data(ret_path)

        if isinstance(ret, pd.DataFrame):
            self.codes = list(ret.columns)
            self.dates = list(ret.index)
        else:
            raise TypeError('ret data is not a dataframe, please check out ret data')

        self.ret, self.origins = DataTransform(ret).align(dfs=origins, align_type='left')
        self.ret, self.origins = torch.tensor(self.ret.values), [torch.tensor(origin.values) for origin in self.origins]
        self.args = args
        self.kwargs = kwargs

    def __getitem__(self, index):
        while True:
            len_dates = np.random.randint(len(self.ret))
            if len_dates >= 0.3 * len(self.ret):
                dates_index = np.array(random.sample(range(len(self.ret)), len_dates))
                dates_index.sort()
                ret_sample = self.ret[dates_index]
                origins_sample = [origin[dates_index] for origin in self.origins]
                return ret_sample, origins_sample

    def __len__(self):
        return len(self.ret)


class MaskDataset(Dataset):
    def __init__(self,
                 tokenizer,
                 mask_path,
                 mask_list,
                 bucket_size,
                 *args,
                 **kwargs
                 ):
        self.tokenizer = tokenizer
        self.bucket_size = bucket_size

        self.mask = None
        mask_name = [mp.split('/')[-1].split('.')[0] for mp in mask_path]
        for mn, mp in zip(mask_name, mask_path):
            if mn in mask_list:
                mask = DataTransform(read_data(mp).set_index([nc.date_name, nc.code_name])).get_dummy().get_df()
                mask = mask.where(mask.isin(mask_rule[1]), 1)
                mask = mask.where(~mask.isna(), 0)
                setattr(self, mn, mask)
                self.mask = self.add(self.mask, getattr(self, mn))
        self.mask = np.exp(self.mask.where(self.mask > 0, - np.inf)).T
        self.args = args
        self.kwargs = kwargs

    @classmethod
    def add(cls, df1, df2):
        column1 = df1.columns[~df1.columns.isin(df2.columns)]
        column2 = df2.columns[~df2.columns.isin(df1.columns)]
        new1 = pd.DataFrame(np.ones(shape=(len(df1), len(column2))), columns=column2, index=df1.index)
        new2 = pd.DataFrame(np.ones(shape=(len(df2), len(column1))), columns=column1, index=df2.index)
        df1_new = pd.concat([df1, new1], axis=1)
        df2_new = pd.concat([df2, new2], axis=1)
        return df1_new + df2_new

    def __getitem__(self, index):
        while True:
            len_dates = np.random.randint(len(self.ret))
            if len_dates >= 0.3 * len(self.ret):
                dates_index = np.array(random.sample(range(len(self.ret)), len_dates))
                dates_index.sort()
                ret_sample = self.ret[dates_index]
                origins_sample = [origin[dates_index] for origin in self.origins]
                return ret_sample, origins_sample

    def __len__(self):
        return len(self.ret)


if __name__ == '__main__':
    datasets_name = [x for x in globals().keys() if x.endswith('Dataset') and x != 'Dataset']
    datasets_class = [globals()[x] for x in datasets_name]
    keys = {}
    mix_wrapper(*datasets_class, **keys)

