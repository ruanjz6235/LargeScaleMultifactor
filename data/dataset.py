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


class PadDataset(Dataset):
    def __init__(self,
                 tokenizer,
                 origins,
                 ret_path,
                 bucket_size,
                 ret=None,
                 ascending=True):
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

    def __getitem__(self, index):
        while True:
            len_dates = torch.random
            if len_dates >= 0.3 * len(self.ret):
                break

    def __len__(self):
        return len(self.ret)


class MaskDataset(Dataset):
    def __init__(self, tokenizer, mask_path, mask_list, bucket_size, ascending=True):
        self.tokenizer = tokenizer
        self.bucket_size = bucket_size
        mask_name = [mp.split('/')[-1].split('.')[0] for mp in mask_path]
        for mn, mp in zip(mask_name, mask_path):
            if mn in mask_list:
                setattr(self, mn, read_data(mp))

    def __getitem__(self, index) -> T_co:
        pass
































