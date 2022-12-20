#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/24 4:07 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : dataloader.py
# @IDE     : PyCharm
# @REMARKS : 说明文字
from torch.utils.data.dataset import T_co
from tqdm import tqdm
from pathlib import Path
from os.path import join, getsize
from joblib import Parallel, delayed
from torch.utils.data import Dataset, DataLoader
import torch
import feather
import pandas as pd
import numpy as np
import random
from sklearn.utils.random import check_random_state
from data_transform import DataTransform
from data.process import PreProcess


def collate_process_batch(batch, pre_process):
    asset_feat, asset_len, origin = [], [], []
    with torch.no_grad():
        for b in batch:
            feat = pre_process(b)
            asset_feat.append(feat)
            asset_len.append(len(feat))
            origin.append(torch.LongTensor(b))
    asset_len, asset_feat, origin = zip(*[
        (feat_len, feat, txt) for feat_len, feat, orig in
        sorted(zip(asset_len, asset_feat, origin), reverse=True, key=lambda x:x[0])])
    # Zero-padding
    asset_feat = pad_sequence(asset_feat, batch_first=True)
    origin = pad_sequence(origin, batch_first=True)
    return asset_feat, asset_len, origin


if __name__ == '__main__':
    kwargs = {}
    from data.dataset import datasets_class, mix_wrapper
    mix_dataset = mix_wrapper(*datasets_class, **kwargs)
    collate_fn = partial(collate_process_batch, audio_transform=PreProcess())
    mix_dataset = DataLoader(mix_dataset, drop_last=drop_last, collate_fn=collect_tr, num_workers=n_jobs)
    for train_data in mix_dataset:
        print(train_data)

