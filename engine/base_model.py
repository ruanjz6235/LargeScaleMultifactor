#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/20 9:30 上午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : base_model.py
# @IDE     : PyCharm
# @REMARKS : 说明文字

import torch.nn as nn
from engine.base_engine import BaseEngine


class BaseModel(nn.Module):
    def __init__(self):
        super().__init__()

    def fit(self):
        pass























