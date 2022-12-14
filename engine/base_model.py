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


class BaseModel(BaseEngine):
    def __init__(self, model_type):
        super(BaseModel, self).__init__()
        self.model_type = model_type

    def load_data(self):
        pass

    def set_model(self):
        pass

    def exec(self):
        pass

    def out_data(self):
        pass























