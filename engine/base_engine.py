#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/30 6:04 下午
# @Author  : ruanjz
# @project : Trace
# @Email   : ruanjz6235@163.com
# @File    : base_engine.py
# @IDE     : PyCharm
# @REMARKS : 说明文字
# 以下四个模块均要实现向量化和GPU才能达到大规模自动挖掘因子的效果

# 模型，forward函数为主：forward函数内容有，回归，随机森林，xgboost，遗传算法，小波分析，傅里叶，模拟退火，粒子群，transformer等

# 数据处理，数据pipeline：mask预处理，score预处理，ret预处理，收益归因中的dummy预处理，
# 包括补全code和补全tradingday及简单的因子处理，最后做成pipeline的形式

# 回测，分层回测和单期回测，其他可以继续添加：得到收益曲线

# 评价：topsis模型评价，收益归因，Barra
from abc import ABC, abstractmethod
from copy import deepcopy
import torch
from torch import nn
from sklearn.base import BaseEstimator

# from backtest.backtest_gpu import *
from backtest.backtest_vec import *
from evaluate import MetricEngine
from query.ret_attribution import RetSelect
from src.option import default_hparas


class BaseEngine(nn.Module):
    def __init__(self):
        super().__init__()
        for k, v in default_hparas.items():
            setattr(self, k, v)
        self.device = torch.device('cuda') if self.paras.gpu and torch.cuda.is_available() else torch.device('cpu')
        self.max_port = None
        self.max_ret = None

    def backward(self, loss):
        """
        Standard backward step with self.timer and debugger
        Arguments
            loss - the loss to perform loss.backward()
        """
        self.timer.set()
        loss.backward()
        grad_norm = torch.nn.utils.clip_grad_norm_(self.model.parameters(), self.GRAD_CLIP)
        if math.isnan(grad_norm):
            self.verbose('Error : grad norm is NaN @ step '+str(self.step))
        else:
            self.optimizer.step()
        self.timer.cnt('bw')
        return grad_norm

    def fit_gp(self,
               X: torch.tensor,
               sample_weight=None,
               *args,
               **kwargs):
        """genetic programming"""
        x = deepcopy(X)
        for sub_args in args:
            if not isinstance(sub_args, (tuple, list, np.array, torch.tensor)):
                sub_args = (sub_args, )
            x = self.forward(x, sample_weight, *sub_args, **kwargs)

    def fit_ml(self,
               X: torch.tensor,
               sample_weight=None,
               max_n=10,
               *args,
               **kwargs):
        """machine learning"""
        score = self.forward(X, sample_weight, *args, **kwargs)
        ret = RetSelect.get_data(RetSelect.stock_ret, codes=x[:, 1].unique(), dates=x[:, 0].unique())
        index_return = RetSelect.get_data(RetSelect.index_ret, codes='000300')
        metric = MetricEngine()
        fitness = metric(score, ret, index_return)
        self.max_port = self.ports[fitness.argsort() > len(fitness) - 1 - max_n]
        self.max_ret = metric.asset_return[fitness.argsort() > len(fitness) - 1 - max_n]

    def fit_dl(self,
               X: torch.tensor,
               sample_weight=None,
               *args,
               **kwargs):
        """deep learning"""
        score = self.forward(X, sample_weight, *args, **kwargs)
        ret = RetSelect.get_data(RetSelect.stock_ret, codes=x[:, 1].unique(), dates=x[:, 0].unique())
        index_return = RetSelect.get_data(RetSelect.index_ret, codes='000300')
        metric = MetricEngine()
        fitness = metric(score, ret, index_return)
        grad_norm = self.backward(fitness)
        return grad_norm

    def fit_esb(self,
                X: torch.tensor,
                sample_weight=None,
                *args,
                **kwargs):
        """ensemble"""
        pass

    @abc.abstractmethod
    def load_data(self):
        """
        Called by main to load all data
        After this call, data related attributes should be setup (e.g. self.tr_set, self.dev_set)
        No return value
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_model(self):
        """
        Called by main to set models
        After this call, model related attributes should be setup (e.g. self.l2_loss)
        The followings MUST be setup
            - self.model (torch.nn.Module)
            - self.optimizer (src.Optimizer),
                init. w/ self.optimizer = src.Optimizer(self.model.parameters(),**self.config['hparas'])
        Loading pre-trained model should also be performed here
        No return value
        """
        raise NotImplementedError

    @abc.abstractmethod
    def exec(self):
        """
        Called by main to execute training/inference
        """
        raise NotImplementedError

    def load_ckpt(self):
        """ Load ckpt if --load option is specified """
        if self.paras.load:
            # Load weights
            ckpt = torch.load(self.paras.load, map_location=self.device if self.mode == 'train' else 'cpu')
            self.model.load_state_dict(ckpt['model'])
            if self.emb_decoder is not None:
                self.emb_decoder.load_state_dict(ckpt['emb_decoder'])
            # if self.amp:
            #    amp.load_state_dict(ckpt['amp'])
            # Load task-dependent items
            metric = "None"
            score = 0.0
            for k, v in ckpt.items():
                if type(v) is float:
                    metric, score = k, v
            if self.mode == 'train':
                self.step = ckpt['global_step']
                self.optimizer.load_opt_state_dict(ckpt['optimizer'])
                self.verbose('Load ckpt from {}, restarting at step {} (recorded {} = {:.2f} %)'.format(
                              self.paras.load, self.step, metric, score))
            else:
                self.model.eval()
                if self.emb_decoder is not None:
                    self.emb_decoder.eval()
                self.verbose('Evaluation target = {} (recorded {} = {:.2f} %)'.format(self.paras.load, metric, score))

    def verbose(self, msg):
        """ Verbose function for print information to stdout"""
        if self.paras.verbose:
            if type(msg) == list:
                for m in msg:
                    print('[INFO]', m.ljust(100))
            else:
                print('[INFO]', msg.ljust(100))

    def progress(self, msg):
        """ Verbose function for updating progress on stdout (do not include newline) """
        if self.paras.verbose:
            sys.stdout.write("\033[K")  # Clear line
            print('[{}] {}'.format(human_format(self.step), msg), end='\r')

    def write_log(self, log_name, log_dict):
        """
        Write log to TensorBoard
            log_name  - <str> Name of tensorboard variable
            log_value - <dict>/<array> Value of variable (e.g. dict of losses), passed if value = None
        """
        if type(log_dict) is dict:
            log_dict = {key: val for key, val in log_dict.items() if (
                val is not None and not math.isnan(val))}
        if log_dict is None:
            pass
        elif len(log_dict) > 0:
            if 'align' in log_name or 'spec' in log_name:
                img, form = log_dict
                self.log.add_image(
                    log_name, img, global_step=self.step, dataformats=form)
            elif 'text' in log_name or 'hyp' in log_name:
                self.log.add_text(log_name, log_dict, self.step)
            else:
                self.log.add_scalars(log_name, log_dict, self.step)

    def save_checkpoint(self, f_name, metric, score, show_msg=True):
        """
        Ckpt saver
            f_name - <str> the name phnof ckpt file (w/o prefix) to store, overwrite if existed
            score  - <float> The value of metric used to evaluate model
        """
        ckpt_path = os.path.join(self.ckpdir, f_name)
        full_dict = {
            "model": self.model.state_dict(),
            "optimizer": self.optimizer.get_opt_state_dict(),
            "global_step": self.step,
            metric: score
        }
        # Additional modules to save
        # if self.amp:
        #    full_dict['amp'] = self.amp_lib.state_dict()
        if self.emb_decoder is not None:
            full_dict['emb_decoder'] = self.emb_decoder.state_dict()

        torch.save(full_dict, ckpt_path)
        if show_msg:
            self.verbose("Saved checkpoint (step = {}, {} = {:.2f}) and status @ {}".
                         format(human_format(self.step), metric, score, ckpt_path))

    def enable_apex(self):
        if self.amp:
            # Enable mixed precision computation (ToDo: Save/Load amp)
            from apex import amp
            self.amp_lib = amp
            self.verbose(
                "AMP enabled (check https://github.com/NVIDIA/apex for more details).")
            self.model, self.optimizer.opt = self.amp_lib.initialize(
                self.model, self.optimizer.opt, opt_level='O1')
