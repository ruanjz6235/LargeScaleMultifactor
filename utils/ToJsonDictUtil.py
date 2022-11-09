#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2020/1/19 10:50 上午

from app.utils.util import np_array_to_json_dict, structured_array_to_json_dict
import pandas
import numpy


class ToJsonDictUtil:

    @classmethod
    def to_json_dict(cls, result):
        if type(result) in (int, float, str):
            return result
        if type(result) in (numpy.float64, numpy.int64):
            return result
        # list, set, dict
        if type(result) in (list, set):
            return [cls.to_json_dict(item) for item in result]

        if type(result) == dict:
            return {key: cls.to_json_dict(value) for key, value in result.items()}

        if type(result) == pandas.DataFrame:
            return {
                column: np_array_to_json_dict(result[column].values) for column in result.columns
            }

        if type(result) == numpy.ndarray:
            return structured_array_to_json_dict(result)

        if type(result) == numpy.float64:
            return result

        return result.__dict__