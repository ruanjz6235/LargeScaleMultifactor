import numpy
from numpy import nan
from numpy.testing import assert_array_equal
from unittest import TestCase

from app.util import fill_ndarray, drop_nan_row_from_2darray


class FFill1dArrayTest(TestCase):

    def test_1d_array(self):
        result = fill_ndarray(numpy.array([numpy.nan, 1, numpy.nan, numpy.nan, 2, numpy.nan]), fill='f')
        assert_array_equal(numpy.array([numpy.nan, 1, 1, 1, 2, 2]), result)

        result = fill_ndarray(numpy.array([numpy.nan, 1, numpy.nan, numpy.nan, 2, numpy.nan]), fill='b')
        assert_array_equal(numpy.array([1, 1, 2, 2, 2, nan]), result)

    def test_2d_array_axis_1(self):
        result = fill_ndarray(
            numpy.array(
                [
                    [numpy.nan, 1, numpy.nan, numpy.nan, 2, numpy.nan],
                    [6, 1, numpy.nan, 3, 2, numpy.nan]
                ]),
            axis=1,
            fill='f')
        assert_array_equal(
            numpy.array([
                [numpy.nan, 1, 1, 1, 2, 2],
                [6, 1, 1, 3, 2, 2],
            ]),
            result)

        result = fill_ndarray(
            numpy.array(
                [
                    [numpy.nan, 1, numpy.nan, numpy.nan, 2, numpy.nan],
                    [6, 1, numpy.nan, 3, 2, numpy.nan]
                ]),
            axis=1,
            fill='b')
        assert_array_equal(
            numpy.array([
                [1, 1, 2, 2, 2, nan],
                [6, 1, 3, 3, 2, nan],
            ]),
            result)

    def test_2d_array_axis_0(self):
        result = fill_ndarray(numpy.array(
            [
                    [nan, 6.],
                    [1., nan],
                    [nan, nan],
                    [nan, 3.],
                    [2., 2.],
                    [nan, nan]
                ]),
            axis=0, fill='f')
        assert_array_equal(numpy.array([[nan, 6.], [1., 6.], [1., 6.], [1., 3.], [2., 2.], [2., 2.]]), result)

        result = fill_ndarray(numpy.array(
            [
                    [nan, 6.],
                    [1., nan],
                    [nan, nan],
                    [nan, 3.],
                    [2., 2.],
                    [nan, nan]
                ]),
            axis=0, fill='b')
        print(result)
        assert_array_equal(numpy.array([[1, 6.], [1., 3.], [2., 3.], [2., 3.], [2., 2.], [nan, nan]]), result)


class DropNaNRowFromNDArrayTest(TestCase):

    def test_2d_array_row(self):
        result = drop_nan_row_from_2darray(numpy.array(
            [
                [nan, 6.],
                [1., 1],
                [nan, nan],
                [nan, 3.],
                [2., 2.]
            ]))
        print(result)
        assert_array_equal(
            numpy.array(
                [
                    [1., 1],
                    [2., 2.]
                ]),
            result
        )
