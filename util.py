import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
import time
import numpy
import uuid


def gen_uuid():
    uid = str(uuid.uuid4())
    uid = ''.join(uid.split('-'))
    return uid


def str_to_date(d):
    if not d:
        return None

    return datetime.strptime(d, '%Y-%m-%d').date()


def date_to_str(d):
    return d.strftime('%Y-%m-%d') if d is not None else None


def datetime_to_str(d):
    return d.strftime('%Y-%m-%d %H:%M:%S') if d is not None else None


def datetime64_to_date_str(d):
    return d.astype(date).strftime('%Y-%m-%d') if d is not None else None


def datetime64_to_datetime_str(d):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(d.astype(int) / 1000000000)) if d is not None else None


def datetime64_to_month_str(d):
    return d.astype(date).strftime('%Y-%m') if d is not None else None


def uuid_to_str(u):
    if not u:
        return None

    return str(u)


def str_to_uuid(s):
    if not s:
        return None

    return uuid.UUID(s)


def array_drop_na(array):
    return array[~numpy.isnan(array)]


def structured_array_equal(array1, array2):
    if len(array1) != len(array2):
        return False

    if isinstance(array1, list) or isinstance(array1, tuple):
        for element1, element2 in zip(array1, array2):
            if not _structured_array_equal(element1, element2):
                return False
        return True

    return _structured_array_equal(array1, array2)


def _structured_array_equal(array1, array2):
    if array1.dtype != array2.dtype:
        return False

    if array1.shape != array2.shape:
        return False

    for column in array1.dtype.names:
        if column == 'DATE':
            if not numpy.all(array1[column] == array2[column]):
                return False
            continue

        if not numpy.allclose(array1[column], array2[column], equal_nan=True):
            return False

    return True


def to_json_dict_float(f):
    if f is None or f == float('nan') or numpy.isnan(f):
        return None

    result = float(f)

    if -1e-6 < result < 1e-6:
        return 0.0

    return result


def structured_array_to_json_dict(array):
    if array is None:
        return None

    values = {}
    if isinstance(array, list) or isinstance(array, tuple):
        for element in array:
            _structured_array_to_json_dict(element, values)

        return values

    return _structured_array_to_json_dict(array, values)


def _structured_array_to_json_dict(array, result):
    if array is None:
        return result

    column_names = array.dtype.names
    result.update({column_name: _array_column_to_json_dict(array[column_name]) for column_name in column_names})
    return result


def to_json_dict_int(i):
    return int(i)


def np_array_to_json_dict(array):
    return _array_column_to_json_dict(array)


def _array_column_to_json_dict(column):
    converter = lambda x: x
    dtype = column.dtype

    if dtype == numpy.dtype('f8'):
        converter = to_json_dict_float
    elif dtype == numpy.dtype('M8[D]'):
        converter = datetime64_to_date_str
    elif dtype == numpy.dtype('M8[M]'):
        converter = datetime64_to_month_str
    elif dtype == numpy.dtype('M8[ns]'):
        converter = datetime64_to_datetime_str
    elif dtype == numpy.dtype('i4') or dtype == numpy.dtype('i8'):
        converter = to_json_dict_int

    return [converter(x) for x in column]


_POOL = ThreadPoolExecutor(max_workers=2)


class CoroutineSyncExecutor:

    def __init__(self, coroutine):
        self.coroutine = coroutine
        self.loop = asyncio.new_event_loop()

    def execute(self):
        try:
            future = _POOL.submit(self._wrapper)
            return future.result()
        finally:
            self.loop.close()

    def _wrapper(self):
        return self.loop.run_until_complete(self.coroutine)


def fill_ndarray(array, axis=0, fill='f'):
    assert 0 <= axis <= 1
    assert array.ndim <= 2

    if axis == 0:
        array = array.T

    array = numpy.flip(array) if fill == 'b' else array

    mask = numpy.isnan(array)
    if mask.all():
        return array

    idx = numpy.where(~mask, numpy.arange(mask.shape[-1]), 0)
    numpy.maximum.accumulate(idx, axis=(len(array.shape) - 1), out=idx)
    out = array[numpy.arange(idx.shape[0])[:, None], idx] if array.ndim == 2 else array[idx]

    out = numpy.flip(out) if fill == 'b' else out

    out = out.T if axis == 0 else out

    return out


def drop_nan_row_from_2darray(array):
    mask = ~numpy.isnan(array)
    mask = numpy.all(mask, axis=(array.ndim - 1))
    return array[mask]
