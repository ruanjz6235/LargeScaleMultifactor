import numpy


def format_dataframe(dataframe):
    dtypes = dataframe.dtypes
    # formatted_dtypes = [dtypes[name] for name in dtypes.index.values]
    return """
        pandas.DataFrame(
            {{
                {series}
            }},
        )
        
    """.format(series=',\n'.join([formate_series(dataframe[column]) for column in dataframe.columns]))


def formate_series(series):
    if series.dtype == numpy.dtype('float64'):
        formatted_values = ','.join([str(value) for value in series.values])
    else:
        formatted_values = ','.join(["'%s'" % value for value in series.values])
    return """
        '{name}': pandas.Series([{elements}], dtype=dtype('{dtype}'))
    """.format(name=series.name, elements=formatted_values, dtype=series.dtype)
