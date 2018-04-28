from collections import namedtuple, OrderedDict

from intake.source import base

import pandas as pd

dtypes = [('row', 'str'),
          ('column_family', 'str'),
          ('column_qualifier', 'str'),
          ('column_visibility', 'str'),
          ('time', 'datetime64[ms]'),
          ('value', 'object')]

KeyValue = namedtuple('KeyValue', [dtype[0] for dtype in dtypes])


class AccumuloSource(base.DataSource):
    def __init__(self, table, host, port, username, password, metadata=None):
        from pyaccumulo import Accumulo
        self._table = table
        self._connection = Accumulo(host=host,
                                    port=port,
                                    user=username,
                                    password=password)

        super(AccumuloSource, self).__init__(container='dataframe',
                                             metadata=metadata)

    def _get_schema(self):
        return base.Schema(datashape=None,
                           dtype=OrderedDict(dtypes),
                           shape=(None, len(dtypes)),
                           npartitions=1,
                           extra_metadata={})

    def _get_partition(self, i):
        data = []
        for entry in self._connection.scan(self._table):
            kv = KeyValue(entry.row,
                          entry.cf,
                          entry.cq,
                          entry.cv,
                          entry.ts,
                          entry.val)
            data.append(kv)
        df = pd.DataFrame(data, columns=KeyValue._fields)
        return df.astype(dtype=OrderedDict(dtypes))

    def _close(self):
        self._connection.close()
