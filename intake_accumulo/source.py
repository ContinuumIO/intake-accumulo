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
    """Read data from Accumulo table.

    Parameters
    ----------
    table : str
        The database table that will act as source
    host : str
        The server hostname for the given table
    port : int
        The server port for the given table
    username : str
        The username used to connect to the Accumulo cluster
    password : str
        The password used to connect to the Accumulo cluster
    """

    def __init__(self, table, host, port, username, password, metadata=None):
        from .accumulo import Accumulo

        self._client = Accumulo(host, port, username, password)
        self._table = table

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
        scanner = self._client.create_scanner(self._table)

        while True:
            chunk = self._client.nextk(scanner)
            for entry in chunk.results:
                kv = KeyValue(entry.key.row,
                              entry.key.colFamily,
                              entry.key.colQualifier,
                              entry.key.colVisibility,
                              entry.key.timestamp,
                              entry.value)
                data.append(kv)
            if not chunk.more:
                break

        df = pd.DataFrame(data, columns=KeyValue._fields)
        return df.astype(dtype=OrderedDict(dtypes))

    def _close(self):
        self._client.close()
