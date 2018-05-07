from intake.source import base

from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


class Plugin(base.Plugin):
    """Plugin for Accumulo reader"""
    def __init__(self):
        super(Plugin, self).__init__(name='accumulo',
                                     version=__version__,
                                     container='dataframe',
                                     partition_access=False)

    def open(self, table, host="localhost", port=42424, username="root", password="secret", **kwargs):
        """
        Create AccumuloSource instance

        Parameters
        ----------
        table, host, port, username, password
            See ``AccumuloSource``.
        """
        from intake_accumulo.source import AccumuloSource
        base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
        return AccumuloSource(table=table,
                              host=host,
                              port=port,
                              username=username,
                              password=password,
                              metadata=base_kwargs['metadata'])
