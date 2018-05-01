import pytest

import intake_accumulo as accumulo

from .utils import verify_plugin_interface, verify_datasource_interface


@pytest.fixture(scope="module")
def proxy():
    from pyaccumulo import Accumulo, Mutation, Range
    from .utils import start_proxy, stop_proxy

    stop_proxy(let_fail=True)
    local_port = start_proxy()

    c = Accumulo(host="localhost", port=int(local_port), user="root", password="secret")

    table = "test"
    if not c.table_exists(table):
        c.create_table(table)

    for num in range(0, 5):
        m = Mutation("row_%d"%num)
        m.put(cf="cf1", cq="cq1", val="%d"%num)
        m.put(cf="cf2", cq="cq2", val="%d"%num)
        c.write(table, m)

    try:
        yield local_port
    finally:
        stop_proxy()


def test_plugin():
    plugin = accumulo.Plugin()
    assert isinstance(plugin.version, str)
    assert plugin.container == 'dataframe'
    verify_plugin_interface(plugin)


def test_open(proxy):
    plugin = accumulo.Plugin()
    src = plugin.open("test")
    assert src.container == 'dataframe'
    assert src.description is None
    verify_datasource_interface(src)


def test_discover(proxy):
    plugin = accumulo.Plugin()
    src = plugin.open("test")
    info = src.discover()
    assert info['shape'] == (None, 6)
    assert info['npartitions'] == 1


def test_read(proxy):
    plugin = accumulo.Plugin()
    src = plugin.open("test")
    df = src.read()
    assert len(df) == 10


def test_close(proxy):
    plugin = accumulo.Plugin()
    src = plugin.open("test")
    src.close()
