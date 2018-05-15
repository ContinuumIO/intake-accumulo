Quickstart
==========

This guide will show you how to get started using Intake to read an Accumulo
table.


Installation
------------

For conda users, the Intake Accumulo plugin is installed with the following
commands::

  conda install -c intake intake-accumulo


Example: Reading Accumulo table without catalog
-----------------------------------------------

The simplest use case for this plugin is to read an existing Accumulo table.
Assuming the Accumulo instance is located at ``localhost:42424`` and the table
is in the variable, ``table``, this will read the entire table into a
dataframe.::

  >>> import intake
  >>> ds = intake.open_accumulo(table)
  >>> df = ds.read()
  >>> df
        row column_family column_qualifier column_visibility                    time value
  0   row_0           cf1              cq1                   2018-05-15 22:53:37.990     0
  1   row_0           cf2              cq2                   2018-05-15 22:53:38.009     0
  2   row_1           cf1              cq1                   2018-05-15 22:53:38.018     1
  3   row_1           cf2              cq2                   2018-05-15 22:53:38.026     1
  4   row_2           cf1              cq1                   2018-05-15 22:53:38.034     2
  5   row_2           cf2              cq2                   2018-05-15 22:53:38.042     2
  6   row_3           cf1              cq1                   2018-05-15 22:53:38.049     3
  7   row_3           cf2              cq2                   2018-05-15 22:53:38.057     3
  8   row_4           cf1              cq1                   2018-05-15 22:53:38.065     4
  9   row_4           cf2              cq2                   2018-05-15 22:53:38.072     4


Example: Reading Accumulo table with catalog
-----------------------------------------------

This example is equivalent to the above example, except we now access the table
through an existing catalog, ``catalog.yml``.::

  >>> from intake.catalog import Catalog
  >>> c = Catalog("catalog.yml")
  >>> df = c.basic.read()
  >>> df
        row column_family column_qualifier column_visibility                    time value
  0   row_0           cf1              cq1                   2018-05-15 22:53:37.990     0
  1   row_0           cf2              cq2                   2018-05-15 22:53:38.009     0
  2   row_1           cf1              cq1                   2018-05-15 22:53:38.018     1
  3   row_1           cf2              cq2                   2018-05-15 22:53:38.026     1
  4   row_2           cf1              cq1                   2018-05-15 22:53:38.034     2
  5   row_2           cf2              cq2                   2018-05-15 22:53:38.042     2
  6   row_3           cf1              cq1                   2018-05-15 22:53:38.049     3
  7   row_3           cf2              cq2                   2018-05-15 22:53:38.057     3
  8   row_4           cf1              cq1                   2018-05-15 22:53:38.065     4
  9   row_4           cf2              cq2                   2018-05-15 22:53:38.072     4
