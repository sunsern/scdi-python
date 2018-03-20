.. SCDI documentation master file, created by
   sphinx-quickstart on Mon Mar 19 12:02:44 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to SCDI's documentation!
================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

.. automodule:: pyscdi
 :members:

Connecting to SCDI
===================
.. autoclass:: pyscdi.Scdi
 :members:

Bucket Types
============

There are currently 4 buckets types.

KWS Bucket
----------
.. autoclass:: pyscdi.Kws
 :members:
 :inherited-members:

Timeseries Bucket
-----------------
.. autoclass:: pyscdi.Timeseries
  :members:
  :inherited-members:

Geotemporal Bucket
------------------
.. autoclass:: pyscdi.Geotemporal
  :members:
  :inherited-members:

Keyvalue Bucket
---------------
.. autoclass:: pyscdi.Keyvalue
  :members:
  :inherited-members:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
