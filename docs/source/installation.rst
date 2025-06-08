Installation
============

Installing PySpark Ingestion Framework
--------------------------------------

There are several ways to install the PySpark Ingestion Framework depending on your needs:

Using pip
~~~~~~~~~

To install the latest release version:

.. code-block:: bash

   pip install pyspark-ingestion-framework

For Development
~~~~~~~~~~~~~~

For development purposes, clone the repository and install using Poetry:

.. code-block:: bash

   git clone https://github.com/username/pyspark-ingestion-framework.git
   cd pyspark-ingestion-framework
   poetry install --with=dev,test

Requirements
-----------

The PySpark Ingestion Framework requires:

* Python 3.8 or higher
* Apache PySpark 3.0 or higher
* Poetry (for development)

Optional Dependencies
-------------------

For development and testing, additional dependencies can be installed:

.. code-block:: bash

   # Install development dependencies
   poetry install --with=dev
   
   # Install test dependencies
   poetry install --with=test
   
   # Install documentation dependencies
   poetry install --with=docs
