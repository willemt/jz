.. image:: https://img.shields.io/badge/rsttst-testable-brightgreen.svg
   :target: https://github.com/willemt/rsttst

*WARNING: Experimental & just for fun*

jz: Simple JSON database presented as a HTTP REST server

Goals:

* Durability (HTTP response means the write has been persisted to disk)
* Column schema (the most efficient index by datatype)
* LMDB (zero copy and fast reads)
* Minimise time-to-first-row (online aggregation)
* GROUP BY CUBE support (pivot tables)
* One big table (NoSQL)
* SQL-like querying
* Document tagging
* Multi-tenancy

Inspired by the following papers:

* `Hellerstein etal, Online Aggregation <http://db.cs.berkeley.edu/cs286/papers/ola-sigmod1997.pdf>`_
* `Neumann, Efficiently Compiling Efficient Query Plans for Modern Hardware <http://www.vldb.org/pvldb/vol4/p539-neumann.pdf>`_
* `Krikellas, The case for holistic query evaluation <http://homepages.inf.ed.ac.uk/mc/Publications/krikellas_thesis.pdf>`_
* `Bigtable: A Distributed Storage System for Structured Data <https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf>`_
* `HyperLogLog in Practice: Algorithmic Engineering of a State of The Art Cardinality Estimation Algorithm <http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf>`_


How do I use this?
==================

All examples below make use of the excellent `httpie <https://github.com/jakubroztocil/httpie>`_

Start the server
----------------
.. code-block:: bash

   sudo ./jz/jz.py -z

.. code-block:: bash

   pidfile is: /var/run/jz.pid
   daemonizing...


Add index
---------
Indices allow you to do range queries.

.. code-block:: bash

   http -h --ignore-stdin POST 127.0.0.1:8888/index/ name=x datatype=uint64
   http -h --ignore-stdin POST 127.0.0.1:8888/index/ name=y datatype=uint64

.. code-block:: http
   :class: dotted

   HTTP/1.1 200 OK
   Content-Language: en-us
   Content-Type: application/json
   Content-Length: 2
   Server: jz/0.1.0
   
   HTTP/1.1 200 OK
   Content-Language: en-us
   Content-Type: application/json
   Content-Length: 2
   Server: jz/0.1.0


POST JSON document
------------------
Post a JSON document with dictionary containing a key "x" with value 10.

.. code-block:: bash

   echo '{"x": 10}' | http -h POST 127.0.0.1:8888/

.. code-block:: http
   :class: dotted

   HTTP/1.1 200 OK
   Content-Language: en-us
   Content-Type: application/json
   Content-Length: 2
   Server: jz/0.1.0

GET JSON documents using JSON query
-----------------------------------
Retrieve a list of documents.

.. code-block:: bash

   echo 'WHERE 0 < x' | http GET 127.0.0.1:8888/

.. code-block:: json

   [{"x": 10}
   ]

GET JSON documents using multiple clause JSON query
---------------------------------------------------

.. code-block:: bash

   echo '{"x": 10}' | http POST 127.0.0.1:8888/
   echo '{"x": 20, "y": 50}' | http POST 127.0.0.1:8888/
   echo '{"x": 70, "y": 90}' | http POST 127.0.0.1:8888/
   echo '{"x": 30, "y": 40}' | http POST 127.0.0.1:8888/
   echo 'WHERE x > 25 AND 60 < y' | http -b GET 127.0.0.1:8888/

.. code-block:: json

   [{"x": 70, "y": 90}
   ]


Shutdown server
---------------
.. code-block:: bash

   sudo cat /var/run/jz.pid | sudo xargs kill
   echo Done!

.. code-block:: bash

   Done!


Security
========
jz does not implement SSL/TLS. You will need to use a SSL terminator (eg. ngnix)


TODO
====

* Add sargable iterators
* Multi-vendor
* GROUP BY
