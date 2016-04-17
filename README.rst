#########
PyRDB2RDF
#########

PyRDB2RDF provides RDFLib_ with an interface to relational databases as
RDF_ stores_.  The underlying data is accessed via SQLAlchemy_.  It is
mapped to RDF according to the specifications of RDB2RDF_.  The
corresponding RDF graph is represented as an RDFLib graph_.

Translating from relational data to RDF via `direct mapping`_ is
currently supported.  Translating in the other direction and mapping
with R2RML_ are planned but not yet implemented.


.. _direct mapping: http://www.w3.org/TR/rdb-direct-mapping/

.. _graph:
    http://rdflib.readthedocs.org/en/latest/apidocs/rdflib.html#module-rdflib.graph

.. _R2RML: http://www.w3.org/TR/r2rml/

.. _RDB2RDF: http://www.w3.org/2001/sw/rdb2rdf/

.. _RDF: http://www.w3.org/TR/rdf11-concepts/

.. _RDFLib: http://rdflib.readthedocs.org/

.. _SQLAlchemy: http://www.sqlalchemy.org/

.. _stores: http://rdflib.readthedocs.org/en/latest/univrdfstore.html


************
Installation
************

.. code-block:: bash

    pip install https://github.com/cthoyt/pyrdb2rdf/archive/master.zip


********
Examples
********

Serializing a database as N-Triples
===================================

Suppose the local PostgreSQL_ database ``test_dm`` contains data that
we want to translate to RDF and serialize as N-Triples_.  The following
code achieves that.


.. _N-Triples: http://www.w3.org/TR/n-triples/

.. _PostgreSQL: http://www.postgresql.org/

.. code-block:: python

    db_str = "postgresql://testuser:testpass@localhost:5432/testdb"
    engine = sqlalchemy.create_engine(db_str)
    store = rdb2rdf.stores.DirectMapping(configuration=engine, base_iri=base_iri)
    graph = rdflib.Graph(store)
    print(str(graph.serialize(format='turtle'), 'utf-8'))

