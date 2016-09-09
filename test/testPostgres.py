import getpass
import logging
import sqlite3 as lite
import unittest

import rdflib
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

import rdb2rdf

log = logging.getLogger()

db_str = "postgresql://{}:@localhost:5432/test".format(getpass.getuser())
base_prefix = "base"
base_iri = "http://example.com/ns/"


class TestShit(unittest.TestCase):
    def setUp(self):
        # make sqllite db
        con = lite.connect(":memory:")
        with con:
            cur = con.cursor()
            cur.execute("CREATE TABLE Cars(Id INT, Name TEXT, Price INT)")
            cur.execute("INSERT INTO Cars VALUES(1, 'Audi', 52642)")

    def test_engine(self):
        engine = create_engine('sqlite://')

        insp = reflection.Inspector.from_engine(engine)

        for tname in engine.table_names():
            print(tname)
            print(insp.get_primary_keys(tname))

    def test_stuff1(self):
        engine = create_engine(db_str)

        insp = reflection.Inspector.from_engine(engine)

        for tname in engine.table_names():
            print(tname)
            print(insp.get_primary_keys(tname))

    def test_conn(self):
        engine = create_engine(db_str)

        # TODO use a mapper

        store = rdb2rdf.stores.DirectMapping(configuration=engine, base_iri=base_iri)
        graph = rdflib.Graph(store)
        graph.bind(base_prefix, base_iri)

        """
        insp = reflection.Inspector.from_engine(engine)
        for tname in engine.table_names():
            pkey = insp.get_primary_keys(tname)[0]
            #print(tname)
            #print()
            iri = "{}{}/{}=".format(base_iri, tname, pkey)
            graph.bind(tname, iri)
        """

        print(str(graph.serialize(format='turtle'), 'utf-8'))
