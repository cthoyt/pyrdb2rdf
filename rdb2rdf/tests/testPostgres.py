import rdflib
import sqlalchemy
import rdb2rdf
import getpass
from sqlalchemy.engine import reflection

db_str = "postgresql://{}:@localhost:5432/test".format(getpass.getuser())
base_prefix = "base"
base_iri = "http://example.com/ns/"

engine = sqlalchemy.create_engine(db_str)

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


