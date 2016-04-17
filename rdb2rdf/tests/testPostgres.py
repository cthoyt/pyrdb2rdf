import rdflib
import sqlalchemy
import rdb2rdf
import getpass

db_str = "postgresql://{}:@localhost:5432/test".format(getpass.getuser())
base_prefix = "base"
base_iri = "http://example.com/ns/"

db = sqlalchemy.create_engine(db_str)
# print(db.table_names())

store = rdb2rdf.stores.DirectMapping(configuration=db, base_iri=base_iri)
graph = rdflib.Graph(store)
graph.bind(base_prefix, base_iri)

print(str(graph.serialize(format='turtle'), 'utf-8'))
