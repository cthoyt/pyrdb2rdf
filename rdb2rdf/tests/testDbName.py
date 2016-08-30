import sqlalchemy
import getpass
from sqlalchemy.engine import reflection

db_str = "postgresql://{}:@localhost:5432/test".format(getpass.getuser())
base_prefix = "base"
base_iri = "http://example.com/ns/"

engine = sqlalchemy.create_engine(db_str)

insp = reflection.Inspector.from_engine(engine)

for tname in engine.table_names():
    print(tname)
    print(insp.get_primary_keys(tname))

