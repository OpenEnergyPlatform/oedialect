from psycopg2 import *
from sqlalchemy.dialects import registry

registry.register("postgresql.oedialect", "oedialect.dialect", "OEDialect")
