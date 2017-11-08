from sqlalchemy.dialects import registry
from psycopg2 import *

registry.register("postgresql.oedialect", "oedialect.dialect", "OEDialect")