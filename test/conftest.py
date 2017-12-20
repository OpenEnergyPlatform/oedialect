from sqlalchemy.dialects import registry

registry.register("postgresql.oedialect", "oedialect.dialect", "OEDialect")

from sqlalchemy.testing.plugin.pytestplugin import *