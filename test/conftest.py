from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: needed for pytest --dburi

import oedialect  # noqa: calls registry.register() in oedialect.__init__.py
