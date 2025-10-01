# pytest --dburi=postgresql+dummydialect://test:test@localhost:5432 test/test_dummy.py
from sqlalchemy.testing.suite.test_insert import InsertBehaviorTest  # noqa
from sqlalchemy.testing.suite.test_insert import LastrowidTest  # noqa
from sqlalchemy.testing.suite.test_reflection import HasTableTest  # noqa
