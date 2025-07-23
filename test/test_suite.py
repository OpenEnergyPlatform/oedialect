"""Import tests from internal test suite to run against custom dialect.
needs conftest.py (?)
"""

from sqlalchemy.testing.suite.test_dialect import AutocommitTest  # noqa
from sqlalchemy.testing.suite.test_insert import InsertBehaviorTest  # noqa
from sqlalchemy.testing.suite.test_insert import LastrowidTest  # noqa
from sqlalchemy.testing.suite.test_reflection import HasTableTest  # noqa
from sqlalchemy.testing.suite.test_reflection import NormalizedNameTest  # noqa; noqa
from sqlalchemy.testing.suite.test_results import PercentSchemaNamesTest  # noqa
from sqlalchemy.testing.suite.test_select import CollateTest  # noqa
from sqlalchemy.testing.suite.test_select import CompoundSelectTest  # noqa
from sqlalchemy.testing.suite.test_select import OrderByLabelTest  # noqa
from sqlalchemy.testing.suite.test_types import BooleanTest  # noqa
from sqlalchemy.testing.suite.test_types import DateHistoricTest  # noqa
from sqlalchemy.testing.suite.test_types import DateTest  # noqa
from sqlalchemy.testing.suite.test_types import DateTimeCoercedToDateTimeTest  # noqa
from sqlalchemy.testing.suite.test_types import DateTimeHistoricTest  # noqa
from sqlalchemy.testing.suite.test_types import DateTimeMicrosecondsTest  # noqa
from sqlalchemy.testing.suite.test_types import DateTimeTest  # noqa
from sqlalchemy.testing.suite.test_types import IntegerTest  # noqa
from sqlalchemy.testing.suite.test_types import JSONTest  # noqa
from sqlalchemy.testing.suite.test_types import NumericTest  # noqa
from sqlalchemy.testing.suite.test_types import StringTest  # noqa
from sqlalchemy.testing.suite.test_types import TextTest  # noqa
from sqlalchemy.testing.suite.test_types import TimeMicrosecondsTest  # noqa
from sqlalchemy.testing.suite.test_types import TimestampMicrosecondsTest  # noqa
from sqlalchemy.testing.suite.test_types import TimeTest  # noqa
from sqlalchemy.testing.suite.test_types import UnicodeTextTest  # noqa
from sqlalchemy.testing.suite.test_types import UnicodeVarcharTest  # noqa; noqa
from sqlalchemy.testing.suite.test_update_delete import SimpleUpdateDeleteTest  # noqa

# from sqlalchemy.testing.suite.test_dialect import EscapingTest #
# from sqlalchemy.testing.suite.test_dialect import ExceptionTest #
# from sqlalchemy.testing.suite.test_insert import ReturningTest #
# from sqlalchemy.testing.suite.test_sequence import HasSequenceTest #
# from sqlalchemy.testing.suite.test_reflection import ComponentReflectionTest #
# from sqlalchemy.testing.suite.test_sequence import SequenceTest #
# from sqlalchemy.testing.suite.test_select import LimitOffsetTest # Reason: Contains plain queries
# from sqlalchemy.testing.suite.test_select import ExpandingBoundInTest # TODO: Implement expansion
# from sqlalchemy.testing.suite.test_ddl import TableDDLTest # TODO: Uncomment
# from sqlalchemy.testing.suite.test_sequence import SequenceCompilerTest # TODO: Uncomment
# from sqlalchemy.testing.suite.test_results import RowFetchTest # TODO: Uncomment
# from sqlalchemy.testing.suite.test_results import ServerSideCursorsTest # TODO: Uncomment
# from sqlalchemy.testing.suite.test_select import LikeFunctionsTest # TODO: Uncomment
