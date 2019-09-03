# test/test_suite.py

from sqlalchemy.testing.suite.test_dialect import (
    AutocommitTest,
    EscapingTest,
    ExceptionTest,
)
from sqlalchemy.testing.suite.test_ddl import TableDDLTest

from sqlalchemy.testing.suite.test_insert import (
    LastrowidTest,
    InsertBehaviorTest,
    ReturningTest,
)
from sqlalchemy.testing.suite.test_sequence import (
    HasSequenceTest,
    SequenceCompilerTest,
    SequenceTest,
)
from sqlalchemy.testing.suite.test_select import (
    LimitOffsetTest,
    ExpandingBoundInTest,
    OrderByLabelTest,
    LikeFunctionsTest,
    CollateTest,
    CompoundSelectTest,
)

from sqlalchemy.testing.suite.test_results import (
    PercentSchemaNamesTest,
    RowFetchTest,
    ServerSideCursorsTest,
)
from sqlalchemy.testing.suite.test_update_delete import SimpleUpdateDeleteTest

from sqlalchemy.testing.suite.test_reflection import (
    ComponentReflectionTest,
    HasTableTest,
    NormalizedNameTest,
)
from sqlalchemy.testing.suite.test_types import (
    UnicodeVarcharTest,
    UnicodeTextTest,
    JSONTest,
    DateTest,
    DateTimeTest,
    TextTest,
    NumericTest,
    IntegerTest,
    DateTimeHistoricTest,
    DateTimeCoercedToDateTimeTest,
    TimeMicrosecondsTest,
    TimestampMicrosecondsTest,
    TimeTest,
    DateTimeMicrosecondsTest,
    DateHistoricTest,
    StringTest,
    BooleanTest,
)
