# test/test_suite.py

from sqlalchemy.testing.suite.test_dialect import (
    AutocommitTest,
    # EscapingTest, TODO: Uncommment
    # ExceptionTest,
)
# from sqlalchemy.testing.suite.test_ddl import TableDDLTest TODO: Uncomment

from sqlalchemy.testing.suite.test_insert import (
    LastrowidTest,
    InsertBehaviorTest,
    # ReturningTest,
)
# from sqlalchemy.testing.suite.test_sequence import (
    # HasSequenceTest,
    # SequenceCompilerTest, TODO: Uncomment
    # SequenceTest,
# )
from sqlalchemy.testing.suite.test_select import (
    # LimitOffsetTest, Reason: Contains plain queries
    # ExpandingBoundInTest, TODO: Implement expansion
    OrderByLabelTest,
    # LikeFunctionsTest, TODO: Uncomment
    CollateTest,
    CompoundSelectTest,
)

from sqlalchemy.testing.suite.test_results import (
    PercentSchemaNamesTest,
    # RowFetchTest, TODO: Uncomment
    # ServerSideCursorsTest, TODO: Uncomment
)
from sqlalchemy.testing.suite.test_update_delete import SimpleUpdateDeleteTest

from sqlalchemy.testing.suite.test_reflection import (
    #ComponentReflectionTest,
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
