# sqlalchemy_access/requirements.py

from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    """@property
    def table_reflection(self):
        return exclusions.closed()"""

    @property
    def independent_connections(self):
        return exclusions.closed()

    @property
    def returning(self):
        return exclusions.open()

    @property
    def index_reflection(self):
        return exclusions.closed()

    @property
    def order_by_col_from_union(self):
        return exclusions.closed()

    @property
    def broken_cx_oracle6_numerics(self):
        return exclusions.closed()

    @property
    def cross_schema_fk_reflection(self):
        return exclusions.closed()

    @property
    def named_constraints(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()

    @property
    def reflects_pk_names(self):
        return exclusions.open()
