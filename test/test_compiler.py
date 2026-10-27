"""
Compilation of statements into API documents, without a database.

The SQLAlchemy test plugin loaded by conftest.py needs a database URI, but
these tests do not connect to it:

    pytest --dburi=sqlite:// test/test_compiler.py
"""

import sqlalchemy as sa
from sqlalchemy.testing import fixtures

from oedialect.dialect import OEDialect


def make_table():
    return sa.Table(
        "feedin",
        sa.MetaData(),
        sa.Column("w_id", sa.Integer, primary_key=True),
        sa.Column("carrier", sa.Text),
        sa.Column("power_class", sa.Integer),
        schema="supply",
    )


def resolve(document, params):
    """Fill the bind parameters into a document, as the cursor does."""
    if callable(document):
        return document(params)
    if isinstance(document, dict):
        return {k: resolve(v, params) for k, v in document.items()}
    if isinstance(document, list):
        return [resolve(v, params) for v in document]
    return document


def compile_(statement):
    compiled = statement.compile(dialect=OEDialect())
    return resolve(compiled.string, compiled.params)


def column(name):
    return {
        "type": "column",
        "column": name,
        "is_literal": False,
        "table": "feedin",
        "schema": "supply",
    }


def operator(operator_, *operands):
    return {"type": "operator", "operator": operator_, "operands": list(operands)}


class CompilerTest(fixtures.TestBase):
    def test_dialect_is_registered(self):
        import oedialect  # noqa: F401

        engine = sa.create_engine("postgresql+oedialect://:@openenergyplatform.org")
        assert isinstance(engine.dialect, OEDialect)

    def test_select(self):
        table = make_table()

        assert compile_(sa.select(table.c.w_id)) == {
            "command": "advanced/search",
            "type": "select",
            "fields": [column("w_id")],
            "from": [{"type": "table", "schema": "supply", "table": "feedin"}],
        }

    def test_where_criteria_are_combined_with_and(self):
        table = make_table()
        document = compile_(
            sa.select(table.c.w_id)
            .where(table.c.carrier == "solar")
            .where(table.c.power_class == 2)
        )

        assert document["where"] == operator(
            " AND ",
            operator(" = ", column("carrier"), "solar"),
            operator(" = ", column("power_class"), 2),
        )

    def test_and(self):
        table = make_table()
        document = compile_(
            sa.select(table.c.w_id).where(
                sa.and_(table.c.carrier == "solar", table.c.power_class == 2)
            )
        )

        assert document["where"] == operator(
            " AND ",
            operator(" = ", column("carrier"), "solar"),
            operator(" = ", column("power_class"), 2),
        )

    def test_in(self):
        table = make_table()
        document = compile_(sa.select(table.c.w_id).where(table.c.w_id.in_([1, 2])))

        assert document["where"] == operator(
            " IN ", column("w_id"), {"type": "grouping", "grouping": [1, 2]}
        )

    def test_not_in(self):
        # the API does not know the operator NOT IN
        table = make_table()
        document = compile_(sa.select(table.c.w_id).where(table.c.w_id.not_in([1, 2])))

        assert document["where"] == operator(
            "not",
            operator(" IN ", column("w_id"), {"type": "grouping", "grouping": [1, 2]}),
        )

    def test_group_by_order_by_limit_offset(self):
        table = make_table()
        document = compile_(
            sa.select(table.c.carrier, sa.func.count(table.c.w_id))
            .group_by(table.c.carrier)
            .order_by(table.c.carrier)
            .limit(5)
            .offset(10)
        )

        assert document["group_by"] == [column("carrier")]
        assert document["order_by"] == [column("carrier")]
        assert document["limit"] == 5
        assert document["offset"] == 10

    def test_insert(self):
        table = make_table()
        document = compile_(table.insert().values(w_id=1, carrier="solar"))

        assert document == {
            "command": "advanced/insert",
            "table": "feedin",
            "schema": "supply",
            "fields": ["w_id", "carrier"],
            "values": [[1, "solar"]],
        }

    def test_insert_multiple_rows(self):
        table = make_table()
        document = compile_(
            table.insert().values(
                [{"w_id": 1, "carrier": "a"}, {"w_id": 2, "carrier": "b"}]
            )
        )

        assert document["values"] == [[1, "a"], [2, "b"]]

    def test_update(self):
        table = make_table()
        document = compile_(
            table.update().where(table.c.w_id == 1).values(carrier="wind")
        )

        assert document == {
            "command": "advanced/update",
            "table": "feedin",
            "schema": "supply",
            "fields": [{"type": "column", "column": "carrier", "is_literal": False}],
            "values": ["wind"],
            "where": operator(" = ", column("w_id"), 1),
        }

    def test_delete(self):
        table = make_table()
        document = compile_(table.delete().where(table.c.w_id == 1))

        assert document == {
            "command": "advanced/delete",
            "table": "feedin",
            "schema": "supply",
            "where": operator(" = ", column("w_id"), 1),
        }

    def test_create_and_drop_table(self):
        table = make_table()
        create = sa.schema.CreateTable(table).compile(dialect=OEDialect()).string
        drop = sa.schema.DropTable(table).compile(dialect=OEDialect()).string

        assert create["command"] == "schema/supply/tables/feedin/"
        assert [c["name"] for c in create["columns"]] == [
            "w_id",
            "carrier",
            "power_class",
        ]
        assert drop == {
            "request_type": "delete",
            "command": "schema/supply/tables/feedin/",
        }
