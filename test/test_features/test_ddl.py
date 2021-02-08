from sqlalchemy.testing import fixtures
from sqlalchemy import Column, INTEGER, Table


class TableCommentTest(fixtures.TablesTest):
    table_comment = """{"id":"test"}"""
    expected_result= ('{"id": "test", "metaMetadata": {"metadataVersion": "OEP-1.4.0", '
 '"metadataLicense": {"name": "CC0-1.0", "title": "Creative Commons Zero v1.0 '
 'Universal", "path": "https://creativecommons.org/publicdomain/zero/1.0/"}}}')

    @classmethod
    def define_tables(cls, metadata):
        Table("table", metadata,
              Column('id', INTEGER, primary_key=True),
              comment=cls.table_comment
              )

    def test_comment(self):
        temp_cache = self.metadata.tables
        self.metadata.tables = {}
        t = Table(
            "table", self.metadata, autoload=True
        )
        self.metadata.tables = temp_cache
        assert t.comment == self.expected_result