import re as _re
import typing as _typing

from google.cloud import bigquery as _bigquery
from toolbox import bigquery_sink as _bigquery_sink


class SheetRange(object):
    def __init__(
        self,
        tab: str,
        first_column: str,
        first_row: int,             # starts with 1!
        last_column: str,
        last_row: int = None,       # starts with 1!
    ):
        assert _re.match(r'[a-zA-Z]+', first_column)
        assert _re.match(r'[a-zA-Z]+', last_column)
        assert first_row > 0
        assert last_row is None or last_row > 0

        self.tab = tab
        self.first_column = first_column
        self.first_row = first_row
        self.last_column = last_column
        self.last_row = last_row

    @staticmethod
    def _escape_string(input_str):
        return "'{}'".format(input_str.replace("'", "''"))

    def __str__(self):
        return '{tab}!{c1}{r1}:{c2}{r2}'.format(
            tab=self._escape_string(self.tab),
            c1=self.first_column.upper(),
            r1=self.first_row,
            c2=self.last_column.upper(),
            r2=self.last_row or ''
        )


class BQSheetSink(object):
    """
    Data Sink for loading chunks of rows into bigquery (not streaming insert!)
    """

    def __init__(
        self,
        table_id: str,
        sheet_url: str,
        sheet_range: SheetRange,
        schema: _typing.List[_bigquery_sink.SchemaField],
        options: _bigquery_sink.Options,
        table_description: str = None,
        auto_update_table_schema: bool = False,
    ):
        self.table_id = table_id
        self.dataset_id = options.dataset_id
        self.project_id = options.project_id
        self.table_ref = self.table_ref = "{}.{}.{}".format(
            self.project_id, self.dataset_id, self.table_id
        )
        self.options = options

        self.sheet_url = sheet_url
        self.sheet_range = sheet_range
        self.schema = schema

        self.table_description = table_description
        self.auto_update_table_schema = auto_update_table_schema

        self.bigquery = options.get_bigquery_client(
            scopes=[
                "https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/bigquery",
            ]
        )

    def create_bq_table(self, exists_ok=True):
        bq_schema = (
            None if self.schema is None else [f.to_bq_field() for f in self.schema]
        )
        table = _bigquery.Table(table_ref=self.table_ref, schema=bq_schema)
        external_config = _bigquery.ExternalConfig(
            _bigquery.ExternalSourceFormat.GOOGLE_SHEETS
        )
        external_config.source_uris = [self.sheet_url]
        external_config.options.range = str(self.sheet_range)
        table.external_data_configuration = external_config
        table = self.bigquery.create_table(table, exists_ok=exists_ok)

        table = _bigquery_sink.check_and_update_external_config(
            table=table,
            external_config=external_config,
            bigquery=self.bigquery
        )
        table = _bigquery_sink.check_and_update_labels(
            table=table, labels=self.options.labels, bigquery=self.bigquery
        )
        table = _bigquery_sink.check_and_update_schema(
            table=table,
            schema=self.schema,
            bigquery=self.bigquery,
            auto_update_table_schema=self.auto_update_table_schema,
        )
        table = _bigquery_sink.check_and_update_description(
            table=table,
            table_description=self.table_description,
            bigquery=self.bigquery,
        )
        return table
