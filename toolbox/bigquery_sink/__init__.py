from __future__ import annotations
import enum as _enum
import typing as _typing
from google.cloud import bigquery as _bigquery
from google.cloud import storage as _storage
from google.oauth2 import service_account as _service_account
from toolbox.bigquery_sink.utils import nest as _nest


class AccessConfig(object):
    def __init__(
            self,
            project_id: str,
            dataset_id: str = None,
            temp_bucket_name: str = None,
            temp_bucket_root_path: str = None,
            service_account_credentials: dict = None,
            bq_location: str = None,
    ):
        """
        Reduce copy paste code: create an access config ones and reuse it for multiple sinks
        :param project_id: The project id where table data will be stored
        :param dataset_id: The dataset where table data will be stored
        :param temp_bucket_name: The google cloud storage bucket where temp files are "parked"
        :param temp_bucket_root_path: The path to the root folder where temp files will be "parked" inside the temp bucket
        :param service_account_credentials: You can provide service account credentials for non-user bound access to google cloud
        :param bq_location: The location where the data should be hosted (only relevant if table does not exist, yet).
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.temp_bucket_name = temp_bucket_name
        self.temp_bucket_root_path = temp_bucket_root_path
        self.service_account_credentials = service_account_credentials
        self.bq_location = bq_location

    def get_bigquery_client(self):
        return self._get_client(
            cls=_bigquery.Client,
        )

    def get_storage_client(self):
        return self._get_client(
            cls=_storage.Client,
        )

    def _get_client(self, cls):
        if self.service_account_credentials:
            project_id = self.service_account_credentials['project_id']
        else:
            project_id = self.project_id

        credentials = _service_account.Credentials.from_service_account_info(
            self.service_account_credentials
        )
        return cls(
            project=project_id,
            credentials=credentials
        )


class FieldType(_enum.Enum):
    STRING = 'STRING'
    BYTES = 'BYTES'

    INTEGER = 'INTEGER'
    FLOAT = 'FLOAT'
    NUMERIC = 'NUMERIC'

    TIMESTAMP = 'TIMESTAMP'
    DATE = 'DATE'
    DATETIME = 'DATETIME'  # note that this ignores timezone! Use rather timestamp and set to UTC
    TIME = 'TIME'

    STRUCT = 'STRUCT'
    RECORD = 'STRUCT'


class FieldMode(_enum.Enum):
    NULLABLE = 'NULLABLE'
    REQUIRED = 'REQUIRED'
    REPEATED = 'REPEATED'  # define an array type in standard sql


class SchemaField(object):
    """
    Used to build schemas for bigquery tables
    """
    def __init__(
            self,
            name: str,
            field_type: FieldType,
            description: str = None,
            source_path: _typing.List[str] = None,
            fields: _typing.List[SchemaField] = None,
            mode: FieldMode = None
    ):
        """
        Create new schema field
        :param name: Name of the field in bigquery
        :param field_type: Type of field, pick from any bigquery compatible type, e.g. STRING, TIMESTAMP, INTEGER, FLOAT
        :param description: Describe the content of this field
        :param source_path: Can be used to easily extract data from a data source row. Should be a list of keys
        :param fields: If the field_type is RECORD you can specify fields for the "sub document" of the record
        :param mode: pick from NULLABLE, REQUIRED, REPEATED (view bigquery docs for meaning)
        """
        self.name = name
        self.field_type = field_type
        self.description = description
        self.source_path = source_path or [self.name]
        self.fields = fields
        self.mode = mode or FieldMode.NULLABLE

    def to_bq_field(self):
        """
        Creates a SchemaField compatible with bigquery's python library
        :return: SchemaField from original google lib
        """
        return _bigquery.schema.SchemaField(
            name=self.name, field_type=self.field_type.value, mode=self.mode.value or 'NULLABLE',
            description=self.description, fields=self.fields or ()
        )

    def extract(self, row):
        """
        Extracts the field value from row given source_path
        :param row: the row to extract from
        :return: The field value that was extracted
        """
        value = _nest.get(row, self.source_path)
        if isinstance(value, float) and self.field_type == 'NUMERIC':
            value = str(round(value, 8))
        if isinstance(value, int) and self.field_type == 'BOOLEAN':
            value = value > 0
        return value


def create_view(name: str, sql: str, access_config: AccessConfig, dataset_id: str = None):
    """
    Create a view inside bigquery

    :param name: The name of the view (corresponds to a table id)
    :param sql: The SQL query that specifies the content of the vieww
    :param dataset_id: You can override the default dataset from the access_config
    :param access_config: The access config object that defines project_id and other common parameters
    """
    bigquery = access_config.get_bigquery_client()
    view_ref = '{}.{}.{}'.format(access_config.project_id, dataset_id, name)

    view = _bigquery.Table(view_ref)
    view.view_query = sql
    return bigquery.create_table(view, exists_ok=True)

if __name__ == '__main__':
    print(type(FieldType.STRING.value))