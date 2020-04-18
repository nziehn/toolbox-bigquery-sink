import re as _re
import json as _json
import datetime as _datetime
import decimal as _decimal
import tempfile as _tempfile
import contextlib as _contextlib
import enum as _enum
import typing as _typing

from google.cloud import bigquery as _bigquery

from toolbox.bigquery_sink.utils import generate_id as _generate_id
from toolbox import bigquery_sink as _bigquery_sink


def create_table_date_partitioning(field, expiration_ms=None):
    """
    Utility function to create date(time) partitioned tables
    """
    return {
        'type': 'time_partitioning',
        'definition': _bigquery.TimePartitioning(
            type_=_bigquery.TimePartitioningType.DAY,
            field=field,
            expiration_ms=expiration_ms,
        )
    }


class WriteDisposition(_enum.Enum):
    """
    Allows controlling whether table should be appended/overwritten/only written to if empty

    APPEND: append to the table (creates table if it does not exist)
    IF_EMPTY: will only write if the table is empty (or does not exist)
    REPLACE: replaces table content in atomic operation
    """
    APPEND = _bigquery.WriteDisposition.WRITE_APPEND
    IF_EMPTY = _bigquery.WriteDisposition.WRITE_EMPTY
    REPLACE = _bigquery.WriteDisposition.WRITE_TRUNCATE


class BQBulkSink(object):
    """
    Data Sink for loading chunks of rows into bigquery (not streaming insert!)
    """
    def __init__(
            self,
            table_id: str,
            access_config: _bigquery_sink.AccessConfig,
            dataset_id: str = None,
            project_id: str = None,
            table_partitioning: dict = None,
            table_partition_date: _datetime.date = None,
            table_description: str = None,
            schema: _typing.List[_bigquery_sink.SchemaField] = None,
            labels: dict = None,
            write_disposition: WriteDisposition = WriteDisposition.APPEND,
            auto_update_table_schema: bool = False,
            bq_location: str = None,
            now: _datetime.datetime = None
    ):
        """
        :param table_id: the table id where the data should be stored. This should not contain project_id or dataset_id
        :param dataset_id: the dataset id where the destination table is located, you can overwrite the dataset_id provided from the access_config
        :param project_id: the project id where the destination table is located, you can overwrite the project_id provided from the access_config
        :param table_partitioning: BigQuery table partitioning information, use `create_table_date_partitioning` method to create correct values
        :param table_partition_date: If you want to (over-) write a specific partition, you can specify a date(time) object. This only works for date partitioned tables. If you pass in a datetime object, it will still replace the entire DATE partition.
        :param table_description: You can provide a description of the table.
        :param schema: The schema of the table
        :param labels: A dictionary of labels that should be attached to the table
        :param write_disposition: Specify whether you want to only write if the table is empty or append or replace table content
        :param auto_update_table_schema: Should the schema be automatically updated when uploading content?
        :param bq_location: The location where the data should be hosted (only relevant if table does not exist, yet). By default will be taken from access_config or defaults to 'US'
        :param now: To make it easier to find temp files and job ids, we put a reference to the current time into the ids
        """

        self.access_config = access_config
        self.project_id = project_id or access_config.project_id
        self.dataset_id = dataset_id or access_config.dataset_id
        self.table_id = table_id
        self.table_ref = '{}.{}.{}'.format(self.project_id, self.dataset_id, self.table_id)
        self.table_partitioning = table_partitioning
        self.table_partition_date = table_partition_date
        self.table_description = table_description
        self.schema = schema
        self.bq_schema = None if self.schema is None else [f.to_bq_field() for f in self.schema]
        self.labels = labels or {}
        self.write_disposition = write_disposition.value
        self.auto_update_table_schema = auto_update_table_schema
        self.bq_location = bq_location or access_config.bq_location or 'US'
        self.bigquery = access_config.get_bigquery_client()
        self.storage = access_config.get_storage_client()
        self.temp_bucket_name = access_config.temp_bucket_name
        self.temp_bucket_root_path = access_config.temp_bucket_root_path
        self.now = now or _datetime.datetime.utcnow()

    @_contextlib.contextmanager
    def open(self):
        """
        1) creates temp file
        2) allows writing data using context manager into newline delimited json format
        3) after moving out of the context will start submitting:
        3.1) uploads file to google storage
        3.2) loads bq table from google storage (and ensures that dataset & table exist in bq)
        :return: None
        """
        with _tempfile.TemporaryFile() as tmp_file:
            def __write(row):
                to_write = (_json.dumps(row, default=self._json_default_fn) + '\n')
                tmp_file.write(to_write.encode('utf-8'))

            yield __write

            self._create_bq_dataset(exists_ok=True)  # ensures that dataset exists
            table = self._create_bq_table(exists_ok=True)  # ensures that table exists

            storage_uri = self._upload_file_obj_to_storage(file_obj=tmp_file)
            self._load_bq_table_from_storage(storage_uri=storage_uri, table=table)

    def from_query(self, query):
        self._create_bq_dataset(exists_ok=True)  # ensures that dataset exists
        self._create_bq_table(exists_ok=True)  # ensures that table exists
        if self.table_partition_date:
            table_ref = '{}${:%Y%m%d}'.format(self.table_ref, self.table_partition_date)
        else:
            table_ref = self.table_ref

        job_config = _bigquery.QueryJobConfig(
            allow_large_results=True,
            destination=table_ref,
            write_disposition=self.write_disposition,
        )
        query_job = self.bigquery.query(
            query=query,
            job_config=job_config,
            job_id=self._generate_job_id(),
        )
        query_job.result()

    def create_related_view(self, name, sql_template):
        if '{TABLE}' in sql_template:
            sql_template = sql_template.format(TABLE='`{}`'.format(self.table_ref))

        return _bigquery_sink.create_view(
            name=name,
            sql=sql_template,
            dataset_id=self.dataset_id,
            access_config=self.access_config
        )

    def _generate_job_id(self):
        job_id = '{runner}--{project}--{dataset}--{table}--{date}-{time}-{random}'.format(
            runner='lh-dwh-etl',
            project=self.project_id,
            dataset=self.dataset_id,
            table=self.table_id,
            date=self.now.strftime('%Y-%m-%d'),
            time=self.now.strftime('%H-%M-%S'),
            random=_generate_id.generate_id(),
        )
        return job_id

    def _json_default_fn(self, obj):
        """
        Ensure that we can json dump also special types: datetime, date and decimal
        Override this fn to work with even more types
        """
        if isinstance(obj, (_datetime.datetime, _datetime.date)):
            return obj.isoformat()

        if isinstance(obj, _decimal.Decimal):
            return str(obj)

    def _upload_file_obj_to_storage(self, file_obj, rewind=True):
        """
        Upload content of a file_obj into google cloud storage.

        :param file_obj: The file object to upload
        :param rewind: Whether or not to rewind the provided file
        :return: the google cloud storage uri (e.g. 'gs://BUCKET/FILE_PATH')
        """
        bucket = self.storage.get_bucket(bucket_or_name=self.temp_bucket_name)

        if self.temp_bucket_root_path:
            root_path = self.temp_bucket_root_path
            # remove leading or trailing '/' chars
            root_path = _re.match(r'^/?(.*)/?$', root_path).group(1)
            if root_path:
                root_path += '/'
        else:
            root_path = ''

        file_path = '{root_path}{project}/{dataset}/{table}/{date}/{time}-{random}.nljson'.format(
            root_path=root_path,
            project=self.project_id,
            dataset=self.dataset_id,
            table=self.table_id,
            date=self.now.strftime('%Y-%m-%d'),
            time=self.now.strftime('%H-%M-%S'),
            random=_generate_id.generate_id(),
        )

        blob = bucket.blob(file_path)
        blob.upload_from_file(file_obj=file_obj, rewind=rewind)

        return 'gs://{}/{}'.format(self.temp_bucket_name, file_path)

    def _create_bq_dataset(self, exists_ok):
        """
        Creates a dataset in bigquery for the given project_id and dataset_id.
        Dataset location is automatically EU
        :param exists_ok: Whether or not this should complain if dataset already exists
        :return: the google lib dataset object
        """
        dataset = _bigquery.Dataset(
            dataset_ref='{}.{}'.format(self.project_id, self.dataset_id)
        )
        dataset.location = self.bq_location
        dataset = self.bigquery.create_dataset(dataset=dataset, exists_ok=exists_ok)
        return dataset

    def _create_bq_table(self, exists_ok):
        """
        1) Creates a table in bigquery given the project_id, dataset_id & table_id.
        2) Compares new vs old schema and updates if required (can be controlled with `self.auto_update_table_schema`)
        :param exists_ok: Whether or not this should complain if table already exists
        :return: the google lib table object
        """
        table = _bigquery.Table(
            table_ref=self.table_ref,
            schema=self.bq_schema
        )
        if self.table_partitioning:
            setattr(table, self.table_partitioning['type'], self.table_partitioning['definition'])
        table = self.bigquery.create_table(table=table, exists_ok=exists_ok)

        self._check_and_update_labels(table=table)
        self._check_and_update_description(table=table)
        self._check_and_update_schema(table=table)

        if self.table_partition_date:
            table = _bigquery.Table(
                table_ref='{}${:%Y%m%d}'.format(self.table_ref, self.table_partition_date),
                schema=self.bq_schema
            )

        return table

    def _check_and_update_labels(self, table):
        """
        Make sure the labels are up2date on the table
        """
        if table.labels != self.labels:
            labels = {
                k: None  # make sure "old" labels are actually removed
                for k in table.labels.keys()
            }
            labels.update(self.labels)
            table.labels = labels
            self.bigquery.update_table(table=table, fields=['labels'])

    def _check_and_update_description(self, table):
        """
        Make sure the description is up2date
        """
        if table.description != self.table_description:
            table.description = self.table_description
            self.bigquery.update_table(table=table, fields=['description'])

    def _check_and_update_schema(self, table):
        """
        Make sure that the schema of table matches the one provided.
        The ordering is NOT taken into account - changes in order will not be updated
        """
        def extract_schema_fields(schema):
            if not schema:
                return None
            return sorted([
                (
                    f.name.upper(),
                    f.field_type.upper(),
                    f.mode.upper(),
                    f.description,
                    extract_schema_fields(f.fields)
                )
                for f in schema
            ])

        current_schema_repr = extract_schema_fields(table.schema)
        new_schema_repr = extract_schema_fields(self.bq_schema)
        if current_schema_repr != new_schema_repr:
            if self.auto_update_table_schema:
                table.schema = self.bq_schema
                self.bigquery.update_table(table=table, fields=['schema'])
            else:
                raise ValueError('Schema changed {}.{}.{}'.format(self.project_id, self.dataset_id, self.table_id))

    def _load_bq_table_from_storage(self, storage_uri, table):
        """
        Create/Update a bigquery table given a google cloud storage uri
        :param storage_uri: The source uri where to get the data from, e.g. 'gs://BUCKET/FILE_PATH'
        :return: None
        """
        job_config = _bigquery.LoadJobConfig(
            schema=self.bq_schema,
            source_format=_bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=self.write_disposition,
        )
        load_job = self.bigquery.load_table_from_uri(
            source_uris=storage_uri,
            destination=table,
            job_config=job_config,
            job_id=self._generate_job_id()
        )
        load_job.result()  # Waits for table load to complete.


if __name__ == '__main__':
    pass