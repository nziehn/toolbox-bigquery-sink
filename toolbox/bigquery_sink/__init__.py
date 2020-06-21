from __future__ import annotations
import enum as _enum
import typing as _typing
import datetime as _datetime
import re as _re
from google.cloud import bigquery as _bigquery
from google.cloud import storage as _storage
from google.oauth2 import service_account as _service_account
from toolbox.bigquery_sink.utils import nest as _nest


class Options(object):
    def __init__(
        self,
        project_id: str,
        dataset_id: str = None,
        temp_bucket_name: str = None,
        temp_bucket_root_path: str = None,
        service_account_credentials: dict = None,
        bq_location: str = None,
        correlation_id: str = None,
        now: _datetime.datetime = None,
        labels: dict = None,
    ):
        """
        Reduce copy paste code: create an access config once and reuse it for multiple sinks
        :param project_id: The project id where table data will be stored
        :param dataset_id: The dataset where table data will be stored
        :param temp_bucket_name: The google cloud storage bucket where temp files are "parked"
        :param temp_bucket_root_path: The path to the root folder where temp files will be "parked" inside the temp bucket
        :param service_account_credentials: You can provide service account credentials for non-user bound access to google cloud
        :param bq_location: The location where the data should be hosted (only relevant if table does not exist, yet).
        :param correlation_id: (optional) identifier that allows finding associated objects in microservice architecture
        :param now: (optional) allows trace passed time since a process was originally started
        :param labels: A dictionary of labels that should be attached to tables and other resources
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.temp_bucket_name = temp_bucket_name
        self.temp_bucket_root_path = temp_bucket_root_path
        self.service_account_credentials = service_account_credentials
        self.bq_location = bq_location
        self.labels = labels

        if correlation_id:
            if not _re.match(r'[a-z]+', correlation_id):
                raise ValueError('Correlation id must be sequence of lower case characters!')

        self.correlation_id = correlation_id
        self.now = now

    def get_bigquery_client(self, scopes=None):
        return self._get_client(cls=_bigquery.Client, scopes=scopes)

    def get_storage_client(self):
        return self._get_client(cls=_storage.Client,)

    def _get_client(self, cls, scopes=None):
        if self.service_account_credentials:
            project_id = self.service_account_credentials["project_id"]
        else:
            project_id = self.project_id

        credentials = _service_account.Credentials.from_service_account_info(
            self.service_account_credentials
        )
        if scopes:
            credentials = credentials.with_scopes(scopes=scopes)

        return cls(project=project_id, credentials=credentials)

    def replace(
        self,
        project_id: str = None,
        dataset_id: str = None,
        temp_bucket_name: str = None,
        temp_bucket_root_path: str = None,
        service_account_credentials: dict = None,
        bq_location: str = None,
        correlation_id: str = None,
        now: _datetime.datetime = None,
        labels: dict = None,
    ):
        """
        Return an access config with the same values but replacing the values that are not None
        :param project_id: The project id where table data will be stored
        :param dataset_id: The dataset where table data will be stored
        :param temp_bucket_name: The google cloud storage bucket where temp files are "parked"
        :param temp_bucket_root_path: The path to the root folder where temp files will be "parked" inside the temp bucket
        :param service_account_credentials: You can provide service account credentials for non-user bound access to google cloud
        :param bq_location: The location where the data should be hosted (only relevant if table does not exist, yet).
        :param correlation_id: (optional) identifier that allows finding associated objects in microservice architecture
        :param now: (optional) allows trace passed time since a process was originally started
        :param labels: A dictionary of labels that should be attached to tables and other resources
        :return: A copy of the access config after replacing the provided fields
        """
        return Options(
            project_id=project_id or self.project_id,
            dataset_id=dataset_id or self.dataset_id,
            temp_bucket_name=temp_bucket_name or self.temp_bucket_name,
            temp_bucket_root_path=temp_bucket_root_path or self.temp_bucket_root_path,
            service_account_credentials=service_account_credentials
            or self.service_account_credentials,
            bq_location=bq_location or self.bq_location,
            correlation_id=correlation_id or self.correlation_id,
            now=now or self.now,
            labels=labels,
        )


class FieldType(_enum.Enum):
    STRING = "STRING"
    BYTES = "BYTES"

    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    NUMERIC = "NUMERIC"

    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    DATETIME = "DATETIME"  # note that this ignores timezone! Use rather timestamp and set to UTC
    TIME = "TIME"

    STRUCT = "STRUCT"
    RECORD = "STRUCT"


class FieldMode(_enum.Enum):
    NULLABLE = "NULLABLE"
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"  # define an array type in standard sql


class SourcePathElements(_enum.Enum):
    """
    Use these to define special fields in the source path of a schema field
    """

    ROOT = object()  # make the path absolute and not relative
    LIST_INDEX = object()  # mark that you're accessing a list within the source path


class _MissingToken(object):
    """
    Used internally to detect missing values
    """

    pass


source_fn_type = _typing.Callable[[_typing.Any, _typing.List], _typing.Any]


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
        source_fn: source_fn_type = None,
        fields: _typing.List[SchemaField] = None,
        mode: FieldMode = None,
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
        self.source_path = source_path
        self.source_fn = source_fn
        self.fields = fields
        self.mode = mode or FieldMode.NULLABLE

    def to_bq_field(self):
        """
        Creates a SchemaField compatible with bigquery's python library
        :return: SchemaField from original google lib
        """
        return _bigquery.schema.SchemaField(
            name=self.name,
            field_type=self.field_type.value,
            mode=self.mode.value or "NULLABLE",
            description=self.description,
            fields=[f.to_bq_field() for f in self.fields or ()],
        )

    def extract(self, row, should_ensure_type=True, should_fire_exception=None):
        """
        Extracts the field value from row given source_path
        :param row: the row to extract from
        :param should_ensure_type: Should the types be casted into the output types?
        :param should_fire_exception:
            Pass in fn to check whether an exception should be fired. fn(row, path, exception) -> Boolean
        :return: The field value that was extracted
        """

        return self._extract_inner(
            row=row,
            path=[],
            should_ensure_type=should_ensure_type,
            should_fire_exception=should_fire_exception,
        )

    def _extract_inner(self, row, path, should_ensure_type, should_fire_exception):
        """
        Helper function of extract.
        Extracts the value from the row according to the source definition on the schema field and the provided path.
        :param row: The original row to extract data from
        :param path:
            This function is used recursively and path provides information where we are reading from in the source
        :param should_ensure_type: Should the types be casted into the output types?
        :param should_fire_exception:
            Pass in fn to check whether an exception should be fired. fn(row, path, exception) -> Boolean
        :return: Returns the value of this field extracted from the row
        """
        try:
            missing_token = _MissingToken()
            if self.source_fn:
                return self.source_fn(row, path)

            source_path = self._create_source_path(path=path)

            if self.mode == FieldMode.REPEATED:
                return self._extract_inner_repeated(
                    row, source_path, should_ensure_type, should_fire_exception
                )

            if self.field_type == FieldType.STRUCT:
                value = {
                    field.name: field._extract_inner(
                        row,
                        path=source_path,
                        should_ensure_type=should_ensure_type,
                        should_fire_exception=should_fire_exception,
                    )
                    for field in self.fields
                }
            else:
                value = _nest.get(obj=row, path=source_path, default=missing_token)
                if value == missing_token:
                    value = None
                if should_ensure_type:
                    value = self._ensure_type(value=value)

            return value
        except Exception as exception:
            if should_fire_exception:
                should_fire = should_fire_exception(row, path, exception)
                if should_fire:
                    raise
            else:
                raise

    def _create_source_path(self, path):
        if self.source_path is not None:
            if len(self.source_path) and self.source_path[0] == SourcePathElements.ROOT:
                source_path = self.source_path[1:]

                # replace LIST_INDEX elements in source path, if they follow the same path!
                for idx, (source_path_item, path_item) in enumerate(
                    list(zip(source_path, path))
                ):
                    if source_path_item == path:
                        continue
                    if source_path_item == SourcePathElements.LIST_INDEX:
                        source_path[idx] = path_item
                    if source_path_item != path_item:
                        break
            else:
                source_path = path + self.source_path
        else:
            source_path = path + [self.name]

        return source_path

    def _extract_inner_repeated(
        self, row, source_path, should_ensure_type, should_fire_exception
    ):
        if SourcePathElements.LIST_INDEX in source_path:
            # if there is a LIST_INDEX value in the path,
            # split up the path and deal with the remainder recursively
            list_index_position = source_path.index(SourcePathElements.LIST_INDEX)
            path_to = source_path[:list_index_position]
            remaining_path = source_path[list_index_position + 1 :]
            inner_list = _nest.get(row, path_to)
            next_inner_list = []

            if SourcePathElements.LIST_INDEX in remaining_path:
                # if in the remainder is another LIST_INDEX, we expect to get a list back
                for idx in range(len(inner_list)):
                    next_inner_list += self.replace(
                        source_path=remaining_path
                    )._extract_inner(
                        row,
                        path=path_to + [idx],
                        should_ensure_type=should_ensure_type,
                        should_fire_exception=should_fire_exception,
                    )
                return next_inner_list
            else:
                # if there is no other LIST_INDEX remaining, we expect to get back individual values
                # therefore we remove the field mode REPEATED in the recursive call
                for idx in range(len(inner_list)):
                    next_inner_list.append(
                        self.replace(
                            mode=FieldMode.NULLABLE, source_path=remaining_path
                        )._extract_inner(
                            row,
                            path=path_to + [idx],
                            should_ensure_type=should_ensure_type,
                            should_fire_exception=should_fire_exception,
                        )
                    )
                return next_inner_list

        inner_list = _nest.get(row, source_path)
        if not inner_list or not isinstance(inner_list, list):
            return []

        if self.field_type == FieldType.STRUCT:
            value = [
                {
                    field.name: field._extract_inner(
                        row,
                        path=source_path + [idx],
                        should_ensure_type=should_ensure_type,
                        should_fire_exception=should_fire_exception,
                    )
                    for field in self.fields
                }
                for idx in range(len(inner_list))
            ]
        else:
            value = []
            for idx, inner_value in enumerate(inner_list):
                if should_ensure_type:
                    value.append(self._ensure_type(value=inner_value))
                else:
                    value.append(inner_value)
        return value

    def _ensure_type(self, value):
        """
        Helper function of self.extract - ensures that the output types are correct.
        The types are prepared so that BigQuery can import them!
        :param value: Input value that should be checked and casted
        :return: A type-casted value that BigQuery should be able to read it
        """
        if isinstance(value, float) and self.field_type == FieldType.NUMERIC:
            value = str(round(value, 8))

        if isinstance(value, int) and self.field_type == FieldType.BOOLEAN:
            value = value != 0

        if isinstance(value, _datetime.datetime) and self.field_type == FieldType.DATE:
            value = value.date()

        if (
            not isinstance(value, str)
            and value is not None
            and self.field_type == FieldType.STRING
        ):
            value = str(value)

        if self.field_type == FieldType.INTEGER and value is not None:
            value = int(value)

        if self.field_type == FieldType.FLOAT and value is not None:
            value = float(value)

        if self.field_type == FieldType.DATE and isinstance(value, int):
            value = _datetime.datetime.fromtimestamp(value).date()

        if (
            self.field_type == FieldType.DATETIME
            or self.field_type == FieldType.TIMESTAMP
        ) and isinstance(value, int):
            value = _datetime.datetime.utcfromtimestamp(value)

        return value

    def replace(
        self,
        name: str = None,
        field_type: FieldType = None,
        description: str = None,
        source_path: _typing.List[str] = None,
        source_fn: source_fn_type = None,
        fields: _typing.List[SchemaField] = None,
        mode: FieldMode = None,
    ):
        """
        Returns a copy of this schema field but replacing the values with provided parameters (if not None)
        :param name: Name of the field in bigquery
        :param field_type: Type of field, pick from any bigquery compatible type, e.g. STRING, TIMESTAMP, INTEGER, FLOAT
        :param description: Describe the content of this field
        :param source_path: Can be used to easily extract data from a data source row. Should be a list of keys
        :param fields: If the field_type is RECORD you can specify fields for the "sub document" of the record
        :param mode: pick from NULLABLE, REQUIRED, REPEATED (view bigquery docs for meaning)
        """
        return SchemaField(
            name=name if name is not None else self.name,
            field_type=field_type if field_type is not None else self.field_type,
            description=description if description is not None else self.description,
            source_path=source_path if source_path is not None else self.source_path,
            source_fn=source_fn if source_fn is not None else self.source_fn,
            fields=fields if fields is not None else self.fields,
            mode=mode if mode is not None else self.mode,
        )


def create_view(name: str, query: str, options: Options):
    """
    Create a view inside bigquery

    :param name: The name of the view (corresponds to a table id)
    :param query: The SQL query that specifies the content of the vieww
    :param options: The access config object that defines project_id and other common parameters
    """
    bigquery = options.get_bigquery_client()
    view_ref = "{}.{}.{}".format(options.project_id, options.dataset_id, name)

    view = _bigquery.Table(view_ref)
    view.view_query = query
    return bigquery.create_table(view, exists_ok=True)


def check_and_update_labels(table, labels, bigquery):
    """
    Make sure the labels are up2date on the table
    """

    if labels and table.labels != labels:
        new_labels = {
            k: None  # make sure "old" labels are actually removed
            for k in table.labels.keys()
        }
        new_labels.update(labels)
        table.labels = new_labels
        return bigquery.update_table(table=table, fields=["labels"])
    return table


def check_and_update_description(table, table_description, bigquery):
    """
    Make sure the description is up2date
    """
    if table.description != table_description:
        table.description = table_description
        return bigquery.update_table(table=table, fields=["description"])
    return table


def check_and_update_external_config(table, external_config, bigquery):
    """
    update the external config
    """
    if table.external_data_configuration != external_config:
        table.external_data_configuration = external_config
        return bigquery.update_table(table=table, fields=["external_data_configuration"])
    return table


def check_and_update_schema(table, schema, bigquery, auto_update_table_schema=True):
    """
    Make sure that the schema of table matches the one provided.
    The ordering is NOT taken into account - changes in order will not be updated
    """

    if not schema:
        return table

    bq_schema = (
        None if schema is None else [f.to_bq_field() for f in schema]
    )

    def extract_schema_fields(schema):
        if not schema:
            return None
        return sorted(
            [
                (
                    f.name.upper(),
                    f.field_type.upper(),
                    f.mode.upper(),
                    f.description,
                    extract_schema_fields(f.fields),
                )
                for f in schema
            ]
        )

    current_schema_repr = extract_schema_fields(table.schema)
    new_schema_repr = extract_schema_fields(bq_schema)
    schema_up2date = current_schema_repr == new_schema_repr

    if not schema_up2date:
        if auto_update_table_schema:
            table.schema = bq_schema
            bigquery.update_table(table=table, fields=["schema"])
            return table
        else:
            raise ValueError(
                "Schema not up-to-date"
            )

    return table


if __name__ == "__main__":
    pass
