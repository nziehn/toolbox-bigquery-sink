# toolbox-bigquery-sink

This is an alpha version - be sure to test a lot before using it in production.

Requires python >= 3.7!

Example usage:


```python
from toolbox import bigquery_sink as _bigquery_sink
from toolbox.bigquery_sink import SchemaField as _SF
from toolbox.bigquery_sink import FieldType as _FT
from toolbox.bigquery_sink import bulk_sink as _bulk_sink

access_config = _bigquery_sink.AccessConfig(
    project_id='YOUR_GOOGLE_CLOUD_PROJECT_ID',  # you can provide a default project_id when uploading data
    dataset_id='YOUR_DATASET_ID',  # you can provide a default dataset_id when uploading data
    temp_bucket_name='YOUR_GOOGLE_CLOUD_BUCKET_NAME',  # when uploading bulk data need a temp storage bucket
    service_account_credentials={...},  # provide service account credentials
    bq_location='EU'  # BQ processing location
)

# define the sink
sink = _bulk_sink.BQBulkSink(
    table_id='YOUR_TABLE_NAME',  # specify a table name
    access_config=access_config,  
    schema=[  # if you upload new data, the schema is mandatory 
        _SF(name='asd', field_type=_FT.STRING)
    ]
)

# open the sink as a context manager and write data into it
with sink.open() as writer:
    writer({'asd': '1'})
```

The sink as well as schema fields both offer more parameters for customisation. Esp. on the sink there is more configuration possible to enable table partitioning etc.