# toolbox-bigquery-sink

This is an alpha version - be sure to test a lot before using it in production.

Requires python >= 3.7!

Example usage:


```python
from toolbox import bigquery_sink as _bigquery_sink
from toolbox.bigquery_sink import SchemaField as _SF
from toolbox.bigquery_sink import FieldType as _FT
from toolbox.bigquery_sink import bulk_sink as _bulk_sink

options = _bigquery_sink.Options(
    project_id='YOUR_GOOGLE_CLOUD_PROJECT_ID',  # you can provide a default project_id when uploading data
    dataset_id='YOUR_DATASET_ID',  # you can provide a default dataset_id when uploading data
    temp_bucket_name='YOUR_GOOGLE_CLOUD_BUCKET_NAME',  # when uploading bulk data need a temp storage bucket
    service_account_credentials={...},  # provide service account credentials
    bq_location='EU'  # BQ processing location
)

# define the sink
sink = _bulk_sink.BQBulkSink(
    table_id='YOUR_TABLE_NAME',  # specify a table name
    options=options,  
    schema=[  # if you upload new data, the schema is mandatory 
        _SF(name='asd', field_type=_FT.STRING)
    ]
)

# open the sink as a context manager and write data into it
with sink.open() as writer:
    writer({'asd': '1'})
```

The sink as well as schema fields both offer more parameters for customisation. Esp. on the sink there is more configuration possible to enable table partitioning etc.



## Google Spreadsheet Sink

You can also create a BQ table (sink) from a Google Spreadsheet, like this:

```python
from toolbox.bigquery_sink import sheet_sink as _sheet_sink

sink = _sheet_sink.BQSheetSink(
    table_id='NAME OF YOUR TABLE',
    sheet_url="YOUR GOOGLE SPREADSHEET LINK",  # your user / service account needs read access to it!
    sheet_range=_sheet_sink.SheetRange(
        tab='NAME OF THE TAB IN THE SPREADSHEET',
        first_column='A',
        first_row=2,
        last_column='G',
        # last_row was intentionally left out to take all data regardless where it ends!
    ),
    schema=[
        _SF(name='asd', field_type=_FT.STRING)
    ],
    options=options,  # you can reuse the same options as above
    
    # if set to True, any changes that you make to schema, sheet_range, description, ... are applied (if possible) automatically
    auto_update_table_schema=True,  
)
sink.create_bq_table(exists_ok=True)
```