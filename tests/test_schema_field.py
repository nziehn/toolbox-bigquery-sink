import datetime as _datetime
from nose import tools as _tools

from toolbox import bigquery_sink as _bs


def test_extract_simple():
    field = _bs.SchemaField(name="hello", field_type=_bs.FieldType.INTEGER)
    value = field.extract(row={"hello": 1})
    _tools.assert_equal(value, 1)


def test_extract_source_path():
    field = _bs.SchemaField(
        name="hello", field_type=_bs.FieldType.INTEGER, source_path=["world"]
    )
    value = field.extract(row={"world": 1})
    _tools.assert_equal(value, 1)

    missing_value = field.extract(row={"hello": 1})
    _tools.assert_is_none(missing_value)


def test_extract_source_fn():
    field = _bs.SchemaField(
        name="hello", field_type=_bs.FieldType.INTEGER, source_fn=lambda row, path: 5
    )
    value = field.extract(row={})
    _tools.assert_equal(value, 5)


def test_extract_source_path_deep():
    field = _bs.SchemaField(
        name="hello",
        field_type=_bs.FieldType.STRUCT,
        source_path=["world"],
        fields=[
            _bs.SchemaField(
                name="foo", field_type=_bs.FieldType.INTEGER, source_path=["bar"]
            )
        ],
    )
    value = field.extract(row={"world": {"bar": 1}})
    _tools.assert_equal(value["foo"], 1)


def test_extract_repeated():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.STRING,
        source_path=["world"],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(row={"world": ["a", "b", "c"]})
    _tools.assert_list_equal(value, ["a", "b", "c"])


def test_extract_repeated_deep():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.STRUCT,
        fields=[
            _bs.SchemaField(
                name="inner",
                field_type=_bs.FieldType.INTEGER,
                source_path=["inner_value"],
            )
        ],
        source_path=["world"],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(
        row={
            "world": [{"inner_value": 1}, {"inner_value": 2}, {"inner_value": 3}]
        }
    )
    _tools.assert_list_equal(value, [{"inner": 1}, {"inner": 2}, {"inner": 3}])


def test_extract_repeated_missing():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.STRING,
        source_path=["world"],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(row={"wrong_key": ["a", "b", "c"]})
    _tools.assert_list_equal(value, [])


def test_extract_root_reference():
    field = _bs.SchemaField(
        name="hello",
        field_type=_bs.FieldType.STRUCT,
        source_path=["hello"],
        fields=[
            _bs.SchemaField(
                name="foo", field_type=_bs.FieldType.STRING, source_path=[_bs.SourcePathElements.ROOT, "foo"]
            )
        ],
    )
    value = field.extract(row={"hello": "world", "foo": "bar"})
    _tools.assert_equal(value["foo"], "bar")


def test_extract_should_fire_exception():
    field = _bs.SchemaField(
        name="hello",
        field_type=_bs.FieldType.INTEGER,
    )
    value = field.extract(row={"hello": "world"}, should_fire_exception=lambda *x: False)
    _tools.assert_is_none(value)

    with _tools.assert_raises(ValueError):
        field.extract(row={"hello": "world"})

    with _tools.assert_raises(ValueError):
        field.extract(row={"hello": "world"}, should_fire_exception=lambda *x: True)


def test_extract_repeated_not_ensured_type():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.INTEGER,
        source_path=["world"],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(row={"world": ["a", "b", "c"]}, should_ensure_type=False)
    _tools.assert_list_equal(value, ["a", "b", "c"])


def test_ensure_type_integer():
    field = _bs.SchemaField(
        name="int",
        field_type=_bs.FieldType.INTEGER,
    )
    with _tools.assert_raises(ValueError):
        field._ensure_type(value='a')

    _tools.assert_equal(field._ensure_type(value='1'), 1)


def test_ensure_type_float():
    field = _bs.SchemaField(
        name="float",
        field_type=_bs.FieldType.FLOAT,
    )
    with _tools.assert_raises(ValueError):
        field._ensure_type(value='a')

    _tools.assert_equal(field._ensure_type(value='1.2'), 1.2)


def test_ensure_type_string():
    field = _bs.SchemaField(
        name="str",
        field_type=_bs.FieldType.STRING,
    )

    _tools.assert_equal(field._ensure_type(value=1), '1')


def test_ensure_type_date():
    field = _bs.SchemaField(
        name="date",
        field_type=_bs.FieldType.DATE,
    )
    now = _datetime.datetime.utcnow()
    today = now.date()

    _tools.assert_equal(field._ensure_type(value=now), today)
    _tools.assert_equal(field._ensure_type(value=300), _datetime.date(1970, 1, 1))


def test_ensure_type_timestamp():
    field = _bs.SchemaField(
        name="date",
        field_type=_bs.FieldType.TIMESTAMP,
    )
    now = _datetime.datetime.utcnow()
    _tools.assert_equal(field._ensure_type(value=now), now)
    _tools.assert_equal(field._ensure_type(value=300), _datetime.datetime(1970, 1, 1, 0, 5, 0))


def test_ensure_type_boolean():
    field = _bs.SchemaField(
        name="bool",
        field_type=_bs.FieldType.BOOLEAN,
    )
    _tools.assert_equal(field._ensure_type(value=1), True)
    _tools.assert_equal(field._ensure_type(value=True), True)


if __name__ == "__main__":
    test_extract_repeated_missing()
