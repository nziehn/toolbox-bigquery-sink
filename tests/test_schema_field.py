import datetime as _datetime
from nose import tools as _tools

from toolbox import bigquery_sink as _bs
from toolbox.bigquery_sink import SourcePathElements


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


def test_extract_source_path_with_fn_in_path():
    field = _bs.SchemaField(
        name="hello",
        field_type=_bs.FieldType.STRING,
        source_path=[
            "world",
            lambda x, y: [1],
            "sub1",
            "sub2",
        ],
    )
    value = field.extract(
        row={
            "world": [
                {"test_key": "a", "sub1": {"sub2": "z"}},
                {"test_key": "b", "sub1": {"sub2": "y"}},
                {"test_key": "c", "sub1": {"sub2": "x"}},
                {"test_key": "d", "sub1": {"sub2": "w"}},
                {"test_key": "e", "sub1": {"sub2": "v"}},
            ]
        }
    )
    _tools.assert_equal("y", value)


def test_extract_source_path_with_idx_search_in_path():
    field = _bs.SchemaField(
        name="hello",
        field_type=_bs.FieldType.STRING,
        source_path=[
            "world",
            SourcePathElements.find_list_index_with_key_value("test_key", "c"),
            "sub1",
            "sub2",
        ],
    )
    value = field.extract(
        row={
            "world": [
                {"test_key": "a", "sub1": {"sub2": "z"}},
                {"test_key": "b", "sub1": {"sub2": "y"}},
                {"test_key": "c", "sub1": {"sub2": "x"}},
                {"test_key": "d", "sub1": {"sub2": "w"}},
                {"test_key": "e", "sub1": {"sub2": "v"}},
            ]
        }
    )
    _tools.assert_equal("x", value)


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


def test_extract_repeated_deep_skip():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.INTEGER,
        source_path=[
            "outter",
            _bs.SourcePathElements.LIST_INDEX,
            "level1",
            _bs.SourcePathElements.LIST_INDEX,
            "level2",
        ],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(
        row={
            "outter": [
                {"level1": [{"level2": 1}]},
                {"level1": [{"level2": 2}]},
                {"level1": [{"level2": 3}]},
            ]
        }
    )
    _tools.assert_list_equal(value, [1, 2, 3])


def test_extract_repeated_unroll():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.INTEGER,
        source_path=[
            "outter",
            _bs.SourcePathElements.LIST_INDEX,
            "level1",
            _bs.SourcePathElements.LIST_INDEX,
        ],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(
        row={
            "outter": [
                {"level1": [1, 2]},
                {"level1": [3, 4]},
                {"level1": [5, 6]},
            ]
        }
    )
    _tools.assert_list_equal(value, [1, 2, 3, 4, 5, 6])


def test_extract_repeated_unroll_with_structs():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.STRUCT,
        source_path=[
            "outter",
            _bs.SourcePathElements.LIST_INDEX,
            "level1",
            _bs.SourcePathElements.LIST_INDEX,
        ],
        fields=[
            _bs.SchemaField(
                name="value",
                field_type=_bs.FieldType.INTEGER,
                source_path=[],
            )
        ],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(
        row={
            "outter": [
                {"level1": [1, 2]},
                {"level1": [3, 4]},
                {"level1": [5, 6]},
            ]
        }
    )
    _tools.assert_list_equal(
        value,
        [
            {"value": 1},
            {"value": 2},
            {"value": 3},
            {"value": 4},
            {"value": 5},
            {"value": 6},
        ],
    )


def test_extract_repeated_unroll_with_struct_root_reference():
    field = _bs.SchemaField(
        name="repeated",
        field_type=_bs.FieldType.STRUCT,
        source_path=[
            "outter",
            _bs.SourcePathElements.LIST_INDEX,
            "level1",
            _bs.SourcePathElements.LIST_INDEX,
        ],
        fields=[
            _bs.SchemaField(
                name="value",
                field_type=_bs.FieldType.INTEGER,
                source_path=[],
            ),
            _bs.SchemaField(
                name="other",
                field_type=_bs.FieldType.INTEGER,
                source_path=[
                    _bs.SourcePathElements.ROOT,
                    "outter",
                    _bs.SourcePathElements.LIST_INDEX,
                    "other",
                ],
            ),
        ],
        mode=_bs.FieldMode.REPEATED,
    )
    value = field.extract(
        row={
            "outter": [
                {"level1": [1, 2], "other": 20},
                {"level1": [3, 4], "other": 30},
                {"level1": [5, 6], "other": 40},
            ]
        }
    )
    _tools.assert_list_equal(
        value,
        [
            {"value": 1, "other": 20},
            {"value": 2, "other": 20},
            {"value": 3, "other": 30},
            {"value": 4, "other": 30},
            {"value": 5, "other": 40},
            {"value": 6, "other": 40},
        ],
    )


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
        row={"world": [{"inner_value": 1}, {"inner_value": 2}, {"inner_value": 3}]}
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
                name="foo",
                field_type=_bs.FieldType.STRING,
                source_path=[_bs.SourcePathElements.ROOT, "foo"],
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
    value = field.extract(
        row={"hello": "world"}, should_fire_exception=lambda *x: False
    )
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
        field._ensure_type(value="a")

    _tools.assert_equal(field._ensure_type(value="1"), 1)


def test_ensure_type_float():
    field = _bs.SchemaField(
        name="float",
        field_type=_bs.FieldType.FLOAT,
    )
    with _tools.assert_raises(ValueError):
        field._ensure_type(value="a")

    _tools.assert_equal(field._ensure_type(value="1.2"), 1.2)


def test_ensure_type_string():
    field = _bs.SchemaField(
        name="str",
        field_type=_bs.FieldType.STRING,
    )

    _tools.assert_equal(field._ensure_type(value=1), "1")


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
    _tools.assert_equal(
        field._ensure_type(value=300), _datetime.datetime(1970, 1, 1, 0, 5, 0)
    )


def test_ensure_type_boolean():
    field = _bs.SchemaField(
        name="bool",
        field_type=_bs.FieldType.BOOLEAN,
    )
    _tools.assert_equal(field._ensure_type(value=1), True)
    _tools.assert_equal(field._ensure_type(value=True), True)


if __name__ == "__main__":
    test_extract_repeated_unroll_with_struct_root_reference()
