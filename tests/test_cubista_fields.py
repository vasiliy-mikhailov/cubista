import pytest
import cubista
import pandas as pd
import datetime

def test_when_field_has_int_type_but_data_frame_has_not_int_data_type_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField()

    data = {
        "id": ["Wrong", "Datatype"]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.FieldTypeMismatch):
        _ = Table(data_frame=data_frame)

def test_when_field_has_int_type_and_data_frame_has_int_data_type_without_nulls_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(nulls=False, unique=True, primary_key=True)

    data = {
        "id": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    _ = Table(data_frame=data_frame)

def test_when_field_has_int_type_and_data_frame_has_int_data_type_with_nulls_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(nulls=True)
            pk = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [1, 2, None],
        "pk": [1, 2, 3]
    }

    data_frame = pd.DataFrame(data)

    _ = Table(data_frame=data_frame)

def test_when_field_has_int_type_and_is_required_but_data_frame_has_nulls_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField()

    data_with_nulls = {
        "id": [1, 2, None]
    }

    data_frame = pd.DataFrame(data_with_nulls)

    with pytest.raises(cubista.NullsNotAllowed):
        _ = Table(data_frame=data_frame)

def test_when_field_has_int_type_and_is_not_required_and_data_frame_has_nulls_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(nulls=True)
            pk = cubista.IntField(primary_key=True, unique=True)

    data_with_nulls = {
        "id": [1, 2, None],
        "pk": [1, 2, 3]
    }

    data_frame = pd.DataFrame(data_with_nulls)

    _ = Table(data_frame=data_frame)

def test_when_field_has_int_type_and_requires_unique_but_value_repeats_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(unique=True)

    data_with_repeating_values = {
        "id": [1, 2, 2]
    }

    data_frame = pd.DataFrame(data_with_repeating_values)

    with pytest.raises(cubista.NonUniqueValuesFound):
        _ = Table(data_frame=data_frame)

def test_when_field_has_int_type_and_requires_unique_and_null_value_repeats_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(nulls=True, unique=True)
            pk = cubista.IntField(primary_key=True, unique=True)

    data_with_repeating_nulls = {
        "id": [1, 2, None, None],
        "pk": [1, 2, 3, 4]
    }

    data_frame = pd.DataFrame(data_with_repeating_nulls)

    _ = Table(data_frame=data_frame)

def test_when_field_has_int_type_and_primary_key_and_not_unique_raises_exception():
    with pytest.raises(cubista.PrimaryKeyMustBeUnique):
        class _(cubista.Table):
            class Fields:
                id = cubista.IntField(unique=False, primary_key=True)

def test_when_field_has_int_type_and_primary_key_and_allows_nulls_raises_exception():
    with pytest.raises(cubista.PrimaryKeyCannotHaveNulls):
        class _(cubista.Table):
            class Fields:
                id = cubista.IntField(unique=True, primary_key=True, nulls=True)

def test_when_field_has_string_type_but_data_frame_has_not_object_data_type_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.StringField()

    data_with_wrong_data_types = {
        "id": [1, 2]
    }

    data_frame = pd.DataFrame(data_with_wrong_data_types)

    with pytest.raises(cubista.FieldTypeMismatch):
        _ = Table(data_frame=data_frame)

def test_when_field_has_string_type_and_data_frame_has_object_data_type_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.StringField()
            pk = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": ["Correct", "data"],
        "pk": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    _ = Table(data_frame=data_frame)

def test_when_field_has_string_type_and_does_not_allow_nulls_but_data_frame_has_nulls_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.StringField()

    data = {
        "id": ["Correct", "data type", None]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.NullsNotAllowed):
        _ = Table(data_frame=data_frame)

def test_when_field_has_string_type_and_allows_nulls_and_data_frame_has_nulls_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.StringField(nulls=True)
            pk = cubista.IntField(primary_key=True, unique=True)

    data_with_nulls = {
        "id": ["Correct", "data type", None],
        "pk": [1, 2, 3]
    }

    data_frame = pd.DataFrame(data_with_nulls)

    _ = Table(data_frame=data_frame)

def test_when_field_has_string_type_and_requires_unique_but_value_repeats_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.StringField(unique=True)

    data_with_repeating_values = {
        "id": ["Hello", "Hello"]
    }

    data_frame = pd.DataFrame(data_with_repeating_values)

    with pytest.raises(cubista.NonUniqueValuesFound):
        _ = Table(data_frame=data_frame)

def test_when_field_has_string_type_and_primary_key_and_not_unique_raises_exception():
    with pytest.raises(cubista.PrimaryKeyMustBeUnique):
        class _(cubista.Table):
            class Fields:
                id = cubista.StringField(unique=False, primary_key=True)

def test_when_field_has_string_type_and_primary_key_and_allows_nulls_raises_exception():
    with pytest.raises(cubista.PrimaryKeyCannotHaveNulls):
        class _(cubista.Table):
            class Fields:
                id = cubista.StringField(unique=True, primary_key=True, nulls=True)

def test_when_field_has_float_type_but_data_frame_has_not_float64_data_type_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.FloatField()

    data_with_wrong_data_types = {
        "id": ["Wrong", "data type", None]
    }

    data_frame = pd.DataFrame(data_with_wrong_data_types)

    with pytest.raises(cubista.FieldTypeMismatch):
        _ = Table(data_frame=data_frame)

def test_when_field_has_float_type_and_data_frame_has_float64_data_type_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.FloatField()
            pk = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [3.1415, 2.718],
        "pk": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    _ = Table(data_frame=data_frame)

def test_when_field_has_float_type_and_does_not_allow_nulls_but_data_frame_has_nulls_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.FloatField()

    data = {
        "id": [3.14, 2.718, None]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.NullsNotAllowed):
        _ = Table(data_frame=data_frame)

def test_when_field_has_float_type_and_allows_nulls_and_data_frame_has_nulls_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.FloatField(nulls=True)
            pk = cubista.IntField(primary_key=True, unique=True)

    data_with_nulls = {
        "id": [3.14, 2.718, None],
        "pk": [1, 2, 3]
    }

    data_frame = pd.DataFrame(data_with_nulls)

    _ = Table(data_frame=data_frame)


def test_when_field_has_float_type_and_requires_unique_but_value_repeats_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.FloatField(unique=True)

    data_with_repeating_values = {
        "id": [1.0, 1.0]
    }

    data_frame = pd.DataFrame(data_with_repeating_values)

    with pytest.raises(cubista.NonUniqueValuesFound):
        _ = Table(data_frame=data_frame)

def test_when_field_has_float_type_and_primary_key_and_not_unique_raises_exception():
    with pytest.raises(cubista.PrimaryKeyMustBeUnique):
        class _(cubista.Table):
            class Fields:
                id = cubista.FloatField(unique=False, primary_key=True)

def test_when_field_has_float_type_and_primary_key_and_allows_nulls_raises_exception():
    with pytest.raises(cubista.PrimaryKeyCannotHaveNulls):
        class _(cubista.Table):
            class Fields:
                id = cubista.FloatField(unique=True, primary_key=True, nulls=True)

def test_when_field_has_bool_type_but_data_frame_has_not_bool_data_type_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.BoolField()

    data_with_wrong_data_types = {
        "id": ["Wrong", "data type", None]
    }

    data_frame = pd.DataFrame(data_with_wrong_data_types)

    with pytest.raises(cubista.FieldTypeMismatch):
        _ = Table(data_frame=data_frame)


def test_when_field_has_bool_type_and_data_frame_has_bool_data_type_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.BoolField()
            pk = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [True, False],
        "pk": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    _ = Table(data_frame=data_frame)

def test_when_field_has_bool_type_and_does_not_allow_nulls_but_data_frame_has_nulls_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.BoolField()

    data = {
        "id": [True, False, None]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.NullsNotAllowed):
        _ = Table(data_frame=data_frame)

def test_when_field_has_bool_type_and_allows_nulls_and_data_frame_has_nulls_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.BoolField(nulls=True)
            pk = cubista.IntField(primary_key=True, unique=True)

    data_with_nulls = {
        "id": [True, False, None],
        "pk": [1, 2, 3]
    }

    data_frame = pd.DataFrame(data_with_nulls)

    _ = Table(data_frame=data_frame)

def test_when_field_has_bool_type_and_requires_unique_but_value_repeats_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.BoolField(unique=True)

    data_with_repeating_values = {
        "id": [True, True]
    }

    data_frame = pd.DataFrame(data_with_repeating_values)

    with pytest.raises(cubista.NonUniqueValuesFound):
        _ = Table(data_frame=data_frame)

def test_when_field_has_bool_type_and_primary_key_and_not_unique_raises_exception():
    with pytest.raises(cubista.PrimaryKeyMustBeUnique):
        class _(cubista.Table):
            class Fields:
                id = cubista.BoolField(unique=False, primary_key=True)

def test_when_field_has_bool_type_and_primary_key_and_allows_nulls_raises_exception():
    with pytest.raises(cubista.PrimaryKeyCannotHaveNulls):
        class _(cubista.Table):
            class Fields:
                id = cubista.BoolField(unique=True, primary_key=True, nulls=True)

def test_when_field_has_date_type_but_data_frame_has_not_date_data_type_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.DateField()

    data_with_wrong_data_types = {
        "id": ["Wrong", None]
    }

    data_frame = pd.DataFrame(data_with_wrong_data_types)

    with pytest.raises(cubista.FieldTypeMismatch):
        _ = Table(data_frame=data_frame)

def test_when_field_has_date_type_and_data_frame_has_date_data_type_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.DateField()
            pk = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [datetime.datetime.date(datetime.datetime.now())],
        "pk": [1]
    }

    data_frame = pd.DataFrame(data)

    _ = Table(data_frame=data_frame)

def test_when_field_has_date_type_and_does_not_allow_nulls_but_data_frame_has_nulls_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.DateField()

    data = {
        "id": [datetime.datetime.date(datetime.datetime.now()), None]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.NullsNotAllowed):
        _ = Table(data_frame=data_frame)

def test_when_field_has_date_type_and_allows_nulls_and_data_frame_has_nulls_does_not_raise_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.DateField(nulls=True)
            pk = cubista.IntField(primary_key=True, unique=True)

    data_with_nulls = {
        "id": [datetime.datetime.date(datetime.datetime.now()), None],
        "pk": [1, 2]
    }

    data_frame = pd.DataFrame(data_with_nulls)

    _ = Table(data_frame=data_frame)

def test_when_field_has_date_type_and_requires_unique_but_value_repeats_raises_exception():
    class Table(cubista.Table):
        class Fields:
            id = cubista.DateField(unique=True)

    data_with_repeating_values = {
        "id": [datetime.datetime.date(datetime.datetime.now()), datetime.datetime.date(datetime.datetime.now())]
    }

    data_frame = pd.DataFrame(data_with_repeating_values)

    with pytest.raises(cubista.NonUniqueValuesFound):
        _ = Table(data_frame=data_frame)

def test_when_field_has_date_type_and_primary_key_and_not_unique_raises_exception():
    with pytest.raises(cubista.PrimaryKeyMustBeUnique):
        class _(cubista.Table):
            class Fields:
                id = cubista.DateField(unique=False, primary_key=True)

def test_when_field_has_bool_type_and_primary_key_and_allows_nulls_raises_exception():
    with pytest.raises(cubista.PrimaryKeyCannotHaveNulls):
        class _(cubista.Table):
            class Fields:
                id = cubista.DateField(unique=True, primary_key=True, nulls=True)

