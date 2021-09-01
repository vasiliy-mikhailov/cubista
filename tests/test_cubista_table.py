import cubista
import pandas as pd
import pytest

def test_when_table_is_created_but_field_does_not_exist_raises_exception():
    class Table(cubista.Table):
        class Fields:
            non_existent_field = cubista.IntField()

    data = {
        "id": [1, 2, 3],
        "data": ["Some", "Data", None]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.FieldDoesNotExist):
        _ = Table(data_frame=data_frame)

def test_when_table_is_created_fields_know_their_name():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    table = Table(data_frame=data_frame)

    assert table.Fields.id.name == 'id'

def test_when_table_is_created_fields_know_their_table():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    table = Table(data_frame=data_frame)

    assert table.Fields.id.table == table

def test_when_table_is_created_and_no_primary_key_specified_raises_exception():
    class TableWithoutPrimaryKey(cubista.Table):
        class Fields:
            id = cubista.IntField()

    data = {
        "id": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.NoPrimaryKeySpecified):
        _ = TableWithoutPrimaryKey(data_frame=data_frame)

def test_when_table_is_created_and_more_than_one_primary_key_specified_raises_exception():
    class TableWithTwoPrimaryKeys(cubista.Table):
        class Fields:
            pk1 = cubista.IntField(primary_key=True, unique=True)
            pk2 = cubista.IntField(primary_key=True, unique=True)

    data = {
        "pk1": [1, 2],
        "pk2": [1, 2]
    }

    data_frame = pd.DataFrame(data)

    with pytest.raises(cubista.MoreThanOnePrimaryKeySpecified):
        _ = TableWithTwoPrimaryKeys(data_frame=data_frame)
