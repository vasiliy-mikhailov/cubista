import pytest

import cubista
import pandas as pd

def test_when_data_source_is_created_table_knows_its_data_source():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [1, 2]
    }
    data_frame = pd.DataFrame(data)

    table = Table(data_frame=data_frame)

    data_source = cubista.DataSource(tables=[table])

    assert table.data_source == data_source

def test_when_data_source_is_created_table_can_be_found_by_class_name():
    class Table(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)

    data = {
        "id": [1, 2]
    }
    data_frame = pd.DataFrame(data)

    table = Table(data_frame=data_frame)

    data_source = cubista.DataSource(tables=[table])

    assert data_source.tables[Table] == table

def test_when_column_referencing_primary_key_of_another_table_contains_value_not_from_that_primary_key_it_is_replaced_with_default_value():
    class Table1(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)

    class Table2(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            table1_id = cubista.ForeignKey(lambda: Table1, default=-1)

    data1 = {
        "id": [1, 2]
    }
    data_frame1 = pd.DataFrame(data1)
    table1 = Table1(data_frame=data_frame1)

    data2 = {
        "id": [1, 2],
        "table1_id": [3, 3]
    }
    data_frame2 = pd.DataFrame(data2)
    table2 = Table2(data_frame=data_frame2)

    _ = cubista.DataSource(tables=[
        table1,
        table2,
    ])

    assert table2.data_frame["table1_id"].tolist() == [-1, -1]

def test_when_column_pulled_from_another_table_value_migrates():
    class Table1(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            name = cubista.StringField()

    class Table2(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            table1_id = cubista.ForeignKey(lambda: Table1, default=-1)
            table1_name = cubista.PullByForeignKey(lambda: Table1, source_field="name")

    data1 = {
        "id": [1, 2],
        "name": ["one", "two"]
    }
    data_frame1 = pd.DataFrame(data1)
    table1 = Table1(data_frame=data_frame1)

    data2 = {
        "id": [1, 2],
        "table1_id": [1, 1]
    }
    data_frame2 = pd.DataFrame(data2)
    table2 = Table2(data_frame=data_frame2)

    _ = cubista.DataSource(tables=[
        table1,
        table2,
    ])

    assert table2.data_frame["table1_name"].tolist() == ["one", "two"]

def test_when_column_pulled_from_another_table_value_migrates_by_field_chain():
    class Table1(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            name = cubista.StringField()

    class Table2(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            table1_id = cubista.ForeignKey(lambda: Table1, default=-1)
            table1_name = cubista.PullByForeignKey(lambda: Table1, source_field="name")

    class Table3(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            table2_id = cubista.ForeignKey(lambda: Table2, default=-1)
            table1_name = cubista.PullByForeignKey(lambda: Table2, source_field="table1_name")

    data1 = {
        "id": [1, 2],
        "name": ["one", "two"]
    }
    data_frame1 = pd.DataFrame(data1)
    table1 = Table1(data_frame=data_frame1)

    data2 = {
        "id": [1, 2],
        "table1_id": [1, 1]
    }
    data_frame2 = pd.DataFrame(data2)
    table2 = Table2(data_frame=data_frame2)

    data3 = {
        "id": [1, 2],
        "table2_id": [1, 1]
    }
    data_frame3 = pd.DataFrame(data3)
    table3 = Table3(data_frame=data_frame3)

    _ = cubista.DataSource(tables=[
        table3,
        table2,
        table1,
    ])

    assert table2.data_frame["table1_name"].tolist() == ["one", "two"]

def test_when_column_cannot_be_pulled_from_another_table_exception_is_raised():
    class Table1(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            name = cubista.StringField()

    class Table2MissedNameAttributeMigration(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            table1_id = cubista.ForeignKey(lambda: Table1, default=-1)

    class Table3(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            table2_id = cubista.ForeignKey(lambda: Table2MissedNameAttributeMigration, default=-1)
            table1_name = cubista.PullByForeignKey(lambda: Table2MissedNameAttributeMigration, source_field="table1_name")

    data1 = {
        "id": [1, 2],
        "name": ["one", "two"]
    }
    data_frame1 = pd.DataFrame(data1)
    table1 = Table1(data_frame=data_frame1)

    data2 = {
        "id": [1, 2],
        "table1_id": [1, 1]
    }
    data_frame2 = pd.DataFrame(data2)
    table2 = Table2MissedNameAttributeMigration(data_frame=data_frame2)

    data3 = {
        "id": [1, 2],
        "table2_id": [1, 1]
    }
    data_frame3 = pd.DataFrame(data3)
    table3 = Table3(data_frame=data_frame3)

    with pytest.raises(cubista.CannotEvaluateFields):
        _ = cubista.DataSource(tables=[
            table3,
            table2,
            table1,
        ])

def test_when_field_is_calculated_it_is_evaluated():
    class Table1(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            name = cubista.StringField()
            name_length = cubista.CalculatedField(lambda x: len(x["name"]), source_fields=["name"])

    data1 = {
        "id": [1, 2, 3],
        "name": ["one", "two", "three"]
    }
    data_frame1 = pd.DataFrame(data1)
    table1 = Table1(data_frame=data_frame1)

    _ = cubista.DataSource(tables=[
        table1
    ])

    assert table1.data_frame["name_length"].tolist() == [3, 3, 5]

def test_when_field_is_calculated_only_source_fields_are_sent_to_lambda():
    class Table1(cubista.Table):
        class Fields:
            id = cubista.IntField(primary_key=True, unique=True)
            name = cubista.StringField()
            email = cubista.StringField()
            source_fields_contain_only_index = cubista.CalculatedField(lambda x: x.index.tolist() == ["name", "email"], source_fields=["name", "email"])

    data1 = {
        "id": [1],
        "name": ["one"],
        "email": ["a@b.com"]
    }
    data_frame1 = pd.DataFrame(data1)
    table1 = Table1(data_frame=data_frame1)

    _ = cubista.DataSource(tables=[
        table1
    ])

    assert table1.data_frame["source_fields_contain_only_index"].tolist() == [True]
