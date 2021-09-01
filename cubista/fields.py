import datetime
from .exceptions import *
import pandas as pd

class Field:
    def __init__(self):
        self.name = ''
        self.table = None

    def check_field_data_type_nulls_and_uniqueness_in_data_frame_column_raise_exception_otherwise(
            self,
            data,
            required_types,
            nulls,
            unique
    ):
        name = self.name
        for _, value in data.items():
            if not nulls and pd.isnull(value):
                raise NullsNotAllowed("Field {} cannot contain nulls but null found.".format(name))

            data_type = type(value)
            if value and data_type not in required_types:
                raise FieldTypeMismatch("Field {} must have data type {}, but {} found.".format(name, required_types, data_type))

        if unique:
            counts = data.value_counts()
            repeating_values = counts[counts > 1].index.to_list()
            if repeating_values:
                raise NonUniqueValuesFound("Field {} must have unique values, but has repeating value(s): {}.".format(name, repeating_values))

    def __str__(self):
        name = self.name
        table = self.table
        return "{}.{}".format(type(table), name)

class IntField(Field):
    def __init__(self, nulls=False, unique=False, primary_key=False):
        if primary_key and not unique:
            raise PrimaryKeyMustBeUnique()

        if primary_key and nulls:
            raise PrimaryKeyCannotHaveNulls

        self.nulls = nulls
        self.unique = unique
        self.primary_key = primary_key
        self.evaluated = False

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        nulls = self.nulls
        unique = self.unique
        self.check_field_data_type_nulls_and_uniqueness_in_data_frame_column_raise_exception_otherwise(
            data=data,
            required_types=[int, float],
            nulls=nulls,
            unique=unique
        )

    def do_nothing_intentionally(self):
        pass

    def check_references_raise_exception_otherwise(self):
        self.do_nothing_intentionally()

class StringField(Field):
    def __init__(self, nulls=False, unique=False, primary_key=False):
        if primary_key and not unique:
            raise PrimaryKeyMustBeUnique()

        if primary_key and nulls:
            raise PrimaryKeyCannotHaveNulls

        self.nulls = nulls
        self.unique = unique
        self.primary_key = primary_key
        self.evaluated = False

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        nulls = self.nulls
        unique = self.unique
        self.check_field_data_type_nulls_and_uniqueness_in_data_frame_column_raise_exception_otherwise(
            data=data,
            required_types=[str],
            nulls=nulls,
            unique=unique
        )

    def do_nothing_intentionally(self):
        pass

    def check_references_raise_exception_otherwise(self):
        self.do_nothing_intentionally()

class FloatField(Field):
    def __init__(self, nulls=False, unique=False, primary_key=False):
        if primary_key and not unique:
            raise PrimaryKeyMustBeUnique()

        if primary_key and nulls:
            raise PrimaryKeyCannotHaveNulls

        self.nulls = nulls
        self.unique = unique
        self.primary_key = primary_key
        self.evaluated = False

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        nulls = self.nulls
        unique = self.unique
        self.check_field_data_type_nulls_and_uniqueness_in_data_frame_column_raise_exception_otherwise(
            data=data,
            required_types=[float],
            nulls=nulls,
            unique=unique
        )

    def do_nothing_intentionally(self):
        pass

    def check_references_raise_exception_otherwise(self):
        self.do_nothing_intentionally()

class BoolField(Field):
    def __init__(self, nulls=False, unique=False, primary_key=False):
        if primary_key and not unique:
            raise PrimaryKeyMustBeUnique()

        if primary_key and nulls:
            raise PrimaryKeyCannotHaveNulls

        self.nulls = nulls
        self.unique = unique
        self.primary_key = primary_key
        self.evaluated = False

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        nulls = self.nulls
        unique = self.unique
        self.check_field_data_type_nulls_and_uniqueness_in_data_frame_column_raise_exception_otherwise(
            data=data,
            required_types=[bool],
            nulls=nulls,
            unique=unique
        )

    def do_nothing_intentionally(self):
        pass

    def check_references_raise_exception_otherwise(self):
        self.do_nothing_intentionally()

class DateField(Field):
    def __init__(self, nulls=False, unique=False, primary_key=False):
        if primary_key and not unique:
            raise PrimaryKeyMustBeUnique()

        if primary_key and nulls:
            raise PrimaryKeyCannotHaveNulls

        self.nulls = nulls
        self.unique = unique
        self.primary_key = primary_key
        self.evaluated = False

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        nulls = self.nulls
        unique = self.unique
        self.check_field_data_type_nulls_and_uniqueness_in_data_frame_column_raise_exception_otherwise(
            data=data,
            required_types=[datetime.date],
            nulls=nulls,
            unique=unique
        )

    def do_nothing_intentionally(self):
        pass

    def check_references_raise_exception_otherwise(self):
        self.do_nothing_intentionally()

class ForeignKey(Field):
    def __init__(self, to, default, nulls=False):
        self.to = to
        self.default = default
        self.nulls = nulls
        self.primary_key = False
        self.evaluated = False

    def do_nothing_intentionally(self):
        pass

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        self.do_nothing_intentionally()

    def check_references_raise_exception_otherwise(self):
        table = self.table

        data_frame = table.data_frame

        field_name = self.name

        to = self.to

        referenced_table_type = to()

        data_source = table.data_source

        referenced_table = data_source.tables[referenced_table_type]

        referenced_table_primary_key_field_name = referenced_table.get_primary_key_field_name()

        referenced_values = referenced_table.data_frame[referenced_table_primary_key_field_name].dropna().unique()

        default_value_for_referencing_nowhere = self.default

        data_frame.loc[~data_frame[field_name].isin(referenced_values), field_name] = default_value_for_referencing_nowhere

class PullByForeignKey(Field):
    def __init__(self, to, source_field):
        self.to = to
        self.source_field = source_field
        self.primary_key = False
        self.evaluated = True

    def do_nothing_intentionally(self):
        pass

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        self.do_nothing_intentionally()

    def check_references_raise_exception_otherwise(self):
        self.do_nothing_intentionally()

    def get_referenced_table(self):
        table = self.table

        to = self.to

        referenced_table_type = to()

        data_source = table.data_source

        referenced_table = data_source.tables[referenced_table_type]

        return referenced_table

    def is_ready_to_be_evaluated(self):
        referenced_table = self.get_referenced_table()
        referenced_column_name = self.source_field
        referenced_data_frame = referenced_table.data_frame
        referenced_column_is_evaluated = referenced_column_name in referenced_data_frame.columns

        return referenced_column_is_evaluated

    def evaluate(self):
        source_field = self.source_field
        field_name = self.name
        table = self.table
        data_frame = table.data_frame
        primary_key_field_name = table.get_primary_key_field_name()
        referenced_table = self.get_referenced_table()
        referenced_table_primary_key_field_name = referenced_table.get_primary_key_field_name()

        reduced_reference_data_frame = referenced_table.data_frame[[referenced_table_primary_key_field_name, source_field]]\
            .set_index(referenced_table_primary_key_field_name)\
            .rename(columns={ source_field: field_name })

        new_data_frame = data_frame.merge(
            reduced_reference_data_frame,
            how="left",
            left_on=primary_key_field_name,
            right_index=True,
            suffixes=(None, "")
        )

        table.data_frame = new_data_frame

class CalculatedField(Field):
    def __init__(self, lambda_expression, source_fields):
        self.lambda_expression = lambda_expression
        self.source_fields = source_fields
        self.primary_key = False
        self.evaluated = True

    def do_nothing_intentionally(self):
        pass

    def check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(self, data):
        self.do_nothing_intentionally()

    def check_references_raise_exception_otherwise(self):
        self.do_nothing_intentionally()

    def is_ready_to_be_evaluated(self):
        table = self.table
        data_frame = table.data_frame
        columns = data_frame.columns

        source_fields = self.source_fields

        non_existent_fields = [source_field for source_field in source_fields if source_field not in columns]

        return len(non_existent_fields) == 0

    def evaluate(self):
        table = self.table
        data_frame = table.data_frame
        field_name = self.name
        lambda_expression = self.lambda_expression
        source_fields = self.source_fields
        data_frame[field_name] = data_frame[source_fields].apply(
            lambda_expression,
            axis=1
        )