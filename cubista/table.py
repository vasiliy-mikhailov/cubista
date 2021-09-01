import pandas as pd

import cubista
from .exceptions import FieldDoesNotExist

class Table:
    class Fields:
        pass

    def __init__(self, data_frame):
        self.data_source = None
        self.data_frame = data_frame
        self.set_field_names_and_table()

        self.check_all_not_evaluated_fields_exist_in_data_frame_and_raise_exception_otherwise()
        self.check_all_not_evaluated_fields_has_correct_data_type_in_data_frame_and_raise_exception_otherwise()
        self.check_only_one_primary_key_specified_and_raise_exception_otherwise()

    def get_fields(self):
        return { key: value for key, value in self.Fields.__dict__.items() if not key.startswith("__")}

    def set_field_names_and_table(self):
        fields = self.get_fields()
        for field_name, field_object in fields.items():
            field_object.name = field_name
            field_object.table = self

    def check_all_not_evaluated_fields_exist_in_data_frame_and_raise_exception_otherwise(self):
        fields = self.get_fields()
        data_frame = self.data_frame
        data_frame_columns = data_frame.columns

        for field_name, field_object in fields.items():
            if field_object.is_evaluated():
                if field_name not in data_frame_columns:
                    raise FieldDoesNotExist("Field {} not found in {}".format(field_name, ", ".join(data_frame_columns)))

    def check_all_not_evaluated_fields_has_correct_data_type_in_data_frame_and_raise_exception_otherwise(self):
        fields = self.get_fields()
        data_frame = self.data_frame

        for field_name, field_object in fields.items():
            if field_object.is_evaluated():
                data = data_frame[field_name]
                field_object.check_field_has_correct_data_type_in_data_frame_column_raise_exception_otherwise(data=data)

    def check_only_one_primary_key_specified_and_raise_exception_otherwise(self):
        fields = self.get_fields()

        primary_keys_count = sum([field_object.primary_key for _, field_object in fields.items()])

        if not primary_keys_count:
            raise cubista.NoPrimaryKeySpecified("No primary key specified in {}.".format(type(self)))

        if primary_keys_count > 1:
            raise cubista.MoreThanOnePrimaryKeySpecified("Only one primary key is allowed for {} but {} found.".format(type(self), primary_keys_count))

    def check_references_raise_exception_otherwise(self):
        fields = self.get_fields()

        for field_name, field_object in fields.items():
            field_object.check_references_raise_exception_otherwise()

    def get_primary_key_field_name(self):
        fields = self.get_fields()

        for field_name, field_object in fields.items():
            if field_object.primary_key:
                return field_name

    def get_fields_to_evaluate(self):
        fields = self.get_fields()

        result = []

        for _, field_object in fields.items():
            if not field_object.is_evaluated():
                result.append(field_object)

        return result

    def evaluate(self):
        fields_to_evaluate = self.get_fields_to_evaluate()

        for field_to_evaluate in fields_to_evaluate:
            if field_to_evaluate.is_ready_to_be_evaluated():
                field_to_evaluate.evaluate()

class AggregatedTable(Table):
    class Aggregation:
        source = None
        sort_by: [str] = []
        group_by: [str] = []

    def __init__(self):
        data_frame = pd.DataFrame()
        super(AggregatedTable, self).__init__(data_frame=data_frame)

    def are_fields_evaluated_in_source_table(self, field_names):
        source_table_type = self.Aggregation.source()
        data_source = self.data_source

        source_table = data_source.tables[source_table_type]

        for field_name in field_names:
            field_object = source_table.Fields.__dict__[field_name]

            if not field_object.is_evaluated():
                return False

        return True

    def are_fields_required_for_aggregation_evaluated_in_source_table(self):
        fields = self.get_fields()

        for field_name, field_object in fields.items():
            if field_object.is_required_for_aggregation() and not field_object.is_ready_to_be_aggregated():
                return False

        return True

    def is_ready_to_be_aggregated(self):
        sort_by_field_names = self.Aggregation.sort_by

        if not self.are_fields_evaluated_in_source_table(field_names=sort_by_field_names):
            return False

        group_by_field_names = self.Aggregation.group_by

        if not self.are_fields_evaluated_in_source_table(field_names=group_by_field_names):
            return False

        if not self.are_fields_required_for_aggregation_evaluated_in_source_table():
            return False

        return True

    def get_aggregated_field_name_to_aggregate_function_mapping(self):
        fields = self.get_fields()

        result = {}

        for field_name, field_object in fields.items():
            if isinstance(field_object, cubista.AggregatedField):
                source_field = field_object.source
                aggregate_function = field_object.aggregate_function
                result[source_field] = aggregate_function

        return result

    def get_aggregated_source_field_name_to_destination_field_name_mapping(self):
        fields = self.get_fields()

        result = {}

        for field_name, field_object in fields.items():
            if isinstance(field_object, cubista.AggregatedField) or isinstance(field_object, cubista.GroupField):
                source_field = field_object.source
                result[source_field] = field_name

        return result

    def aggregate(self):
        source_table_type = self.Aggregation.source()
        data_source = self.data_source
        source_table = data_source.tables[source_table_type]
        aggregated_source_field_name_to_destination_field_name_mapping = self.get_aggregated_source_field_name_to_destination_field_name_mapping()
        reduced_field_names = aggregated_source_field_name_to_destination_field_name_mapping.keys()
        sort_by_field_names = self.Aggregation.sort_by
        group_by_field_names = self.Aggregation.group_by
        aggregated_field_name_to_aggregate_function_mapping = self.get_aggregated_field_name_to_aggregate_function_mapping()
        new_data_frame = source_table.data_frame
        new_data_frame = new_data_frame.sort_values(by=sort_by_field_names)
        new_data_frame = new_data_frame[reduced_field_names]
        new_data_frame = new_data_frame.groupby(group_by_field_names)
        new_data_frame = new_data_frame.agg(aggregated_field_name_to_aggregate_function_mapping)
        new_data_frame = new_data_frame.reset_index()

        new_data_frame = new_data_frame.rename(columns=aggregated_source_field_name_to_destination_field_name_mapping)

        primary_key_field_name = self.get_primary_key_field_name()

        new_data_frame[primary_key_field_name] = new_data_frame.apply(
            lambda x: -x.name - 2,
            axis=1
        )

        self.data_frame = new_data_frame

    def evaluate(self):
        is_ready_to_be_aggregated = self.is_ready_to_be_aggregated()

        if is_ready_to_be_aggregated:
            self.aggregate()

        super(AggregatedTable, self).evaluate()