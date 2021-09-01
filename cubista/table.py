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
            if not field_object.evaluated:
                if field_name not in data_frame_columns:
                    raise FieldDoesNotExist("Field {} not found in {}".format(field_name, ", ".join(data_frame_columns)))

    def check_all_not_evaluated_fields_has_correct_data_type_in_data_frame_and_raise_exception_otherwise(self):
        fields = self.get_fields()
        data_frame = self.data_frame

        for field_name, field_object in fields.items():
            if not field_object.evaluated:
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