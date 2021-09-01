import cubista

class DataSource:
    def __init__(self, tables):
        self.tables = {type(table): table for table in tables}

        self.set_data_source_for_tables()
        self.check_references_raise_exception_otherwise()
        self.evaluate_fields()

    def set_data_source_for_tables(self):
        tables = self.tables
        for _, table in tables.items():
            table.data_source = self

    def check_references_raise_exception_otherwise(self):
        tables = self.tables
        for _, table in tables.items():
            table.check_references_raise_exception_otherwise()

    def evaluate_fields(self):
        tables = self.tables

        fields_to_evaluate = []

        for _, table in tables.items():
            for _, field_object in table.get_fields().items():
                if field_object.evaluated:
                    fields_to_evaluate.append(field_object)

        print(fields_to_evaluate)

        while len(fields_to_evaluate) > 0:
            not_evaluated_fields = []

            for field_to_evaluate in fields_to_evaluate:
                if field_to_evaluate.is_ready_to_be_evaluated():
                    field_to_evaluate.evaluate()
                else:
                    not_evaluated_fields.append(field_to_evaluate)

            if len(fields_to_evaluate) == len(not_evaluated_fields):
                raise cubista.CannotEvaluateFields("{}".format(", ".join([str(field) for field in not_evaluated_fields])))

            fields_to_evaluate = not_evaluated_fields