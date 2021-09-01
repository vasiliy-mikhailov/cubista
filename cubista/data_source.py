import cubista

class DataSource:
    def __init__(self, tables):
        self.tables = {type(table): table for table in tables}

        self.set_data_source_for_tables()
        self.check_references_raise_exception_otherwise()
        self.evaluate_tables()

    def set_data_source_for_tables(self):
        tables = self.tables
        for _, table in tables.items():
            table.data_source = self

    def check_references_raise_exception_otherwise(self):
        tables = self.tables
        for _, table in tables.items():
            table.check_references_raise_exception_otherwise()

    def get_fields_to_evaluate(self):
        tables = self.tables
        result = []

        for _, table in tables.items():
            result = result + table.get_fields_to_evaluate()

        return result

    def evaluate_tables(self):
        fields_to_evaluate = self.get_fields_to_evaluate()
        tables = self.tables

        while len(fields_to_evaluate) > 0:
            for _, table_object in tables.items():
                table_object.evaluate()

            not_evaluated_fields = self.get_fields_to_evaluate()

            if len(fields_to_evaluate) == len(not_evaluated_fields):
                raise cubista.CannotEvaluateFields("{}".format(", ".join([str(field) for field in not_evaluated_fields])))

            fields_to_evaluate = not_evaluated_fields