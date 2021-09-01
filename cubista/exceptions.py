class FieldDoesNotExist(Exception):
    pass

class FieldTypeMismatch(Exception):
    pass

class NullsNotAllowed(Exception):
    pass

class NonUniqueValuesFound(Exception):
    pass

class NoPrimaryKeySpecified(Exception):
    pass

class MoreThanOnePrimaryKeySpecified(Exception):
    pass

class PrimaryKeyMustBeUnique(Exception):
    pass

class PrimaryKeyCannotHaveNulls(Exception):
    pass

class CannotEvaluateFields(Exception):
    pass