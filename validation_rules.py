from pyspark.sql.functions import col, when, lit, concat_ws

def not_null_rule(column):
    """
    Returns a Spark SQL expression for not-null validation.
    :param column: Column name as string
    :return: Column expression with "NULL" if value is null, else empty string
    """
    return when(col(column).isNull(), lit("Column should not be NULL")).otherwise(lit(""))

def date_format_rule(column, pattern=r"^\d{4}-\d{2}-\d{2}$"):
    """
    Returns a Spark SQL expression for date format validation.
    :param column: Column name as string
    :param pattern: Regex pattern for date validation
    :return: Column expression with "INVALID_DATE" if format is wrong, else empty string
    """
    return when(~col(column).rlike(pattern), lit("column have and INVALID_DATE")).otherwise(lit(""))

def concat_errors(*args):
    """
    Concatenates multiple error columns, filtering out empty strings.
    :param args: Column expressions
    :return: Concatenated error messages
    """
    return concat_ws("", *args)