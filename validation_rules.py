from pyspark.sql.functions import col, when, lit, concat_ws, length

def not_null_rule(column):
    
    return when(col(column).isNull(), lit("Column should not be NULL")).otherwise(lit(""))

def date_format_rule(column, pattern=r"^\d{4}-\d{2}-\d{2}$"):
   
    return when(~col(column).rlike(pattern), lit("column have and INVALID_DATE")).otherwise(lit(""))


def check_varchar_5(column):
    return when(length(col(column)) > 5, lit("Input exceeds the maximum allowed length of 5 characters."))


def concat_errors(*args):
   
    return concat_ws("", *args)