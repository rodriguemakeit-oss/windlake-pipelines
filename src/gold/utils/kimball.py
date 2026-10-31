from pyspark.sql.functions import monotonically_increasing_id

def add_id_column(df, id_col_name="id"):
    return df.withColumn(id_col_name, monotonically_increasing_id() + 1)
