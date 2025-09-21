import pyspark.sql.functions as F

def filter_null_ids_names_and_cities(df):
    return df.filter(df.id.isNotNull() & df.name.isNotNull() & df.city.isNotNull())


def casting_coordinates_and_postal_code_types(df):
    df = df.withColumn("latitude", F.col("latitude").cast("double")) \
        .withColumn("longitude", F.col("longitude").cast("double")) \
        .withColumn(
            "postal_code",
            F.lpad(F.col("postal_code").cast("string"), 5, "0")
        )
    return df
