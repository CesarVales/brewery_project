from pyspark.sql import functions as F


def unify_address_columns(df):
    df = df.withColumn(
        "full_address",
        F.concat_ws(", ",
            F.col("address_1"),
            F.col("address_2"),
            F.col("address_3"),
            F.col("street")
        )
    )
    return df

def validate_geographic_coordinates(df):
    df = df.filter(
        (F.col("latitude").between(-90, 90)) &
        (F.col("longitude").between(-180, 180))
    )

    df = df.withColumn("country_code", F.lit("US"))
    df = df.withColumn("state_code", F.when(F.col("state") == "Ohio", "OH").otherwise(F.col("state")))

    return df

def build_valid_url(df):

    df = df.withColumn(
        "website_url",
        F.when(F.col("website_url").isNotNull(), 
               F.when(F.col("website_url").startswith("https://"), F.col("website_url"))
                .otherwise(F.concat(F.lit("https://"), F.regexp_replace("website_url", "^http://", "")))
              )
    )
    return df