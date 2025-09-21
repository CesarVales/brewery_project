
from prefect import flow,get_run_logger
from stages.gold.transformations import unify_address_columns, validate_geographic_coordinates, build_valid_url


def filter_null_ids_names_cities(df):
    return df.filter(df.id.isNotNull() & df.name.isNotNull() & df.city.isNotNull())

@flow(name="silver-to-gold")
def silver_to_gold(spark):
    logger = get_run_logger()
    logger.info("Starting silver to gold transformation...")
    
    silver_path = "s3a://brewery-data/silver/"
    df_silver_brewery = spark.read.parquet(silver_path)

    # Apply gold transformations
    df_gold_brewery = unify_address_columns(df_silver_brewery)
    df_gold_brewery = validate_geographic_coordinates(df_gold_brewery)
    df_gold_brewery = build_valid_url(df_gold_brewery)

    df_gold_brewery.show(truncate=False)

    gold_path = "s3a://brewery-data/gold/"

    df_gold_brewery.write.mode("overwrite") \
        .partitionBy("country", "state", "city") \
        .parquet(gold_path)
    
