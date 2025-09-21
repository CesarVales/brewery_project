
def load_data_from_minio(spark, minio_client, bucket_name, object_name, local_path):
    if not minio_client.bucket_exists(bucket_name):
        raise ValueError(f"Bucket {bucket_name} does not exist.")
    minio_client.fget_object(bucket_name, object_name, local_path)
    return spark.read.json(local_path)

def load_data_to_minio(minio_client, bucket_name, path,df):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    return df.write.mode("overwrite").parquet(path)