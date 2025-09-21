import json
from io import BytesIO

def load_data_from_minio(minio_client, bucket_name, object_name, local_path):
    if not minio_client.bucket_exists(bucket_name):
        raise ValueError(f"Bucket {bucket_name} does not exist.")
    
    return minio_client.fget_object(bucket_name, object_name, local_path)

def load_data_to_minio(minio_client, bucket_name, path,df):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    return minio_client.put_object(bucket_name, path, df)

def upload_to_minio_from_memory(minio_client, bucket_name, object_name, data):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    payload = json.dumps(data).encode("utf-8")
    minio_client.put_object(
        bucket_name,
        object_name,
        data=BytesIO(payload),
        length=len(payload),
        content_type="application/json"
    )