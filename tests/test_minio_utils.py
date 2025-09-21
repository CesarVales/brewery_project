from utils.minio_utils import upload_to_minio_from_memory


class FakeMinio:
    def __init__(self):
        self.buckets = set()
        self.objects = {}

    def bucket_exists(self, name):
        return name in self.buckets

    def make_bucket(self, name):
        self.buckets.add(name)

    def put_object(self, bucket_name, object_name, data, length, content_type):
        self.objects[(bucket_name, object_name)] = (data.read(), length, content_type)


def test_upload_to_minio_from_memory_creates_bucket_and_uploads():
    client = FakeMinio()
    upload_to_minio_from_memory(client, "b", "o.json", {"a": 1})
    assert client.bucket_exists("b")
    payload, length, ctype = client.objects[("b", "o.json")]
    assert payload == b"{\"a\": 1}"
    assert length == len(payload)
    assert ctype == "application/json"
