import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxDBClientWriter:
    _client: influxdb_client.InfluxDBClient
    _bucket:str
    _org = "Raccoon"
    _token = "ojRH57p1ASsGOJzb7NEYXqsgxaOsQdDXdsdq0OdkIX3_7TJFPkHatk8ojy-IwaqdakSmLFZ2dq-eB4bMWPRZvQ=="
    _url="http://cnshntceqp1v:8086"
    
    def __init__(self,bucket):
        self._bucket = bucket
        self._client = influxdb_client.InfluxDBClient(
            url=self._url,
            token=self._token,
            org=self._org
        )

        buckets_api = self._client.buckets_api()
        if not buckets_api.find_bucket_by_name(self._bucket):
            buckets_api.create_bucket(bucket_name=self._bucket)
    

    def WritePoint(self,point:influxdb_client.Point):
        with self._client.write_api(write_options=SYNCHRONOUS) as write_api:
            return write_api.write(bucket=self._bucket, org=self._org, record=point)
            
