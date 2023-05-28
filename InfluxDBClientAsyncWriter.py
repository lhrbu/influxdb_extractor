import influxdb_client
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import asyncio

class InfluxDBClientAsyncWriter:
    _client: InfluxDBClientAsync
    _bucket:str
    _org = "Raccoon"
    _token = "ojRH57p1ASsGOJzb7NEYXqsgxaOsQdDXdsdq0OdkIX3_7TJFPkHatk8ojy-IwaqdakSmLFZ2dq-eB4bMWPRZvQ=="
    _url="http://cnshntceqp1v:8086"
    def __init__(self,bucket):
        sync_client = influxdb_client.InfluxDBClient(
            url=self._url,
            token=self._token,
            org=self._org
        )
        buckets_api = self._client.buckets_api()
        if not buckets_api.find_bucket_by_name(self._bucket):
            buckets_api.create_bucket(bucket_name=self._bucket)

        self._bucket = bucket
        self._client = InfluxDBClientAsync(url=self._url, token=self._token, org=self._org)

    def WritePoint(self,point:influxdb_client.Point):
        write_api = self._client.write_api() 
        return asyncio.create_task(write_api.write(bucket=self._bucket, org=self._org, record=point))   
    
