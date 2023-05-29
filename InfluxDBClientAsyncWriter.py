import influxdb_client
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import asyncio

from InfluxDBConnectionBuilders.IInfluxDBConnectionBuilder import IInfluxDBConnectionBuilder

class InfluxDBClientAsyncWriter:
    _client: InfluxDBClientAsync
    _bucket:str
    _connectionBuilder:IInfluxDBConnectionBuilder
    
    def __init__(self,connetionBuilder:IInfluxDBConnectionBuilder,bucket:str):
        sync_client = influxdb_client.InfluxDBClient(
            url=connetionBuilder.Url,
            token=connetionBuilder.Token,
            org=connetionBuilder.Org
        )
        self._bucket = bucket

        buckets_api = sync_client.buckets_api()
        if not buckets_api.find_bucket_by_name(self._bucket):
            buckets_api.create_bucket(bucket_name=self._bucket)

        self._client = InfluxDBClientAsync(url=connetionBuilder.Url, token=connetionBuilder.Token, org=connetionBuilder.Org)
        self._connectionBuilder = connetionBuilder

    def WritePoint(self,point:influxdb_client.Point):
        write_api = self._client.write_api() 
        return asyncio.create_task(write_api.write(bucket=self._bucket, org=self._connectionBuilder.Org, record=point))   
    
    async def __aenter__(self): return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._client.close()
    
