import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

from InfluxDBConnectionBuilders.IInfluxDBConnectionBuilder import IInfluxDBConnectionBuilder

class InfluxDBClientWriter:
    _client: influxdb_client.InfluxDBClient
    _bucket:str
    _connectionBuilder: IInfluxDBConnectionBuilder

    def __init__(self,connetionBuilder:IInfluxDBConnectionBuilder,bucket:str):
        self._bucket = bucket
        self._client = influxdb_client.InfluxDBClient(
            url=connetionBuilder.Url,
            token=connetionBuilder.Token,
            org=connetionBuilder.Org
        )

        buckets_api = self._client.buckets_api()
        if not buckets_api.find_bucket_by_name(self._bucket):
            buckets_api.create_bucket(bucket_name=self._bucket)

        self._connectionBuilder = connetionBuilder
    

    def WritePoint(self,point:influxdb_client.Point):
        with self._client.write_api(write_options=SYNCHRONOUS) as write_api:
            return write_api.write(bucket=self._bucket, org=self._connectionBuilder.Org, record=point)
        
    
            
