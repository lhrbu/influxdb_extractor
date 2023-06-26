import influxdb_client
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import asyncio


class InfluxDBClientAsyncWriter:
    _url:str
    _token:str
    _bucket:str
    _org:str
    _client: InfluxDBClientAsync
    
    def __init__(this,url:str,token:str,bucket:str,org:str):
        this._url = url
        this._token = token
        this._bucket = bucket
        this._org = org

        sync_client = influxdb_client.InfluxDBClient(
            url=url,
            token=token,
            org=org
        )
        this._bucket = bucket

        buckets_api = sync_client.buckets_api()
        if not buckets_api.find_bucket_by_name(this._bucket):
            buckets_api.create_bucket(bucket_name=this._bucket)

        this._client = InfluxDBClientAsync(url=url, token=token, org=org)

    def WritePoint(this,point:influxdb_client.Point):
        write_api = this._client.write_api() 
        return asyncio.create_task(write_api.write(bucket=this._bucket, org=this._org, record=point))   
    
    async def __aenter__(this): return this

    async def __aexit__(this, exc_type, exc, tb):
        await this._client.close()
    
