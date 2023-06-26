import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

class InfluxDBClientWriter:
    _url:str
    _token:str
    _bucket:str
    _org:str
    _client: influxdb_client.InfluxDBClient

    def __init__(this,url:str,token:str,bucket:str,org:str):
        this._url = url
        this._token = token
        this._bucket = bucket
        this._org = org

        this._client = influxdb_client.InfluxDBClient(
            url=url,
            token=token,
            org=org
        )

        buckets_api = this._client.buckets_api()
        if not buckets_api.find_bucket_by_name(this._bucket):
            buckets_api.create_bucket(bucket_name=this._bucket)
    

    def WritePoint(this,point:influxdb_client.Point):
        with this._client.write_api(write_options=SYNCHRONOUS) as write_api:
            return write_api.write(bucket=this._bucket, org=this._org, record=point)
        
    
            
