import asyncio,os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import pandas
from datetime import datetime
from InfluxDBRowParsers.BPlusInfluxDBRowParser import BPlusInfluxDBRowParser

bucket = "python_test5"
org = "Raccoon"
token = "ojRH57p1ASsGOJzb7NEYXqsgxaOsQdDXdsdq0OdkIX3_7TJFPkHatk8ojy-IwaqdakSmLFZ2dq-eB4bMWPRZvQ=="
# Store the URL of your InfluxDB instance
url="http://10.99.144.200:8086"
influxdb_row_parser = BPlusInfluxDBRowParser()

async def main():
   tasks = []
   timeStart = datetime.utcnow()
   data = pandas.read_csv("data.csv",sep=",",header="infer")
   async with InfluxDBClientAsync(url=url, token=token, org=org) as client:
    write_api = client.write_api()
    for index,row in data.iterrows():
        try:
            point = influxdb_row_parser.Parse("TAVASCAN L02_09_03_2023_10h39m40ff_EXCEL-Daten",row,data.columns)
            task = write_api.write(bucket=bucket, org=org, record=point)
            tasks.append(asyncio.create_task(task))
            if len(tasks) >= 8:
               await asyncio.wait(tasks)
               tasks.clear()
        except Exception as exception:
            print(exception)
            continue
   timeEnd = datetime.utcnow()
   print(f"Done,takes {(timeEnd-timeStart).total_seconds()}s to export data")

asyncio.run(main())


