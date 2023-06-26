import asyncio,os, time
import pandas
from InfluxDBClientAsyncWriter import InfluxDBClientAsyncWriter
from InfluxDBRowParsers.BPlusInfluxDBRowParser import BPlusInfluxDBRowParser
from InfluxDBRowParsers.IInfluxDBRowParser import IInfluxDBRowParser
from ExecutionTimer import ExecutionTimer


MAX_CONCURRENT_COUNT = 16

url = "http://10.99.144.200:8086"
token = "ojRH57p1ASsGOJzb7NEYXqsgxaOsQdDXdsdq0OdkIX3_7TJFPkHatk8ojy-IwaqdakSmLFZ2dq-eB4bMWPRZvQ=="
org = "Raccoon"

async def main():
    async with (InfluxDBClientAsyncWriter(url,token,"async_test1",org)) as writer:
        influxdb_row_parser:IInfluxDBRowParser = BPlusInfluxDBRowParser()
        buffered_tasks = []
        with ExecutionTimer() as execution_timer:
            chunks = pandas.read_csv("data.csv",sep=",",header="infer",chunksize=10000)
            for chunk in chunks:
                for index,row in chunk.iterrows():
                    try:
                        point = influxdb_row_parser.Parse("TAVASCAN L02_09_03_2023_10h39m40ff_EXCEL-Daten",row,chunk.columns)
                        task = writer.WritePoint(point)
                        buffered_tasks.append(task)
                        if len(buffered_tasks)>=MAX_CONCURRENT_COUNT :
                            await asyncio.wait(buffered_tasks)
                            buffered_tasks.clear()
                    except Exception as exception:
                        print(exception)
                        continue
                if(len(buffered_tasks)>0):
                    await asyncio.wait(buffered_tasks)
                    buffered_tasks.clear()

asyncio.run(main())