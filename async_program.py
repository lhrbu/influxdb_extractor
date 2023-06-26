import asyncio,os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import pandas
from datetime import datetime
from InfluxDBClientAsyncWriter import InfluxDBClientAsyncWriter
from InfluxDBConnectionBuilders.MockInfluxDBConnectionBuilder import MockInfluxDBConnectionBuilder
from InfluxDBRowParsers.BPlusInfluxDBRowParser import BPlusInfluxDBRowParser
from InfluxDBRowParsers.IInfluxDBRowParser import IInfluxDBRowParser
from ExecutionTimer import ExecutionTimer


MAX_CONCURRENT_COUNT = 16

connection_builder = MockInfluxDBConnectionBuilder()

async def main():
    async with (InfluxDBClientAsyncWriter(connection_builder,"python_test8")) as writer:
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