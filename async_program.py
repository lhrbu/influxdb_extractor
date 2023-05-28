import asyncio,os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
import pandas
from datetime import datetime
from InfluxDBClientAsyncWriter import InfluxDBClientAsyncWriter
from InfluxDBRowParsers.BPlusInfluxDBRowParser import BPlusInfluxDBRowParser
from InfluxDBRowParsers.IInfluxDBRowParser import IInfluxDBRowParser
from ExecutionTimer import ExecutionTimer


MAX_CONCURRENT_COUNT = 16

async def main():
    writer = InfluxDBClientAsyncWriter("python_test5")
    influxdb_row_parser:IInfluxDBRowParser = BPlusInfluxDBRowParser()

    buffered_tasks = []
    with ExecutionTimer() as execution_timer:
        data = pandas.read_csv("data.csv",sep=",",header="infer")
        for index,row in data.iterrows():
            try:
                point = influxdb_row_parser.Parse("TAVASCAN L02_09_03_2023_10h39m40ff_EXCEL-Daten",row,data.columns)
                task = writer.WritePoint(point)
                buffered_tasks.append(task)
                if buffered_tasks.count()>=MAX_CONCURRENT_COUNT :
                    asyncio.wait(buffered_tasks)
                    buffered_tasks.clear()
            except Exception as exception:
                print(exception)
                continue
        if(buffered_tasks.count()>0):
            asyncio.wait(buffered_tasks)
            buffered_tasks.clear()
