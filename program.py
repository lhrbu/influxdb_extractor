import pandas
from InfluxDBConnectionBuilders.MockInfluxDBConnectionBuilder import MockInfluxDBConnectionBuilder
from InfluxDBRowParsers.IInfluxDBRowParser import IInfluxDBRowParser
from InfluxDBRowParsers.BPlusInfluxDBRowParser import BPlusInfluxDBRowParser
from InfluxDBClientWriter import InfluxDBClientWriter
from ExecutionTimer import ExecutionTimer

connection_builder = MockInfluxDBConnectionBuilder()
writer = InfluxDBClientWriter(connection_builder,"python_test9")
influxdb_row_parser:IInfluxDBRowParser = BPlusInfluxDBRowParser()


with ExecutionTimer() as execution_timer:
   chunks = pandas.read_csv("data.csv",sep=",",header="infer",chunksize=10000)
   for chunk in chunks:
      for index,row in chunk.iterrows():
         try:
            point = influxdb_row_parser.Parse("TAVASCAN L02_09_03_2023_10h39m40ff_EXCEL-Daten",row,chunk.columns)
            resp = writer.WritePoint(point)
            if resp:
               print(resp)
         except Exception as exception:
            print(exception)
            continue


