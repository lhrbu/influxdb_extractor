import pandas
from InfluxDBRowParsers.IInfluxDBRowParser import IInfluxDBRowParser
from InfluxDBRowParsers.BPlusInfluxDBRowParser import BPlusInfluxDBRowParser
from InfluxDBClientWriter import InfluxDBClientWriter
from ExecutionTimer import ExecutionTimer

writer = InfluxDBClientWriter("python_test4")
influxdb_row_parser:IInfluxDBRowParser = BPlusInfluxDBRowParser()


with ExecutionTimer() as execution_timer:
   data = pandas.read_csv("data.csv",sep=",",header="infer")
   for index,row in data.iterrows():
      try:
         point = influxdb_row_parser.Parse("TAVASCAN L02_09_03_2023_10h39m40ff_EXCEL-Daten",row,data.columns)
         resp = writer.WritePoint(point)
         if resp:
            print(resp)
      except Exception as exception:
         print(exception)
         continue


