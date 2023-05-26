from influxdb_client import Point
from abc import ABC
from abc import abstractmethod
import pandas

class IInfluxDBRowParser:
    @abstractmethod
    def Parse(self,measurement_name:str,row:pandas.Series,columes_name:pandas.Index)->Point:
        pass