from InfluxDBExtractors.IInfluxDBRowParser import IInfluxDBRowParser
import pandas
import influxdb_client
from datetime import datetime

class BPlusInfluxDBRowParser(IInfluxDBRowParser):
    _utcFormat = "%d.%m.%YT%H:%M:%S"
    def Parse(self,measurement_name:str,row:pandas.Series,columes_name:pandas.Index):
        point = influxdb_client.Point(measurement_name)
        
        dateOnly = row[0]
        timeOnly = row[1]
        localRecordDateTime = datetime.strptime(f"{dateOnly}T{timeOnly}",self._utcFormat)
        recordDateTime = datetime.utcfromtimestamp(localRecordDateTime.timestamp())
        point.time(recordDateTime)

        point.tag("step name",row[2])

        for i in range(3,row.size):
            header:str = columes_name[i]
            if not header.startswith("Unnamed: "):
                item = row[i]
                point.field(header,item)
        
        return point