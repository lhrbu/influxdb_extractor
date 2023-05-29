from InfluxDBConnectionBuilders.IInfluxDBConnectionBuilder import IInfluxDBConnectionBuilder


class MockInfluxDBConnectionBuilder(IInfluxDBConnectionBuilder):
    @property
    def Url(self): return "http://10.99.144.200:8086"

    @property
    def Token(self): return "ojRH57p1ASsGOJzb7NEYXqsgxaOsQdDXdsdq0OdkIX3_7TJFPkHatk8ojy-IwaqdakSmLFZ2dq-eB4bMWPRZvQ=="

    @property
    def Org(self): return "Raccoon"
        