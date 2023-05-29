from abc import ABC
from abc import abstractproperty

class IInfluxDBConnectionBuilder:
    @abstractproperty
    def Url(self): pass

    @abstractproperty
    def Token(self): pass

    @abstractproperty
    def Org(self): pass
