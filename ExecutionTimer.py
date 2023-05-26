from datetime import datetime

class ExecutionTimer:
    _timeStart:datetime
    _timeEnd:datetime
    def __enter__(self):
        self._timeStart = datetime.utcnow()
    def __exit__(self,exc_type, exc_value, traceback):
        self._timeEnd = datetime.utcnow()
        print(f"It takes {(self._timeEnd - self._timeStart).total_seconds()}s to execute")
