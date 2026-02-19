from dataclasses import dataclass, field
from time import perf_counter


@dataclass
class RuntimeMetrics:
    request_count: int = 0
    payload_rows_in: int = 0
    payload_rows_out: int = 0
    module_durations: dict = field(default_factory=dict)
    _starts: dict = field(default_factory=dict)

    def start(self, name: str):
        self._starts[name] = perf_counter()

    def stop(self, name: str):
        start = self._starts.pop(name, None)
        if start is None:
            return
        self.module_durations[name] = round(perf_counter() - start, 3)


METRICS = RuntimeMetrics()
