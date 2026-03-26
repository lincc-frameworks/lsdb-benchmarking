from lbench.dashboard.metrics import MetricRegistry
from lbench.dashboard.metrics.groups import register_all

registry = MetricRegistry()
register_all(registry)
