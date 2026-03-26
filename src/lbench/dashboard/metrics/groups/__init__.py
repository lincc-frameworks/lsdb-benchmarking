from lbench.dashboard.metrics import MetricRegistry
from lbench.dashboard.metrics.groups.computed_group import computed_group
from lbench.dashboard.metrics.groups.dask_group import dask_group
from lbench.dashboard.metrics.groups.execution_group import execution_group
from lbench.dashboard.metrics.groups.stats_group import stats_group

_GROUP_MODULES = [stats_group, execution_group, computed_group, dask_group]

def register_all(registry: MetricRegistry):
    for group in _GROUP_MODULES:
        registry.register_group(group)
