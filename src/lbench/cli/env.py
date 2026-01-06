import os
from pathlib import Path

ROOT_DIR_ENV_VAR = "LBENCH_ROOT"

def get_lbench_root_dir() -> Path:
    """
    Resolve the lbench root directory.

    Priority:
    1. $LBENCH_ROOT_DIR if set
    2. ./lbench_results relative to CWD

    The resolved path is created if missing and written back
    to os.environ for downstream code.
    """
    value = os.environ.get(ROOT_DIR_ENV_VAR)

    if value:
        root = Path(value).expanduser().resolve()
    else:
        root = (Path.cwd() / "lbench_results").resolve()
        os.environ[ROOT_DIR_ENV_VAR] = str(root)

    root.mkdir(parents=True, exist_ok=True)
    return root
