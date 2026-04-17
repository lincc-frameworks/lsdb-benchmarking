"""Jupyter integration for lbench. Load with: %load_ext lbench.notebook"""

from lbench.notebook.magic import LbenchMagics


def load_ipython_extension(ip):
    ip.register_magics(LbenchMagics)
