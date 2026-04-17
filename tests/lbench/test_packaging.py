import lbench


def test_version():
    """Check to see that we can get the package version"""
    assert lbench.__version__ is not None
