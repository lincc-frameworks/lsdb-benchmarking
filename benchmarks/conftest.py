from upath import UPath
from pytest import fixture


@fixture
def catalog_dir() -> UPath:
    return UPath("/epyc/data3/hats/catalogs")


@fixture
def gaia_collection_path(catalog_dir: UPath) -> UPath:
    return catalog_dir / "gaia_dr3"


@fixture
def gaia_s3_path() -> str:
    return "s3://stpubdata/gaia/gaia_dr3/public/hats"
