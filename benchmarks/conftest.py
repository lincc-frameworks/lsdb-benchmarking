from upath import UPath
from pytest import fixture
import pyarrow.dataset
import pyarrow as pa


@fixture
def catalog_dir() -> UPath:
    return UPath("/epyc/data3/hats/catalogs")


@fixture
def gaia_collection_path(catalog_dir: UPath) -> UPath:
    return catalog_dir / "gaia_dr3"


@fixture
def gaia_s3_path() -> UPath:
    return UPath("s3://stpubdata/gaia/gaia_dr3/public/hats")

@fixture
def gaia_s3_metadata_path(gaia_s3_path) -> UPath:
    return gaia_s3_path / "gaia" / "dataset" / "_metadata"

@fixture
def gaia_s3_dataset(gaia_s3_metadata_path) -> pyarrow.dataset.Dataset:
    return pyarrow.dataset.parquet_dataset(
        "stpubdata/gaia/gaia_dr3/public/hats/gaia/dataset/_metadata",
        partitioning="hive",
        filesystem=pa.fs.S3FileSystem(),
    )
