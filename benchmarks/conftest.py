import os

from upath import UPath
from pytest import fixture
import pyarrow.dataset
import pyarrow as pa
import lsdb


@fixture
def catalog_local_dir() -> UPath:
    root = os.environ.get("CATALOG_LOCAL_DIR", "/epyc/data3/hats/catalogs")
    return UPath(root)


@fixture
def gaia_local_collection_path(catalog_local_dir: UPath) -> UPath:
    return catalog_local_dir / "gaia_dr3"


@fixture
def gaia_local_catalog_path(gaia_local_collection_path) -> UPath:
    return gaia_local_collection_path / "gaia"


@fixture
def gaia_local_metadata_path(gaia_local_catalog_path) -> UPath:
    return gaia_local_catalog_path / "dataset" / "_metadata"


@fixture(scope="session")
def gaia_s3_collection_path() -> UPath:
    return UPath("s3://stpubdata/gaia/gaia_dr3/public/hats")


@fixture(scope="session")
def gaia_s3_catalog_path(gaia_s3_collection_path) -> UPath:
    return gaia_s3_collection_path / "gaia"


@fixture(scope="session")
def gaia_s3_metadata_path(gaia_s3_catalog_path) -> UPath:
    return gaia_s3_catalog_path / "dataset" / "_metadata"


@fixture(scope="session")
def gaia_s3_dataset(gaia_s3_metadata_path) -> pyarrow.dataset.Dataset:
    return pyarrow.dataset.parquet_dataset(
        gaia_s3_metadata_path.path,
        partitioning="hive",
        filesystem=pa.fs.S3FileSystem(),
    )


@fixture
def gaia_local_dataset(gaia_local_metadata_path) -> pyarrow.dataset.Dataset:
    return pyarrow.dataset.parquet_dataset(
        gaia_local_metadata_path,
        partitioning="hive",
        filesystem=pa.fs.LocalFileSystem(),
    )


@fixture
def gaia_local_catalog(gaia_local_catalog_path):
    return lsdb.open_catalog(gaia_local_catalog_path)


@fixture
def gaia_s3_catalog(gaia_s3_catalog_path):
    return lsdb.open_catalog(gaia_s3_catalog_path)


@fixture
def helpers(request):
    class Helpers:
        @staticmethod
        def get_pyarrow_dataset(io_method):
            if io_method == "s3":
                return request.getfixturevalue("gaia_s3_dataset")
            elif io_method == "local":
                return request.getfixturevalue("gaia_local_dataset")
            else:
                raise ValueError(f"Unsupported IO method: {io_method}")

        @staticmethod
        def get_lsdb_catalog(io_method, **kwargs):
            if io_method == "s3":
                path = request.getfixturevalue("gaia_s3_catalog_path")
            elif io_method == "local":
                path = request.getfixturevalue("gaia_local_catalog_path")
            else:
                raise ValueError(f"Unsupported IO method: {io_method}")
            return lsdb.open_catalog(path.as_uri(), **kwargs)

    return Helpers()
