import lsdb
import pytest
from lsdb import ConeSearch


@pytest.fixture
def gaia_path(request):
    return request.getfixturevalue(request.param)


@pytest.mark.parametrize(
    "gaia_path",
    ["gaia_collection_path", "gaia_s3_path"],
    indirect=True,
    ids=["local-fs", "s3"],
)
class TestSimpleFiltering:
    def test_query(self, dask_benchmark, gaia_path):
        def filter_catalog():
            gaia = lsdb.read_hats(gaia_path)
            filtered = gaia.partitions[0].query("parallax > 10 & phot_g_mean_mag < 15")
            filtered.compute()

        dask_benchmark(filter_catalog)

    def test_spatial_filter(self, dask_benchmark, gaia_path):
        def spatial_filter_catalog():
            gaia = lsdb.read_hats(gaia_path)
            filtered = gaia.cone_search(ra=10, dec=10, radius_arcsec=100)
            filtered.compute()

        dask_benchmark(spatial_filter_catalog)

    def test_pyarrow_filter(self, dask_benchmark, gaia_path):
        def pyarrow_filter_catalog():
            gaia = lsdb.read_hats(
                gaia_path,
                search_filter=ConeSearch(ra=10, dec=10, radius_arcsec=100),
                filters=(("phot_g_mean_mag", "<", 20),),
            )
            gaia.compute()

        dask_benchmark(pyarrow_filter_catalog)
