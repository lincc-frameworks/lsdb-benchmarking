import pytest
import pyarrow.dataset
import pyarrow.compute as pc


COLUMN_CONFIGS = [
    ["source_id", "ra", "dec"],
    ["source_id", "ra", "dec", "ra_error", "dec_error", "parallax", "pm", "designation", "phot_g_mean_mag", "phot_bp_mean_mag"],
]


@pytest.mark.parametrize(
    "columns",
    COLUMN_CONFIGS,
    ids=["3col", "10col"]
)
@pytest.mark.parametrize(
    "io_method",
    ["s3"],
    ids=["s3"]
)
class TestLsdbIO:
    def test_pyarrow_single_partition(self, columns, io_method, lbench, helpers):
        dataset = helpers.get_pyarrow_dataset(io_method)
        frag = list(dataset.get_fragments())[0]
        def load_partition():
            df = frag.to_table(columns=columns).to_pandas()
        lbench(load_partition)

    def test_lsdb_single_partition(self, columns, io_method, lbench_dask, helpers):
        catalog = helpers.get_lsdb_catalog(io_method, columns=columns)
        partition = catalog.partitions[0]
        def load_partition():
            df = partition.compute()
        lbench_dask(load_partition)

    def test_pyarrow_multi_partition(self, columns, io_method, lbench, helpers):
        dataset = helpers.get_pyarrow_dataset(io_method)
        frag = list(dataset.get_fragments())[:10]
        paths = [f.path for f in frag]
        ds = pyarrow.dataset.dataset(paths, format="parquet", schema=dataset.schema, filesystem=dataset.filesystem)
        def load_partition():
            df = ds.to_table(columns=columns).to_pandas()
        lbench(load_partition)

    def test_lsdb_multi_partition(self, columns, io_method, lbench_dask, helpers):
        catalog = helpers.get_lsdb_catalog(io_method, columns=columns)
        partition = catalog.partitions[0:10]
        def load_partition():
            df = partition.compute()
        lbench_dask(load_partition)

    def test_pyarrow_filtered_query(self, columns, io_method, lbench, helpers):
        dataset = helpers.get_pyarrow_dataset(io_method)
        frag = list(dataset.get_fragments())[:10]
        paths = [f.path for f in frag]
        ds = pyarrow.dataset.dataset(
            paths,
            format="parquet",
            schema=dataset.schema,
            filesystem=dataset.filesystem,
        ).filter((pc.field("ra") > 45.0) & (pc.field("ra") < 46.0))

        def load_partition():
            df = ds.to_table(columns=columns).to_pandas()

        lbench(load_partition)

    def test_lsdb_filtered_query(self, columns, io_method, lbench_dask, helpers):
        catalog = helpers.get_lsdb_catalog(io_method, columns=columns, filters=[("ra", ">", 45.0), ("ra", "<", 46.0)])
        partition = catalog.partitions[0:10]

        def load_partition():
            df = partition.compute()

        lbench_dask(load_partition)

    def test_lsdb_cone_search(self, columns, io_method, lbench_dask, helpers):
        catalog = helpers.get_lsdb_catalog(
            io_method, columns=columns
        )
        partition = catalog.cone_search(ra=45.5, dec=0.0, radius_arcsec=1800.0)

        def load_partition():
            df = partition.compute()

        lbench_dask(load_partition)

