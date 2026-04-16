import lsdb
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow as pa
import nested_pandas as npd


def test_pyarrow_mean(gaia_collection_path, lbench):
    gaia_root = gaia_collection_path / "gaia"
    parquet_root = f"{gaia_root}/dataset"
    pyarrow_ds = ds.parquet_dataset(f"{parquet_root}/_metadata")

    def dataset_mean(dataset, field: str, *, use_threads: bool = True):
        total_sum = None  # Arrow Scalar
        total_count = 0  # Python int

        for batch in dataset.to_batches(columns=[field], use_threads=use_threads):
            col = batch.column(0)
            b_sum = pc.sum(col)  # Scalar (or null if all-null)
            b_count = pc.count(col, mode="only_valid")  # Int64 Scalar
            if not pc.is_null(b_sum).as_py() and b_count.as_py() > 0:
                total_sum = b_sum if total_sum is None else pc.add(total_sum, b_sum)
                total_count += b_count.as_py()

        if total_sum is None or total_count == 0:
            return None

        # Avoid pc.divide to steer clear of Expression mixing; just unwrap to Python
        return pc.cast(total_sum, pa.float64()).as_py() / float(total_count)

    lbench(dataset_mean, pyarrow_ds, "phot_g_mean_mag")


def test_lsdb_mean(gaia_collection_path, lbench_dask):
    def catalog_mean(df, target_column=""):
        result = npd.NestedFrame(
            {
                "sum": [df[target_column].sum()],
                "count": [len(df)],
            }
        )
        return result

    lsdb_gaia = lsdb.open_catalog(gaia_collection_path, columns=["phot_g_mean_mag"])
    unrealized = lsdb_gaia.map_partitions(
        catalog_mean,
        target_column="phot_g_mean_mag",
    )

    def compute_mean():
        result = unrealized.compute()

    lbench_dask(compute_mean)
