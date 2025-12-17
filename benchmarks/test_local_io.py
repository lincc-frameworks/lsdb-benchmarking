import hats.io.paths
import lsdb
import nested_pandas as npd
import pandas as pd


def test_local_catalog_partition_read(
    gaia_collection_path, benchmark, single_thread_dask_client
):
    gaia = lsdb.read_hats(gaia_collection_path)
    cat = gaia.partitions[0]

    def load_partition():
        cat.compute()

    benchmark(load_partition)


def test_local_catalog_npd_read(gaia_collection_path, benchmark):
    gaia = lsdb.read_hats(gaia_collection_path)
    partition_0_pixel = gaia.partitions[0].get_healpix_pixels()[0]
    partition_0_path = hats.io.paths.pixel_catalog_file(
        gaia.hc_structure.catalog_base_dir, partition_0_pixel
    )

    def load_partition_npd():
        npd.read_parquet(partition_0_path)

    benchmark(load_partition_npd)


def test_local_catalog_pd_read(gaia_collection_path, benchmark):
    gaia = lsdb.read_hats(gaia_collection_path)
    partition_0_pixel = gaia.partitions[0].get_healpix_pixels()[0]
    partition_0_path = hats.io.paths.pixel_catalog_file(
        gaia.hc_structure.catalog_base_dir, partition_0_pixel
    )

    def load_partition_pd():
        pd.read_parquet(partition_0_path)

    benchmark(load_partition_pd)


def test_local_catalog_partition_read_dask_performance(
    gaia_collection_path, dask_benchmark
):
    gaia = lsdb.read_hats(gaia_collection_path)
    cat = gaia.partitions[0]

    def load_partition():
        cat.compute()

    dask_benchmark(load_partition)


def test_local_catalog_multi_partition_read(
    gaia_collection_path, benchmark, single_thread_dask_client
):
    gaia = lsdb.read_hats(gaia_collection_path)
    n_partitions = 10
    cat = gaia.partitions[:n_partitions]

    def load_partitions():
        cat.compute()

    benchmark(load_partitions)


def test_local_catalog_multi_partition_npd_read(gaia_collection_path, benchmark):
    gaia = lsdb.read_hats(gaia_collection_path)
    n_partitions = 10
    partition_paths = []
    for i in range(n_partitions):
        partition = gaia.partitions[i]
        partition_pixel = partition.get_healpix_pixels()[0]
        partition_path = hats.io.paths.pixel_catalog_file(
            gaia.hc_structure.catalog_base_dir, partition_pixel
        )
        partition_paths.append(partition_path)

    def load_partitions_npd():
        for path in partition_paths:
            npd.read_parquet(path)

    benchmark(load_partitions_npd)
