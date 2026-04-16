import pytest
import lsdb
import nested_pandas as npd

from hats.pixel_math import HealpixPixel
from lsdb.dask.crossmatch_catalog_data import perform_crossmatch
from hats.io import pixel_catalog_file
from lsdb.core.crossmatch.kdtree_match import KdTreeCrossmatch
from lsdb.dask.merge_catalog_functions import generate_meta_df_for_joined_tables


@pytest.mark.lbench_memory
def test_crossmatch(lbench, catalog_local_dir):
    """Benchmark single pixel crossmatch using ZTF DR22 and Gaia DR3."""

    # Size (memory) of ZTF pixel: 4.4 GiB
    ztf_path = catalog_local_dir / "ztf_dr22" / "ztf_lc"
    ztf_pixel = HealpixPixel(5, 7999)
    ztf_part = npd.read_parquet(pixel_catalog_file(ztf_path, ztf_pixel))
    ztf = lsdb.open_catalog(ztf_path)

    # Size (memory) of Gaia pixel: 321.6 MiB
    gaia_path = catalog_local_dir / "gaia_dr3" / "gaia"
    gaia_pixel = HealpixPixel(4, 1999)
    gaia_part = npd.read_parquet(pixel_catalog_file(gaia_path, gaia_pixel))
    gaia = lsdb.open_catalog(gaia_path)

    # Size (memory) of Gaia margin pixel: 87.9 MiB
    gaia_margin_path = catalog_local_dir / "gaia_dr3" / "gaia_300arcs"
    gaia_margin_part = npd.read_parquet(pixel_catalog_file(gaia_margin_path, gaia_pixel))
    gaia_margin = lsdb.read_hats(gaia_margin_path)

    algorithm = KdTreeCrossmatch()
    suffixes = ("_ztf", "_gaia")
    suffix_method = "all_columns"

    meta_df = generate_meta_df_for_joined_tables(
        (ztf, gaia),
        suffixes,
        suffix_method=suffix_method,
        extra_columns=algorithm.extra_columns,
        log_changes=False,
    )

    def crossmatch():
        return perform_crossmatch(
            left_df=ztf_part,
            right_df=gaia_part,
            right_margin_df=gaia_margin_part,
            aligned_df=None,
            left_pix=ztf_pixel,
            right_pix=gaia_pixel,
            right_margin_pix=gaia_pixel,
            aligned_pixel=ztf_pixel,
            left_catalog_info=ztf.hc_structure.catalog_info,
            right_catalog_info=gaia.hc_structure.catalog_info,
            right_margin_catalog_info=gaia_margin.hc_structure.catalog_info,
            aligned_catalog_info=None,
            algorithm=algorithm,
            how="inner",
            suffixes=suffixes,
            suffix_method=suffix_method,
            meta_df=meta_df,
        )

    lbench(crossmatch)
