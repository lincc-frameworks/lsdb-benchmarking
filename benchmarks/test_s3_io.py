import nested_pandas as npd
from upath import UPath


GAIA_COLUMNS = ["ra", "dec", "phot_g_mean_mag", "parallax"]


def test_pyarrow_file_load(gaia_s3_dataset, lbench):
    frag = list(gaia_s3_dataset.get_fragments())[0]
    def load_pa_fragment():
        pa_df = frag.to_table(columns=GAIA_COLUMNS).to_pandas()

    lbench(load_pa_fragment)

def test_npd_file_load(gaia_s3_dataset, lbench):
    path = UPath(f"s3://{list(gaia_s3_dataset.get_fragments())[0].path}", anon=True)
    def load_npd_fragment():
        pa_df = npd.read_parquet(path.path, filesystem=path.fs, columns=GAIA_COLUMNS)

    lbench(load_npd_fragment)

# @pytest.mark.benchmark(min_rounds=1)
# def test_pd_file_load(gaia_s3_dataset, lbench):
#     path = UPath(f"s3://{list(gaia_s3_dataset.get_fragments())[0].path}", anon=True)
#     def load_npd_fragment():
#         pa_df = pd.read_parquet(path.path, filesystem=path.fs, columns=GAIA_COLUMNS)
#
#     lbench(load_npd_fragment)
