import pyarrow
import pyarrow.dataset
import pyarrow.fs
import pyarrow.compute as pc

MAX_MAG = 24.5
MIN_FLUX = 10 ** ((MAX_MAG - 23.9) / -2.5)
OBJECT_ID = "object_id"
PHZ_Z = "phz_phz_median"
COLUMNS = [OBJECT_ID, PHZ_Z]

s3_bucket = "nasa-irsa-euclid-q1"
euclid_prefix = "contributed/q1/merged_objects/hats"
euclid_hats_collection_uri = f"s3://{s3_bucket}/{euclid_prefix}"  # for lsdb
euclid_parquet_metadata_path = f"{s3_bucket}/{euclid_prefix}/euclid_q1_merged_objects-hats/dataset/_metadata"  # for pyarrow

def test_s3_pyarrow_load(benchmark):
    phz_filter = (
            (pc.field("mer_vis_det") == 1)  # No NIR-only objects.
            & (pc.field("mer_flux_detection_total") > MIN_FLUX)  # I < 24.5
            & (pc.divide(pc.field("mer_flux_detection_total"), pc.field("mer_fluxerr_detection_total")) > 5)  # I band S/N > 5
            & ~pc.field("phz_phz_classification").isin([1, 3, 5, 7])  # Exclude objects classified as star.
            & (pc.field("mer_spurious_flag") == 0)  # MER quality
    )
    def load_pa_df():
        dataset = pyarrow.dataset.parquet_dataset(euclid_parquet_metadata_path, partitioning="hive", filesystem=pyarrow.fs.S3FileSystem())
        pa_df = dataset.to_table(columns=COLUMNS, filter=phz_filter).to_pandas()

    benchmark.pedantic(load_pa_df, rounds=1, iterations=1)


def test_dataset_init(lbench):
    def load_dataset():
        dataset = pyarrow.dataset.parquet_dataset(
            euclid_parquet_metadata_path,
            partitioning="hive",
            filesystem=pyarrow.fs.S3FileSystem(),
        )

    lbench(load_dataset)
