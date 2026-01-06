class TestLsdbIO:
    def test_pyarrow_single_partition(self, columns, io_method, lbench, helpers):
        dataset = helpers.get_pyarrow_dataset(io_method)
        frag = list(dataset.get_fragments())[0]
        def load_partition():
            df = frag.to_table(columns=columns).to_pandas()
        lbench(load_partition)

    def test_lsdb_single_partition(self):
        pass

    def test_pyarrow_multi_partition(self):
        pass

    def test_lsdb_multi_partition(self):
        pass

    def test_pyarrow_whole_catalog(self):
        pass

    def test_lsdb_whole_catalog(self):
        pass

    def test_pyarrow_filtered_query(self):
        pass

    def test_lsdb_filtered_query(self):
        pass

    def test_lsdb_cone_search(self):
        pass
