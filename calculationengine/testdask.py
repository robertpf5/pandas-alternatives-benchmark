import dask.dataframe as dd

class TestDask:

    def read_parquet(self, loc):
        self.data = dd.read_parquet(loc)

    def mean(self, col):
        value = self.data[col].mean().compute()
        print(f"dask mean {value}")
        return value

    def std(self, col):
        value = self.data[col].std().compute()
        print(f"dask std {value}")
        return value

    def unique_len(self, col):
        value = len(self.data[col].unique())
        print(f"dask unique_len {value}")
        return value

    def group_by_mean(self, group_col, mean_col):
        value = self.data.groupby(group_col)[mean_col].mean().compute()
        print(f"dask group_by_mean {value}")
        return value

    def exit(self):
        del self.data