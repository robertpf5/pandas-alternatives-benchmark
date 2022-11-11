import ray
class TestRay:

    def read_parquet(self, loc):
        self.data = ray.data.read_parquet(loc)

    def mean(self, col):
        value = self.data.mean(col)
        print(f"ray mean {value}")
        return value

    def std(self, col):
        value = self.data.std(col)
        print(f"ray std {value}")
        return value

    def unique_len(self, col):
        raise(Exception("Not supported"))

    def group_by_mean(self, group_col, mean_col):
        value = self.data.groupby(group_col).mean(mean_col)
        print(f"ray group_by_mean {value}")
        return value

    def exit(self):
        del self.data
        ray.shutdown()