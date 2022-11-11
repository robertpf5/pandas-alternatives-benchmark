import pandas as pd


class TestPandas:
    def read_parquet(self, loc):
        self.data = pd.read_parquet(loc)

    def mean(self, col):
        value = self.data[col].mean()
        print(f"pandas mean {value}")
        return value

    def std(self, col):
        value = self.data[col].std()
        print(f"pandas std {value}")
        return value

    def unique_len(self, col):
        value = len(self.data[col].unique())
        print(f"pandas unique_len {value}")
        return value

    def group_by_mean(self, group_col, mean_col):
        value = self.data.groupby(group_col)[mean_col].mean()
        print(f"pandas group_by_mean {value}")
        return value

    def exit(self):
        del self.data
