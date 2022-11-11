import modin.pandas as pd
import ray



class TestModin:
    
    def __init__(self):
        ray.init(runtime_env={'env_vars': {'__MODIN_AUTOIMPORT_PANDAS__': '1'}})

    def read_parquet(self, loc):
        self.data = pd.read_parquet(loc)

    def mean(self, col):
        value = self.data[col].mean()
        print(f"modin mean {value}")
        return value

    def std(self, col):
        value = self.data[col].std()
        print(f"modin std {value}")
        return value

    def unique_len(self, col):
        value = len(self.data[col].unique())
        print(f"modin unique_len {value}")
        return value

    def group_by_mean(self, group_col, mean_col):
        value = self.data.groupby(group_col)[mean_col].mean()
        print(f"modin group_by_mean {value}")
        return value
    
    def exit(self):
        del self.data
        ray.shutdown()
        
