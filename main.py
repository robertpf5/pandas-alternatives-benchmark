import importlib
import timeit
from argparse import ArgumentParser


def main():

    parser = ArgumentParser()
    parser.add_argument("-f", "--parquet_file", dest="parquet_file")
    parser.add_argument("-d", "--data_column", dest="data_column")
    parser.add_argument("-g", "--groupby_column", dest="groupby_column")
    parser.add_argument("-t", "--testing_tools", dest="testing_tools")
    args = parser.parse_args()
    testing_tools = [
        f"Test{tool.capitalize()}"
        for tool in args.testing_tools.replace(" ", "").split(",")
    ]

    benchmark = DfBenchmark(
        parquet_file=args.parquet_file,
        data_column=args.data_column,
        groupby_column=args.groupby_column,
        testing_tools=testing_tools,
    )
    benchmark.run()


class DfBenchmark:
    def __init__(self, parquet_file, data_column, groupby_column, testing_tools):
        self.timers = {}
        self.PARQUET_FILE = parquet_file
        self.DATA_COLUMN = data_column
        self.GROUPBY_COLUMN = groupby_column
        self.TESTING_TOOLS = testing_tools

    def run(self):
        """
        runs the calculations for every engine
        """
        for engine_name in self.TESTING_TOOLS:
            engine = self._init_engine(engine_name)
            start_time = timeit.default_timer()

            self.run_callable_with_timer(
                "read_parquet", engine, engine_name, [self.PARQUET_FILE]
            )
            self.run_callable_with_timer(
                "mean", engine, engine_name, [self.DATA_COLUMN]
            )
            self.run_callable_with_timer("std", engine, engine_name, [self.DATA_COLUMN])
            # self.run_callable_with_timer("unique_len", engine, engine_name, [self.DATA_COLUMN])
            self.run_callable_with_timer(
                "group_by_mean",
                engine,
                engine_name,
                [self.GROUPBY_COLUMN, self.DATA_COLUMN],
            )
            self.timers.update(
                {
                    "total_calculations_time "
                    + engine_name: (timeit.default_timer() - start_time)
                }
            )
            self.run_callable_with_timer("exit", engine, engine_name, [], False)
            self.print_timers()
            del engine

    def _init_engine(self, engine_name):
        """
        Initializes the engine to return and saves time it took.
        """
        print(f"testing {engine_name}")
        init_time = timeit.default_timer()
        engine = getattr(
            importlib.import_module("calculationengine.%s" % engine_name.lower()),
            engine_name,
        )()
        self.timers.update(
            {"init_time " + engine_name: (timeit.default_timer() - init_time)}
        )
        return engine

    def run_callable_with_timer(
        self, callable_name, engine, engine_name, arguments, print_timers=True
    ):
        """
        Tracks the time that the function defined in callable_name takes. Calls that function for the given engine with given arguments.
        """
        start_time = timeit.default_timer()
        callable_function = getattr(engine, callable_name)
        callable_function(*arguments)
        if print_timers:
            self.timers.update(
                {
                    f"{callable_name}_time {engine_name}": (
                        timeit.default_timer() - start_time
                    )
                }
            )

    def print_timers(self):
        """
        Prints all the timers stored in self.timers dictionary
        """
        for line in sorted(
            [
                f"{name}: {timer}".replace("Test", "")
                for name, timer in self.timers.items()
            ]
        ):
            print(line)


if __name__ == "__main__":
    main()
