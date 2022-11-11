# Requirements

* must be run in wsl (modin and ray dont work on windows)
* must download large parquet data sample and give it columns name to use for calculations and for group by. By default it works with:
    * area 1 sample (2.4gb) from https://www.synthcity.xyz/download.html
    * 2022 january Yellow Taxi Trip Records (37mb) from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
* potential issues with modin/ray when running in a windows mounted directory that contains spaces
 

# Installation
* If using default test data samples:
    * download https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet and save it in data folder as yellow_tripdata_2022-01.parquet
    * download https://rdr.ucl.ac.uk/s/5cc49e8bcc7497581b30 and save it in data folder as area1.parquet


* in wsl:

```shell
python -m venv venv
cd venv/bin
source ./activate
cd ../../
python -m pip install -r requirements.txt
```

# Execution
  sample execultion for the proposed data samples:

```shell
python main.py --parquet_file data/yellow_tripdata_2022-01.parquet --data_column trip_distance --groupby_column passenger_count --testing_tools pandas,dask,modin,ray
python main.py --parquet_file data/data/area1.parquet --data_column X --groupby_column R --testing_tools pandas,dask,modin
```
