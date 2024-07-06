## Week 2 Homework

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green`

### Assignment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`
- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course.
  - `BONUS`: load the final three months using a for loop and `pd.concat`
- Add a transformer block and perform the following:
  - Remove rows where the passenger count is equal to 0 _or_ the trip distance is equal to zero.
  - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
  - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
  - Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0
- Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
- Write your data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date`. Use the `pyarrow` library!
- Schedule your pipeline to run daily at 5AM UTC.

#### DATA LOADER 
``` python
@data_loader
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd
import os 
@data_loader
def load_data(*args, **kwargs):
    data=pd.DataFrame()
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    for month in [10,11,12]:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz'
        csv_name= f'green_taxi_{month}.csv.gz'
        os.system(f"wget {url} -O {csv_name}")
        
        df=pd.read_csv(csv_name,sep=',',compression="gzip",parse_dates=parse_dates)
        data= pd.concat([data,df],ignore_index=True)

    return data


@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'

```
#### TRANSFORMER 
```python

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    # Specify your transformation logic here
    data['lpep_pickup_date']= data['lpep_pickup_datetime'].dt.date #date column
    data.columns = (data.columns
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True) #camel to snake
                    .str.lower() # all lower case
                )
    # filter out rows with passenger_count = 0 or trip_distance = 0
    data=data[data['passenger_count']>0] 
    data=data[data['trip_distance']>0]

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert 'vendor_id' in output.columns, 'vendor_id does not exist'
    assert output['trip_distance'][output['trip_distance']==0].sum()==0, "there are zeros"
    assert output['passenger_count'][output['passenger_count']==0].sum()==0, "there are zeros"
    
```

#### data_exporter
```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

bucket_name="mage-zoomcamp-homework-week2"
project_id= "iconic-valve-428216-s4"

table_name = "green_taxi"

root_path = f"{bucket_name}/{table_name}"
@data_exporter
def export_data(data, *args, **kwargs):
    
    # Specify your data exporting logic here
    table=pa.Table.from_pandas(data)
    gcs=pa.fs.GcsFileSystem()
    
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )




```


### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns ***THIS ONE***
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is equal to 0 _or_ the trip distance is equal to zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows ***THIS ONE***
* 266,856 rows


## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* data = data['lpep_pickup_datetime'].date
* data('lpep_pickup_date') = data['lpep_pickup_datetime'].date
* data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date  ***THIS ONE***
* data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()

## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2   ***THIS ONE***
* 1, 2, 3, 4
* 1

```python 
value_counts= data['vendorid'].value_counts()
value_counts
```

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4  ***THIS ONE***

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?  ***I GET 95 for the days and then 1 for green taxi***

* 96 ***THIS ONE***
* 56
* 67
* 108

