from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def fetch(url_file):
    df = pd.read_csv(url_file)
    return df

@task(log_prints=True)
def clean(df):
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    return df

task()
def createpath(df,color,dfile):
    path = Path(f'data/{color}/{dfile}.parquet')
    print(path)
    return path

@task()
def writegcs(path,file):
    gcs_block = GcsBucket.load("test-gcs-bigger124")
    gcs_block.upload_from_file_object(
    from_file_object = file,
    to_path = path
    )
    return

@flow(name='what',log_prints=True)
def etl_web_to_gcs(color,year,month):
    dataset_file =f'{color}_tripdata_{year}-{month:02}'
    url_file = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch(url_file)
    df = clean(df)
    print(f'There are total of {len(df)} rows in the data.')
    path = createpath(df,color,df)
    writegcs(path,df) 
    return df

@flow(name='parent_fcs')
def parentflow_github(color='yellow',year=2019,month=3) -> None:
    etl_web_to_gcs(color,year,month)
    return None

if __name__ == "__main__":
    color='yellow'
    year='2019'
    month =3   
    parentflow_github(color,year,month)