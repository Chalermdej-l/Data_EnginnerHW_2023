from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from pathlib import Path
import pandas as  pd
@task()
def extract_from_gcs(color,year,month):
    gcs_path =f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load("test-gcs-bigger124")
    gcs_block.get_directory(from_path=gcs_path,local_path=f'./data/')
    return Path(f'./{gcs_path}')

@task()
def loaddata(path):
    df = pd.read_parquet(path)


    return df

@task()
def write_bq(df: pd.DataFrame):

    gcp_credentials_block = GcpCredentials.load("test-gcp-cred")

    df.to_gbq (
    destination_table='test_bigger124test.yellow_2019',
    project_id='quantum-plasma-376313',
    credentials=gcp_credentials_block.get_credentials_from_service_account(),
    chunksize=500_000,
    if_exists='append'
    )


@flow(name='bq',log_prints=True)
def etl_gcs_to_bq(color,year,months):
    total_row = 0
    for month in months:
        path = extract_from_gcs(color,year,month)
        df = loaddata(path)
        rows = len(df)
        print(f'Loading total of {rows} rows.')
        total_row +=rows 
        write_bq(df)

    print(f'Total row proceed {total_row}.')    
    return None

@flow(name='parent')
def parentflow(color,year,month):
    color='yellow'
    year='2019'
    month =[2,3]    
    etl_gcs_to_bq(color,year,month)


if __name__ == "__main__":
    color='yellow'
    year='2019'
    month =[2,3]    
    path = parentflow(color,year,month)
    