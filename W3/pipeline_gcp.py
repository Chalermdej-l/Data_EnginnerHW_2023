from prefect import task,flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import bigquery_create_table
from google.cloud import bigquery
import pandas as pd  
from config import credential
task()
def writegcs(file_list):
    block = GcsBucket.load("gcs-dataengineer")
    to_name = file_list[file_list.rfind('\\')+1:]
    block.upload_from_path(from_path=file_list,to_path=to_name)

    return None

task() 
def loadfile(year,month):
    url =f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz'
    print(url)
    file = pd.read_csv(url)
    return file

task() 
def createtable(file,project_id):

    client = bigquery.Client(project=project_id)

    table = f'{credential.project_id}.dataset.bigquery_pq'
    job = client.load_table_from_dataframe(file, table)


@flow(name='bq_external')
def create_externaltable(project_id):
    gcp_credentials = GcpCredentials(project=project_id)    

    external = bigquery.ExternalConfig('PARQUET')
    external.autodetect = True
    external.source_uris = [ 'gs://dataengineer_2023/fhv_tripdata_2019.parquet']
    result = bigquery_create_table(
        dataset="dataset",
        table="external_pq",
        location='asia-southeast1',
        gcp_credentials=gcp_credentials,
        external_config =external    
    )
    return result


flow()
def mainflow():
    # Define Variable
    year = 2019
    month = [i for i in range(1,13)]
    project_id = credential.project_id
    file_all = pd.DataFrame()

    # Load in the file from Github
    for i in month:
         file =loadfile(year,i)
         file_all = file_all.append(file)
    # Create a combine 2019 data in parquet to local
    
    path = f'data\\fhv_tripdata_{year}.parquet'
    file_all.to_parquet(path,index=False)

    # Upload file to GCS
    _ = writegcs(path)

    #Create table in BQ   
    _ = create_externaltable(project_id) 
    _ = createtable(file_all,project_id)    
    

    return None


if __name__ =='__main__':
    # Call the pipeline
    mainflow()