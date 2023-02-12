from prefect_gcp.cloud_storage import GcsBucket
from prefect import task,flow
import pandas as pd  
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
flow()
def mainflow():
    year = 2019
    month = [i for i in range(1,13)]
    file_all = pd.DataFrame()
    for i in month:
        file =loadfile(year,i)
        file_all = file_all.append(file)
    path = f'data\\fhv_tripdata_{year}.csv.gz'
    file_all.to_csv(path,index=False)

    writegcs(path)
    return None
if __name__ =='__main__':
    mainflow()