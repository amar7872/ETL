import pandas as pd
from google.cloud import storage, bigquery, secretmanager
import os
from pathlib import Path
import json

GCP_PROJECT_ID = "project-test1-389013"

class ETL:

   
    class DataFrames:
        raw_df: pd.DataFrame = pd.DataFrame()
    
    df=DataFrames()
    uploaded_files: list[str] = []
    secret_name="etl_credentials"

    def run(self):
        print("reading raw data..")
        self.upload_data_in_gcs()

    def get_secret(self):

        c=secretmanager.SecretManagerServiceClient.from_service_account_json("/home/amar_gcplearning7872/secrets/secretmanager.json")
        secret_name_long=f"projects/{GCP_PROJECT_ID}/secrets/{self.secret_name}/versions/latest"
        response = c.access_secret_version(name=secret_name_long)
        secret_dict = response.payload.data.decode("UTF-8")        
        return secret_dict
        
    def upload_data_in_gcs(self):

        secret_str=self.get_secret()

        gcs=storage.Client.from_service_account_info(json.loads(secret_str))#.from_service_account_json("/home/amar_gcplearning7872/secrets/etl_user.json")
        bucket=gcs.get_bucket('etl-source-data0')
        
        input_dir=Path("../../input_data/ETL1")
        input_files =os.listdir(input_dir)

        for f in input_files:
            print(f"uploading file {Path(input_dir,f)}")
            blob= bucket.blob(f)
            blob.upload_from_filename(Path(input_dir,f))
            self.uploaded_files.append(f)
        

        



    def read_rawdata(self):
        df = pd.read_csv("input_data/source1_raw20230605.csv", header=0, sep=";")
        self.df.raw_df=df