import pandas as pd
from google.cloud import storage, bigquery
import os
from pathlib import Path
import json
from config import GCP_PROJECT_ID, get_secret


class ETL:

   
    class DataFrames:
        raw_df: pd.DataFrame = pd.DataFrame()
    
    df=DataFrames()
    uploaded_files: list[str] = []
    secret_name="etl_credentials"

    def run(self):
        print("uploading raw data in GCS ...")
        self.upload_data_in_gcs()

        print("staging data in BigQuery...")
        self.stage_in_bigquery()

      
    def upload_data_in_gcs(self):

        secret_dict= get_secret(GCP_PROJECT_ID, self.secret_name)

        gcs=storage.Client.from_service_account_info(secret_dict)#.from_service_account_json("/home/amar_gcplearning7872/secrets/etl_user.json")
               
        bucket=gcs.get_bucket('etl-source-data0')
        
        input_dir=Path("../../input_data/ETL1")
        input_files =os.listdir(input_dir)

        for f in input_files:
            print(f"\t\tuploading file {Path(input_dir,f)}")
            blob= bucket.blob(f)
            blob.upload_from_filename(Path(input_dir,f))
            self.uploaded_files.append(f)
        


    def stage_in_bigquery(self):

        secret_dict= get_secret(GCP_PROJECT_ID, self.secret_name)
        bq_client=bigquery.Client.from_service_account_info(secret_dict)

        c=0
        for blob in self.uploaded_files:
            print(f"\t\timporting blob {blob}")
            c+=1
            load_conf=bigquery.LoadJobConfig(skip_leading_rows=1, 
                                             write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if c==1 else bigquery.WriteDisposition.WRITE_APPEND , 
                                             source_format=bigquery.SourceFormat.CSV, 
                                             field_delimiter=";")
            job = bq_client.load_table_from_uri(f"gs://etl-source-data0/{blob}", 'stage.etl1_raw', job_config=load_conf)
            job.result()






    def read_rawdata(self):
        df = pd.read_csv("input_data/source1_raw20230605.csv", header=0, sep=";")
        self.df.raw_df=df