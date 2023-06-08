import pandas as pd
from google.cloud import storage, bigquery
import os
from pathlib import Path
import json
from config import GCP_PROJECT_ID, TIMESTAMP, TIMESTAMP_STR
from logging import Logger
from config import get_secret, get_etl_logger

my_logger=get_etl_logger("etl_logger", f"logs/log_etl1_{TIMESTAMP_STR}.log")

class ETL:

   
    class DataFrames:
        raw_df: pd.DataFrame = pd.DataFrame()
    
    df=DataFrames()
    uploaded_files: list[str] = []
    secret_name="etl_credentials"

    def run(self):

        my_logger.info("***************************Run beggining********************************")
        my_logger.info("uploading raw data to GCS...")
        try:
            self.upload_data_in_gcs()
        except Exception as e:
            my_logger.error(e)

        
        my_logger.info("staging data in BigQuery...")
        try:
            self.stage_in_bigquery()
        except Exception as e:
            my_logger.error(e)

        my_logger.info("uploading logfile to GCS...")
        try:
            self.upload_logs_in_GCS()
        except Exception as e:
            my_logger.error(e)

        my_logger.info("loading data to core table...")
        try:
            self.load_to_core()
        except Exception as e:
            my_logger.error(e)
        my_logger.info("******************************Run End************************************\n")


      
    def upload_data_in_gcs(self):

        secret_dict= get_secret(GCP_PROJECT_ID, self.secret_name)

        gcs=storage.Client.from_service_account_info(secret_dict)#.from_service_account_json("/home/amar_gcplearning7872/secrets/etl_user.json")
               
        bucket=gcs.get_bucket('etl-source-data0')
        
        input_dir=Path("/home/amar_gcplearning7872/input_data/ETL1/")
        input_files =os.listdir(input_dir)

        for f in input_files:
            my_logger.info(f"\tuploading file {Path(input_dir,f)}")
            blob= bucket.blob(f)
            blob.upload_from_filename(Path(input_dir,f))
            self.uploaded_files.append(f)

        my_logger.info(f"\t{len(input_files)} files uploaded in GCS bucket 'etl-source-data0'")
        


    def stage_in_bigquery(self):

        secret_dict= get_secret(GCP_PROJECT_ID, self.secret_name)
        bq_client=bigquery.Client.from_service_account_info(secret_dict)

        c=0
        for blob in self.uploaded_files:
            my_logger.info(f"\timporting blob {blob}")
            c+=1
            load_conf=bigquery.LoadJobConfig(skip_leading_rows=1, 
                                             write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if c==1 else bigquery.WriteDisposition.WRITE_APPEND , 
                                             source_format=bigquery.SourceFormat.CSV, 
                                             field_delimiter=";")
            job = bq_client.load_table_from_uri(f"gs://etl-source-data0/{blob}", 'stage.etl1_raw', job_config=load_conf)
            job.result()


    def load_to_core(self):

        with open("/home/amar_gcplearning7872/python_projects/test_project1/etl1/sql/query.sql", 'r') as f:
            sql_str= f.read() 

        secret_dict= get_secret(GCP_PROJECT_ID, self.secret_name)
        bq_client=bigquery.Client.from_service_account_info(secret_dict)

        sql_job=bq_client.query(sql_str)
        sql_job.result()



    def upload_logs_in_GCS(self):

        secret_dict= get_secret(GCP_PROJECT_ID, self.secret_name)
        gcs=storage.Client.from_service_account_info(secret_dict)#.from_service_account_json("/home/amar_gcplearning7872/secrets/etl_user.json")
        bucket=gcs.get_bucket('etl__logs')
        
        blob = bucket.blob(f"ETL1_logs/log_etl1_{TIMESTAMP_STR}new.log")

        blob.upload_from_filename(f"logs/log_etl1_{TIMESTAMP_STR}.log")
 



    def read_rawdata(self):
        df = pd.read_csv("input_data/source1_raw20230605.csv", header=0, sep=";")
        self.df.raw_df=df