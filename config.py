import pandas as pd
from google.cloud import storage, bigquery, secretmanager
import os, sys
from pathlib import Path
import json
import logging
import datetime

TIMESTAMP = datetime.datetime.now()

TIMESTAMP_STR = datetime.datetime.now().strftime("%Y%m%d")

GCP_PROJECT_ID = "project-test1-389013"

def get_secret(project_id, secret_name, version="latest")-> dict:

    c=secretmanager.SecretManagerServiceClient.from_service_account_json("/home/amar_gcplearning7872/secrets/secretmanager.json")
    secret_name_long=f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    response:str = c.access_secret_version(name=secret_name_long)
    secret_dict:dict = json.loads(response.payload.data.decode("UTF-8"))   
    return secret_dict


def get_storage_client(self, credentials_dict:dict):

    client=storage.Client.from_service_account_info(credentials_dict)

    return client



def get_bigquery_client():
    pass



def get_etl_logger(name, path):

    Path("logs").mkdir(exist_ok=True)

    logger=logging.getLogger(name)
    f=logging.FileHandler(path) 
    f.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(f)
    logger.setLevel('INFO')
    return logger


