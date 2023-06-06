from google.cloud import storage, bigquery

gcs=storage.Client.from_service_account_json("/home/amar_gcplearning7872/secrets/etl_user.json")

bucket=gcs.get_bucket('etl-source-data0')
blob= bucket.blob('etl/source1_raw20230605.csv')
x =blob.download_as_bytes()
print(x)
# blob.upload_from_filename("input_data/source1_raw20230605.csv")
#gcs.create_bucket("amar-bucket2")

# bq=bigquery.Client.from_service_account_json("/home/amar_gcplearning7872/secrets/etl_user.json")
# bq.create_dataset("amar_dataset")