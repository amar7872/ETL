import pandas as pd
from google.cloud import storage

class ETL:

   
    class DataFrames:
        raw_df: pd.DataFrame = pd.DataFrame()
    
    df=DataFrames()

    def run(self):
        print("reading raw data..")
        self.read_rawdata()

    def read_rawdata(self):
        df = pd.read_csv("input_data/source1_raw20230605.csv", header=0, sep=";")
        self.df.raw_df=df