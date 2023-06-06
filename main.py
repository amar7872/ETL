from etl1.EtlClass import ETL

etl=ETL()
etl.run()
print('\n\n')
print(etl.df.raw_df)