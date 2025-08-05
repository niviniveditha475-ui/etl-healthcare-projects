import pandas as pd 
from pymongo import MongoClient
import os
#ETL / ELT Pipeline for Employee Data
#Extract - csv to pandas DataFrame
patients_df= pd.read_csv(os.path.join(os.path.dirname(__file__), 'hda.csv'))
patients_df.columns = patients_df.columns.str.strip()
print("Columns names:",patients_df.columns.tolist())
print(patients_df.head())

#Load - lake pandas DataFrame to MongoDB
client = MongoClient('mongodb+srv://niviniveditha475:niveditha123@cluster0.xazd8qi.mongodb.net/')
lake_db = client['lake_patients']
lake_collection = lake_db['patients']
lake_collection.delete_many({})
lake_collection.insert_many(patients_df.to_dict('records'))
print('Patients loaded to lake database')

#Transform - MongoDB to pandas DataFrame
patients_df['Length of Stay'] = patients_df['Length of Stay'].fillna(0)
patients_length_of_stay_df = patients_df.groupby('Patient ID')['Length of Stay'].sum().reset_index()
print(patients_length_of_stay_df)

#Load - warehouse pandas DataFrame to MongoDB
warehouse_db = client['warehouse_patients']
warehouse_patients_collection = warehouse_db['patients']
warehouse_patients_collection.delete_many({})
warehouse_patients_collection.insert_many(patients_df.to_dict('records'))
print('Raw Patients data loaded to warehouse database')
warehouse_records_collection = warehouse_db['patients_records']
warehouse_records_collection.delete_many({})
warehouse_records_collection.insert_many(patients_length_of_stay_df.to_dict('records'))
print('Processed Patients records loaded to warehouse database')
