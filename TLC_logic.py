# Arrange the directory structure of the data files as shown in the documentatin
# instll required modules using pip 
# required modules pandas, pyarrow
# run the file from the data directory loaction 
# python3 RLC_logic.py

import os
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import logging

# Industry file location paths
yellow_directory = 'TLC_Trip_Record_Data/Yellow_Trip'
green_directory = 'TLC_Trip_Record_Data/Green_Trip'
fhv_directory = 'TLC_Trip_Record_Data/FHV_Trips'
hvh_directory = 'TLC_Trip_Record_Data/High_volume_For_Hire_Trips'

file_extension = '.parquet'

# Define expected schema for each industry
yellow_expected_schema = {
                'VendorID': 'int', 'tpep_pickup_datetime': 'datetime64[ns]',
                'tpep_dropoff_datetime': 'datetime64[ns]', 'passenger_count': 'int',
                'trip_distance': 'float', 'PULocationID': 'int', 'DOLocationID': 'int',
                'RatecodeID': 'int', 'store_and_fwd_flag': 'string', 'payment_type': 'int',
                'fare_amount': 'float', 'extra': 'float', 'mta_tax': 'float','improvement_surcharge':'float',
                'tip_amount': 'float', 'tolls_amount': 'float', 'total_amount': 'float',
                'congestion_surcharge': 'float', 'airport_fee': 'float'
}

green_expected_schema = {
                'VendorID': 'int', 'lpep_pickup_datetime': 'datetime64[ns]',
                'lpep_dropoff_datetime': 'datetime64[ns]', 'passenger_count': 'int',
                'trip_distance': 'float', 'PULocationID': 'int', 'DOLocationID': 'int',
                'RatecodeID': 'int', 'store_and_fwd_flag': 'string', 'payment_type': 'int',
                'fare_amount': 'float', 'extra': 'float', 'mta_tax': 'float', 'tip_amount': 'float',
                'tolls_amount': 'float', 'total_amount': 'float', 'trip_type':'int'
                }

fhv_expected_schema = {
              'dispatching_base_num': 'string', 'pickup_datetime': 'datetime64[ns]',
              'dropOff_datetime': 'datetime64[ns]', 'PUlocationID': 'int', 'DOlocationID': 'int',
              'SR_Flag': 'int', 'Affiliated_base_number': 'string'
              }

hvh_expected_schema = {
              'hvfhs_license_num': 'string', 'dispatching_base_num': 'string',
              'pickup_datetime': 'datetime64[ns]', 'dropoff_datetime': 'datetime64[ns]',
              'PULocationID': 'int', 'DOLocationID': 'int', 'originating_base_num': 'string',
              'request_datetime': 'datetime64[ns]', 'on_scene_datetime': 'datetime64[ns]',
              'trip_miles': 'float', 'trip_time': 'datetime64[ns]', 'base_passenger_fare': 'float',
              'tolls': 'float', 'bcf': 'float', 'sales_tax': 'float', 'congestion_surcharge': 'float',
              'airport_fee': 'float', 'tips': 'float', 'driver_pay': 'float',
              'shared_request_flag': 'string', 'shared_match_flag': 'string'
              }

# Configure logging
logging.basicConfig(filename='data_validation.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Helper function to validate and coerce DataFrame columns
def validate_and_coerce(df, expected_schema):
    df = df.copy()

    # Convert column names to lowercase for case-insensitive comparison
    df.columns = df.columns.str.lower()
    expected_schema = {key.lower(): value for key, value in expected_schema.items()}

    # Check if all expected columns are present in the DataFrame
    missing_columns = set(expected_schema.keys()) - set(df.columns)
    if missing_columns:
        logging.error(f"Missing columns: {', '.join(missing_columns)}")

    # Reindexing DataFrame columns based on the expected schema
    if list(df.columns) != expected_schema.keys(): # checking the columns order
        df = df.reindex(columns=expected_schema.keys())

    # Iterate through each column and validate its data type
    for column, expected_type in expected_schema.items():
       
        if df.dtypes.to_dict().get(column) != expected_type:  # validating  colum dtype 
        
            try:
                # filling the NA records with 0, because we Cannot convert non-finite values (NA or inf) to int datatype
                df = df.fillna(0)
                df[column] = df[column].astype(expected_type) # Converting df[column] data type to expected type
            except ValueError as ve:
                logging.error(f"Error occurred while coercing column '{column}': {str(ve)}")
            except Exception as e:
                logging.error(f"An error occurred while processing column '{column}': {str(e)}")

    return df

# Read and validate data for Yellow industry
yellow_files = [file for file in os.listdir(yellow_directory) if file.endswith(file_extension)]

for file in yellow_files:
    file_path = os.path.join(yellow_directory, file)
    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()
        validated_df = validate_and_coerce(df, yellow_expected_schema)
                # .....
        # ..... we can add addition validations and transformations here
        print(validated_df.head())# printing the top 5 records for reference purpose
        # instead of printing data here we can configure the cloud(AWS S3) to upload 
    except Exception as e:
        logging.error(f"An error occurred while processing file '{file}': {str(e)}")

# Read and validate data for Green industry
green_files = [file for file in os.listdir(green_directory) if file.endswith(file_extension)]

for file in green_files:
    file_path = os.path.join(green_directory, file)
    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()
        validated_df = validate_and_coerce(df, green_expected_schema)
                # .....
        # ..... we can add addition validations and transformations here
        print(validated_df.head())# printing the top 5 records for reference purpose
        # instead of printing data here we can configure the cloud(AWS S3) to upload 
    except Exception as e:
        logging.error(f"An error occurred while processing file '{file}': {str(e)}")

# Read and validate data for For-Hire Vehicle industry
fhv_files = [file for file in os.listdir(fhv_directory) if file.endswith(file_extension)]

for file in fhv_files:
    file_path = os.path.join(fhv_directory, file)
    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()
        validated_df = validate_and_coerce(df, fhv_expected_schema)
                # .....
        # ..... we can add addition validations and transformations here
        print(validated_df.head())# printing the top 5 records for reference purpose
        # instead of printing data here we can configure the cloud(AWS S3) to upload 

    except Exception as e:
        logging.error(f"An error occurred while processing file '{file}': {str(e)}")

# Read and validate data for High Volume For-Hire Vehicle industry

hvh_files = [file for file in os.listdir(hvh_directory) if file.endswith(file_extension)]

for file in hvh_files:
    file_path = os.path.join(hvh_directory, file)
    try:
        table = pq.read_table(file_path)
        df = table.to_pandas()
        validated_df = validate_and_coerce(df, hvh_expected_schema)
        # .....
        # ..... we can add addition validations and transformations here
        print(validated_df.head()) # printing the top 5 records for reference purpose
        # instead of printing data here we can configure the cloud(AWS S3) to upload 

    except Exception as e:
        logging.error(f"An error occurred while processing file '{file}': {str(e)}")




# Check the data_validation.log file for any possible errors

