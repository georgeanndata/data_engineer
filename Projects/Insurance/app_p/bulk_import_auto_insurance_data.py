########################################
# Auto Insurance Claims Project
########################################

#####################################################
# Import required packages
#####################################################

from importlib.metadata import metadata
import os
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Table, Column, Integer, String, Date, Float
from sqlalchemy import func


#####################################################
# Import data from csv to dataframe
#####################################################

import os
import pandas as pd

## Function to check if file does NOT exist>  If it does, print exits and data is loaded (load_data function)
def check_file_exists(filepath):
        if not os.path.exists(filepath):
            print("OOPS!", filepath, "file does not exist!")
        else:
            print(filepath, "exists.")
            print("Beginning to load data....")
            

            ## Function to load data
            def load_data(ms_file):
                data_from_csv = pd.read_csv(ms_file)
                print("Data has completed loading")
                number_of_cols = len(data_from_csv.columns)
                number_of_rows = len(data_from_csv.index)
                print("Columns:", number_of_cols, "and Rows:", number_of_rows, "were loaded." )
                print("Data was loaded...complete.")
                return data_from_csv

            csv_data = load_data(filepath)
            return csv_data
            

## data path variable in case it ever changes
path = "data/"
file_name = "insurance_claims.csv" #use small_insurance_claims.csv for testing

date_file_name  = path+file_name 

csv_file_data = check_file_exists(date_file_name)

## Inspect the data
print(csv_file_data.head())

## check column names

def check_col_names(v, b, csv_col_name):
    col_name_1 =  [ext_char for ext_char in csv_col_name if any(xs in ext_char for xs in v)]
    print("These columns contain extended characters", col_name_1)
    col_name_2 = [ext_char for ext_char in csv_col_name if any(ext_char.startswith(bs) for bs in b)]  
    print("These columns start with extended characters", col_name_2)

    
 
v = ["*", "!", "-"]
b = ["_"]
b.extend(v)
check_col_names(v, b, csv_file_data.columns.values)

## update column name(s)

csv_file_data.columns = csv_file_data.columns.str.replace('_c39', 'c39')
csv_file_data.columns =  csv_file_data.columns.str.replace('capital-gains', 'capital_gains')
csv_file_data.columns =  csv_file_data.columns.str.replace('capital-loss', 'capital_loss')

for col in csv_file_data.columns.values:
    print(col)


csv_file_data.head()
csv_file_data.info()



#####################################################
# Import data in PostGres
#####################################################


from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def db_connection(uri): 
    try:
        db_connection.engine = create_engine(uri, echo=True)
        db_connection.conn = db_connection.engine.connect()
        print("Connection was successful!")
    except SQLAlchemyError as err:
        print("error", err.__cause__)  # this will give what kind of error


uri = 'postgresql+psycopg2://postgres:data@localhost:5432/postgresdb'
db_connection(uri)


### drop table >> for testing
##db_connection.engine.execute("DROP TABLE IF EXISTS {}".format(table_name))
 

def create_table(engine, table_name, csv_file_data):
    ## isert dataframe to postgres table
    csv_file_data.to_sql(
    table_name,
    engine,
    if_exists='replace',
    chunksize = 500,
    dtype={
        'months_as_customer': Integer,
        'age':  Integer,
        'policy_number': Integer,
        'policy_bind_date': Date, 
        'policy_state': String(2),
        'policy_csl': String(20), 
        'policy_deductable': Integer,
        'policy_annual_premium': Float, 
        'umbrella_limit': Integer, 
        'insured_zip': Integer, 
        'insured_sex': String(6), 
        'insured_education_level': String(20), 
        'insured_occupation': String(50), 
        'insured_hobbies': String(50),        
        'insured_relationship': String(20),  
        'capital_gains': Integer, 
        'capital_loss': Integer, 
        'incident_date': Date, 
        'incident_type': String(50), 
        'collision_type': String(50), 
        'incident_severity': String(50), 
        'authorities_contacted': String(30), 
        'incident_state': String(2), 
        'incident_city': String(30), 
        'incident_location': String(100), 
        'incident_hour_of_the_day': Integer, 
        'number_of_vehicles_involved': Integer, 
        'property_damage': String(3),
        'bodily_injuries': Integer, 
        'witnesses': Integer, 
        'police_report_available': String(3), 
        'total_claim_amount': Integer, 
        'injury_claim': Integer, 
        'property_claim': Integer, 
        'vehicle_claim': Integer, 
        'auto_make': String(30), 
        'auto_model': String(30), 
        'auto_year': Integer, 
        'fraud_reported': String(1), 
        'c39': String(30)
    }
)


from sqlalchemy import Table, Column, Integer, String, Date, Float, MetaData
## desginate table

table_name = 'auto_claims'

def import_data(engine,table_name, csv_file_data):
    # drop table if it already exists
    db_connection.engine.execute("DROP TABLE IF EXISTS {}".format(table_name))
    
    ## call create_table function to create table
    create_table(db_connection.engine, table_name, csv_file_data)

engine = db_connection.engine
import_data(engine,table_name, csv_file_data)






