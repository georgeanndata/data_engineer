
# How I used Kafka, Spark, Postgres, and Metabase in a Docker container to create a data engineering platform for auto insurance claims


# Introduction & Goals
Last year I completed a 
<a href="https://georgeanndata.github.io/2021/08/01/insurance_fraud_detection.html">data science project</a> where I used Python, Pandas and Tableau to better predict auto insurance fraud claims. After completing the project, I wondered if it would be more helpful for a data scientist to be able access the data without having to import the CSV file using Python and how much better it would be for them to be working on the most updated data available, instead of a file that was stored locally, which in all probability was not current. To improve upon my project, I decided to take a data engineering program so I could create a data platform that would conveniently supply the most updated auto insurance fraud data to the data scientist, making their jobs not only easier but also improving their analysis and reporting.   

__Links to programs__:
* <a href="https://data-science-infinity.teachable.com/">Data Science Infinity</a>
* <a href="https://learndataengineering.com">Data Engineering Academy</a>

__Architecture__

![](images/insurance_project_architecture_.png)


# Contents

- [The Data Set](#the-data-set)
- [Used Tools](#used-tools)
  - [Connect](#connect)
  - [Buffer](#buffer)
  - [Processing](#processing)
  - [Storage](#storage)
  - [Visualization](#visualization)
- [Pipelines](#pipelines)
  - [Batch Processing](#batch-processing)
  - [Stream Processing](#stream-processing)
    - [Processing Data Stream](#processing-data-stream)
    - [Storing Data Stream](#storing-data-stream)
  - [Visualizations](#visualizations)
- [Conclusion](#conclusion)
- [Follow Me On](#follow-me-on)
- [Appendix](#appendix)


# The Data Set

I used the same [auto insurance fraud dataset](data/insurance_claims.csv) that I used from my data science project.  

The dataset contains 40 attributes for 1,000 auto insurance claims, 247 that were labeled as fraudulent and 753 that were labeled as non-fraudulent. 

![](images/data_points_in_file.png)

# Used Tools

The tools I used for this project included:

* Python
* SQLAlchemy
* FastAPI
* Apache Kafka
* Apache Spark
* Postgres
* Metabase 
* Docker container for above tools 

## Connect

For this project I have taken an auto claims data CSV file and ingested it into the Postgres database in two ways.  

1. The first way was by streaming. I create a POST API using FastAPI from the CSV file to create a system that would mimic claims streaming into the system like it would in production.
2. The second way was by using Python and SQLAlchemy to bulk import the data into Postgres.

The reason I used two ways was so there was a primary process (__<em>streaming</em>__) and a backup process (__<em>bulk import</em>__). 

 

## Buffer

For the data that is streamed into the system, I used Apache Kafka as a message queue.  As mentioned earlier, I put all the tools in a Docker container. 

In the docker-compose file, I used latest bitnami image for both the Zookeeper and Kafka services,  added them to the same network (__insurance-streaming__) and gave the Kafka service both an internal and external listeners; internal so services on the Docker container can communicate with Kafka and external so communication can be done outside of the Docker container, such when using Postman that is installed on a local client for testing APIs.

``` python
version: "3"

services:
  zookeeper:
    image: 'bitnami/zookeeper:latest'
    ports:
      - '2181:2181'
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    networks:
     - insurance-streaming

  kafka:
    image: 'bitnami/kafka:latest'
    depends_on:
     - zookeeper
    ports:
     - 9093:9093  
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT     #add aditional listener for external
      - KAFKA_CFG_LISTENERS=CLIENT://:9092,EXTERNAL://:9093                              #9092 will be for other containers, 9093 for your windows client
      - KAFKA_CFG_ADVERTISED_LISTENERS=CLIENT://kafka:9092,EXTERNAL://localhost:9093     #9092 will be for other containers, 9093 for your windows client
      - KAFKA_INTER_BROKER_LISTENER_NAME=CLIENT
    networks:
     - insurance-streaming 
```



![Source code](/docker-compose-kafka-spark-postgres-metabase.yml)

## Processing

For the processing of the data, I used Apache Spark. In the docker-compose file, I used the bitnami/spark jupyter/pyspark-notebook:spark-2 image, a volume location where the Spark data will be presisted and added it to the same netork as the other services.

```python
  spark:
    image: 'jupyter/pyspark-notebook:spark-2'
    ports:
      - '8888:8888'
      - "4040-4080:4040-4080"
    volumes:
      - ./ApacheSpark/:/home/jovyan/work
    networks:
      - insurance-streaming
```

![Source code](/docker-compose-kafka-spark-postgres-metabase.yml)

## Storage

For storing the data, I used a PostGres database. The reason I chose Postgres is because it is not only a relational database but also a document store database. Choosing another database such as Oralce, which is only a relational database, or MongoDB, which is only a document store database, would require having to maintaining two seperate databases for the processes, instead of one.

In the docker-compose file I designated, postgres:12.9 for Postgres and dpage/pgadmin4 for PGAdmin as the images, a volume location where the Postgres data will be presisted and assigned it to the same network as the other services.

For security reasons, I used an environment file to hold the Postgres database name, Postgres user name and password, PGadmin email and PGadmin password.  

_*Ignored the postgres.env file for Github._
```python
postgres_ins:
    image: postgres:12.9
    restart: always
    env_file:
      - ./config/postgres.env
    ports:
     - "5432:5432"
    volumes:
     - "/var/lib/postgresql/data/"
    networks:
    - insurance-streaming 
  
  pgadmin:  
    image: dpage/pgadmin4
    env_file:
     - ./config/postgres.env
    ports:
     - "5050:80" 
    networks:
     - insurance-streaming
```


![Source code](/docker-compose-kafka-spark-postgres-metabase.yml)

## Visualization

I used Metabase as the visualiation tool. I chose Metabase because it is open source, could be put in the Docker container and integrates well with PostGres. 

For its image I used metabase/metabase, a volume location where the Metabase data will be presisted and assigned it to the same network as the other services.

For security reasons, I used an environment file to hold the metabase database type, name, port, user name and password. Additionally, the metabase database host, encryption secret key, java timezone and port. 


_*Ignored the metabase_database.env file for Github._

```python
  metabase:
    image: metabase/metabase
    env_file:
     - ./config/metabase_database.env
    volumes:
      # Volumes where Metabase data will be persisted
      - '/var/lib/metabase-data'
    depends_on:
     - 'postgres_ins'
    networks:
      - insurance-streaming 
```
![Source code](/docker-compose-kafka-spark-postgres-metabase.yml)

# Pipelines

## Batch Processing

Using Python, I created a function that checks that the CSV file exists and if so, loads it into a Pandas dataframe. If it doesn't exist, it will throw an error. 

```python
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
```

After importing the file, I inspected the dataframe, looking for anything strange. 

```python
## Inspect the data
print(csv_file_data.head())
```

In order to create the table in Postgres using SQLAlchemy, column names cannot contain certain extended characters  (*, ! and -.) and cannot start with _, even though _ is an allowable extended character. Using Python I checked that the column names adhered to this requirement and if they did not to update them accordingly.
```python
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
```

I then took the dataframe used SQLAcademy to imported it into the Postgres database. 

```python
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
```

![Source code](app_p/bulk_import_auto_insurance_data.py)

## Stream Processing

### __Processing Data Stream__

1. __Create API to mimic production streaming__
  
  To mimic the claims streaming into the system, like it would in production, I created a POST API using FastAPI.

```python
add code here
```

![Source code](API-Ingest/app/main.py)

2. __Prerequisites for API in Docker__
  - Create requirements.txt file to hold libraries needed to build containers. The  library needed is kafak-python for writing to Kafka. 

```python
kafka-python
```

![Source code](API-Ingest/requirements.txt)

  - Create dockerfile for building the container. It installed the kafka-python library so the API can connect to Kafka.

```python
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY requirements.txt /tmp/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --requirement /tmp/requirements.txt

COPY ./app /app
```

![Source code](API-Ingest/dockerfile)


__I then needed to build the API__

I then deployed the API.




docker compose for api ingest
In API-Ingest:
    Dockerfile - need for building our container
    Requirements.txt - for building our container, contain list of libraries that we need (main.py)

build api
deploy api

__Test API using Postman__

Step 1:
From the csv dataframe, I created a new JSON column converting the dataframe object to a JSON string 
Step 2:
Created a new dataframe from just the new JSON column
Step 3:
Saved new json dataframe to a file to be used to create the API

![](images/code/transform_for_api.png)

![Source code](client/transformer.py)

Step 4:
From 

#### Apache Kafka

For docker-compose configuration, see [Kafka](#buffer).

Kafka
create kafka topic


#### Apache Spark
Spark
-get token to get jupyter
-listeng for topic
-write into a new topic
-create session



### Storing Data Stream



## Visualizations

### Metabase




# Conclusion
Write a comprehensive conclusion.
- How did this project turn out
- What major things have you learned
- What were the biggest challenges

# Follow Me On
Add the link to your LinkedIn Profile

# Appendix

[Markdown Cheat Sheet](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet)