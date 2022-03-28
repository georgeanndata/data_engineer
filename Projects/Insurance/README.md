
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
- [Follow Me On](#follow-me-on)


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

1. The first way was by streaming. I create a POST API using FastAPI from the CSV file to mimic claims streaming into the system like it would in production.
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



![Docker-compose](docker-compose-kafka-spark-postgres-metabase.yml)

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

![Docker-compose](docker-compose-kafka-spark-postgres-metabase.yml)

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


![Docker-compose](docker-compose-kafka-spark-postgres-metabase.yml)

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
![Docker-compose](docker-compose-kafka-spark-postgres-metabase.yml)

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

I then took the dataframe and used SQLAcademy to import the data into the Postgres database. 

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
Validate table was created in database:
![](images/autoclaims_table_bulk_import.png)


![Source code](app_p/bulk_import_auto_insurance_data.py)

## Stream Processing

### __Processing Data Stream__
<br>
#### __1. Create and Test API__
<br>

__Create API to mimic production streaming__
  
  To mimic the claims streaming into the system, like it would in production, I created a POST API using FastAPI.

```python
# To use FastAPI, work with statuses and be able to end HTTPExceptions
from fastapi import FastAPI, status, HTTPException
 
# To turn classes into JSONs and return
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# Needed for json.dumps
import json

# Both used for BaseModel
from pydantic import BaseModel

from datetime import datetime
from kafka import KafkaProducer, producer


# Create class (schema) for the JSON
# Date get's ingested as string and then before writing validated
class AutoClaim(BaseModel):
    months_as_customer: int
    age:  int
    policy_number: int
    policy_bind_date: str 
    policy_state: str
    policy_csl: str 
    policy_deductable: int
    policy_annual_premium: float 
    umbrella_limit: int
    insured_zip: int 
    insured_sex: str 
    insured_education_level: str 
    insured_occupation: str 
    insured_hobbies: str        
    insured_relationship: str  
    capital_gains: int   
    capital_loss: int  
    incident_date: str 
    incident_type: str 
    collision_type: str 
    incident_severity: str 
    authorities_contacted: str 
    incident_state: str 
    incident_city: str 
    incident_location: str 
    incident_hour_of_the_day: int 
    number_of_vehicles_involved: int 
    property_damage: str
    bodily_injuries: int 
    witnesses: int 
    police_report_available: str 
    total_claim_amount: int 
    injury_claim: int 
    property_claim: int 
    vehicle_claim: int 
    auto_make: str 
    auto_model: str 
    auto_year: int 
    fraud_reported: str
    c39: str = None
 

# This is important for general execution and the docker later
app = FastAPI()

# Base URL
@app.get("/")
async def root():
    return {"message": "Hello World"}

# Add a new auto claim
@app.post("/autoclaim")
async def post_auto_claim(item: AutoClaim): #body awaits a json with invoice item information
    print("Message received")
    try:
        # Evaluate the timestamp and parse it to datetime object you can work with
        ###date = datetime.strptime(item.policy_bind_date, "%d/%m/%Y %H:%M")

        ###print('Found a timestamp: ', date)

        # Replace strange date with new datetime
        # Use strftime to parse the string in the right format (replace / with - and add seconds)
        ###item.AutoClaim = date.strftime("%d-%m-%Y %H:%M:%S")
        ###print("New item date:", item.incident_date)
        
        # Parse item back to json
        json_of_item = jsonable_encoder(item)
        
        # Dump the json out as string
        json_as_string = json.dumps(json_of_item)
        print(json_as_string)
        
        # Produce the string
        produce_kafka_string(json_as_string)

        # Encode the created customer item if successful into a JSON and return it to the client with 201
        return JSONResponse(content=json_of_item, status_code=201)
    
    # Will be thrown by datetime if the date does not fit
    # All other value errors are automatically taken care of because of the InvoiceItem Class
    except ValueError:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)
        

def produce_kafka_string(json_as_string):
    # Create producer
         # below when testing API-Ingest to Docker
        producer = KafkaProducer(bootstrap_servers='kafka:9092',acks=1,  api_version=(0,11,5))
       
        # Write the string as bytes because Kafka needs it this way
        producer.send('auto-claims-ingestion-topic', bytes(json_as_string, 'utf-8'))
        producer.flush()


```

![Source code](API-Ingest/app/main.py)
<br>

__Prerequisites for API in Docker__
 1. Create requirements.txt file to hold the libraries needed to build the container. The  library needed is kafka-python for writing to Kafka. 

```python
kafka-python
```

![Source code](API-Ingest/requirements.txt)<br>

  2. Create dockerfile for building the container. It installed the kafka-python library so the API can connect to Kafka.

```python
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

COPY requirements.txt /tmp/

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --requirement /tmp/requirements.txt

COPY ./app /app
```

![Source code](API-Ingest/dockerfile)

<br>

__Build the API__

In Terminal, I entered the following in the API-Ingest folder of project to build the API image
  ```python
  docker build -t api-ingest .
```
<br>

__Deploy the API__


In Terminal, I entered the following in from the project folder to deploy the API

```python
docker run --rm --network insurance-streaming --name my-api-ingest -p 80:80 api-ingest
```

__Test API using Postman__

__Create JSON file__<br>

__Step 1__:
From the csv dataframe, I created a new JSON column converting the dataframe object to a JSON string<br> 
__Step 2__:
Created a new dataframe from just the new JSON column<br>
__Step 3__:
Saved new json dataframe to a file to be used to create the API

```python
import numpy as np
from numpy import add
import pandas as pd

####################################################
# DataFrame add JSON for API
#####################################################

def json_api (csv):
    ## make a copy of csv dataframe to apply json for API
    data_json_tmp = pd.DataFrame(csv_file_data)

    ## Creat new JSON column from the CSV dataframe
    data_json_tmp['json'] = data_json_tmp.to_json(orient='records', lines=True).splitlines()

    ## Take out json column and put it into new json dataframe
    json_columns = data_json_tmp['json']
 
    ## print out new json dataframe to file
    np.savetxt(r'./json.txt', json_columns.values, fmt='%s')
    print('File has been created!')

json_api(csv_file_data)
```

![Source code](client/transformer.py)

__Step 4__:

From Postman, import file and enter localhost:80/autoclaims.  <br>
*__(201 returned)__*

![](images/postman.png)


#### __2.  Apache Kafka__
<br>
<strong>Step 1</strong>: Create topic
  - From <strong>/opt/bitnami/kafka/bin</strong> in the Kafka container, enter: <br>
  <strong><em>kafka-topics.sh --create --topic auto-claims-ingestion-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1</em></strong><br>
  <strong>Step 2</strong>: Create topic for Spark output
  - From <strong>/opt/bitnami/kafka/bin</strong> in the Kafka container, enter: <br>
   <strong><em>./kafka-topics.sh --create --topic spark-output --bootstrap-server localhost:9092  --partitions 1 --replication-factor 1</em></strong>


<br>
<br>
*For docker-compose configuration, see [Kafka](#buffer)*

<br>

#### __3. Apache Spark__
<br>

__Step 1__:  Create local consumer for spark topic
<strong><em>
./kafka-console-consumer.sh --topic spark-output --bootstrap-server localhost:9092</em></strong>

__Step 2__: Get token for jupyter (from VS Code logs)

__Step 3__: Create session
```python
from pyspark.sql import SparkSession

# Create Spark session & context
spark = (SparkSession
         .builder
         .master('local')
         .appName('kafka-streaming')
         # Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
         .getOrCreate())
#sc = spark.sparkContext

```
__Step 4__: Read message from Kafka

```python
# Read the message from the kafka stream
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "auto-claims-ingestion-topic") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
```
__Step 5__: Create a small temporary view

```python
#Create a small temporary view for SparkSQL
df.createOrReplaceTempView("message")
```
__Step 6__: Write the message to the console
```python
# Write out the message to the console of the environment
res = spark.sql("SELECT * from message")
res.writeStream.format("console") \
            .outputMode("append") \
            .start() 
```

__Step 7__: Write the message back to another topic
```python
# Write the message back into Kafka in another topic that you are going to listen to with a local consumer
ds = df \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("topic", "spark-output") \
  .option("checkpointLocation", "/tmp") \
  .start() \
  .awaitTermination()
```

__Step 8__: Test using Postman

From Postman, import file and enter localhost:80/autoclaims.  <br>
*__(201 returned)__*

![](images/postman.png)

![Source code](ApachSpark/01-streaming-kafka-src-dst.ipynb)

*For docker-compose configuration, see [Spark](#processing)*

### __Storing Data Stream__

Adding onto the Spark processing above:

__Step 1__: Add Postgres packages and Postgres configurations to the SparkSession
```python
         # Versions need to match the Spark version (trial & error)
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,org.postgresql:postgresql:42.3.1")
         # Postgres config including the username and password from compose file
         .config("spark.postgres.input.uri","jdbc:postgresql://postgres_ins:5432/postgresdb.autoclaims_docs?user=postgres&password=data")
         .config("spark.postgres.output.uri","jdbc:postgresql://postgres_ins:5432/postgresdb.autoclaims_docs?user=postgres&password=data")
         .config('spark.sql.session.timeZone', "America/New_York")
         .getOrCreate())
```
__Step 2__: Add the following reading the message from Kafka

```python
# Read the message from the kafka stream

# convert the binary values to string
df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
```

__Step 3__: Add to write message into Postgres
```python
from pyspark.sql.functions import from_json, current_timestamp

# Write the message into Posgres
def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF in this foreach
    # writes the dataframe with complete kafka message into postgres
 
    db_target_url = "jdbc:postgresql://postgres_ins:5432/postgresdb"
    table = "autoclaims_docs" #public.autoclaims_docs
    db_target_properties =  {"driver": 'org.postgresql.Driver',"user":"postgres", "password":"data"}

    
    #Transform the values of all rows in column value and create a dataframe out of it (will also only have one row)
    df2=df.withColumn("value",from_json(df.value,MapType(StringType(),StringType())))  
   
    # Transform the dataframe so that it will have individual columns 
    df3 = df2.select(["value.months_as_customer","value.age","value.policy_number","value.policy_bind_date", \
    "value.policy_state","value.policy_csl","value.policy_deductable","value.policy_annual_premium", \
    "value.umbrella_limit","value.insured_zip","value.insured_sex","value.insured_education_level", \
    "value.insured_occupation","value.insured_hobbies","value.insured_relationship","value.capital_gains", \
    "value.capital_loss","value.incident_date","value.incident_type","value.collision_type",  \
    "value.incident_severity","value.authorities_contacted","value.incident_state","value.incident_city",  \
    "value.incident_location","value.incident_hour_of_the_day","value.number_of_vehicles_involved","value.property_damage", \
    "value.bodily_injuries","value.witnesses","value.police_report_available","value.total_claim_amount", \
    "value.injury_claim","value.property_claim", "value. vehicle_claim","value.auto_make", "value.auto_model", \
    "value.auto_year","value.fraud_reported","value.c39"])
    
    ## add load datetime column to df3
    df3 = df3.withColumn("load_datetimestamp", current_timestamp())##convert in Postgres
    
    # Send the dataframe into Postgres which will create a JSON document out of it
    db_target_url = "jdbc:postgresql://postgres_ins:5432/postgresdb"
    table = "autoclaims_docs" #public.autoclaims_docs
    db_target_properties =  {"driver": 'org.postgresql.Driver',"user":"postgres", "password":"data"}
    
    df3.write.jdbc(url=db_target_url, table=table, properties=db_target_properties, mode="append")#.mode("append")
    #df3.show()
    pass
```
![Source code](ApachSpark/02-streaming-kafka-src-dst-postgres.ipynb)

__Step 7__: Test using Postman

From Postman, import file and enter localhost:80/autoclaims.  <br>
*__(201 returned)__*

![](images/postman.png)

__Step 8__: Log into Postgres to ensure table and data is created

![](images/db_docs.png)

Log files:
![](images/log.png)

Spark jobs:
![](images/sparkjobs.png)

<br>

![Docker-compose](docker-compose-kafka-spark-postgres-metabase.yml)

## __Visualizations__

### __Metabase__

For the visualization of the data, I used the open source tool Metabase. In the docker-compose file, I used the metabase/metabase image, a volume location where the Metabase data will be presisted, its dependences on postgres and added it to the same netork as the other services.

```python
metabase:
    image: metabase/metabase
    restart: "always"
    ports:
    - 3000:3000
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
Metabase_database.env holds the environment information 
```python
ENV MB_DB_TYPE=postgres
ENV MB_DB_DBNAME= <database name>
ENV MB_DB_PORT= 5432
ENV MB_DB_USER= <metabase user>
ENV MB_DB_PASS= <metabase password>
# Make sure the firewall at the database server allows connections to port 54320
ENV MB_DB_HOST=localhost
ENV MB_ENCRYPTION_SECRET_KEY= <random key>
ENV MB_JAVA_TIMEZONE=America/New_York
ENV MB_PORT=3000
```
<br>
<strong>Step 1</strong>: 
Add Postgres database to Metabase

![](images/metabase_db_config.png)

Tables in database

![](images/tables_n_db.png)

Data in table

![](images/data_n_table.png)

<strong>__Step 2__</strong>: 
Create some graphs and reports

![](images/dashboard.png)

![Docker-compose](docker-compose-kafka-spark-postgres-metabase.yml)


# Follow Me On
My LinkedIn Profile: https://www.linkedin.com/in/gasauer/
