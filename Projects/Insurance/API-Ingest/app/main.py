# You need this to use FastAPI, work with statuses and be able to end HTTPExceptions
from fastapi import FastAPI, status, HTTPException
 
# You need this to be able to turn classes into JSONs and return
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
