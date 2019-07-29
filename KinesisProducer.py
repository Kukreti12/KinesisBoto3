#!/usr/bin/env python
# coding: utf-8

# In[26]:


# sending the data to kinesis stream using csv in chunks

import csv
import json
import boto3
from random import randint
def chunkit(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

kinesis = boto3.client("kinesis")
with open("flights.csv") as f:
    #Creating the ordered Dict
    reader = csv.DictReader(f)
    #putting the json as per the number of chunk we will give in below function 
    #Create the list of json and push like a chunk
    records = chunkit([{"PartitionKey": 'sau', "Data": json.dumps(row)} for row in reader], 500)
    for chunk in records:
        kinesis.put_records(StreamName="Flight-Simulator", Records=chunk)
        


# In[34]:


#Generating the random number of record and sendint to Kinesis data stream

import boto3
import json
from datetime import datetime
import calendar
import random
import time

my_stream_name = 'Flight-Simulator'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def put_to_stream(thing_id, property_value, property_timestamp):
    payload = {
                'prop': str(property_value),
                'timestamp': str(property_timestamp),
                'thing_id': thing_id
              }

    print(payload)

    put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=thing_id)

while True:
    property_value = random.randint(40, 120)
    property_timestamp = calendar.timegm(datetime.utcnow().timetuple())
    thing_id = 'aa-bb'

    put_to_stream(thing_id, property_value, property_timestamp)

    # wait for 5 second
    time.sleep(5)


# In[48]:


#Producer SDK using python 
#Sending the data from CSV to Kinesis data stream row by row
my_stream_name = 'Flight-Simulator'
thing_id ='Saurabh'
kinesis_client = boto3.client('kinesis', region_name='us-east-1')

with open("flights_Test.csv") as f:
    #Creating the ordered Dict
    reader = csv.DictReader(f)
    for row in reader:
        put_response = kinesis_client.put_record(
                    StreamName=my_stream_name,
                    Data=json.dumps(row),
                    PartitionKey=thing_id)


# In[ ]:




