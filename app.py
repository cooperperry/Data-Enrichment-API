'''
This project pulls data from a public API which is then parsed to JSON and input into a dataframe.
We use a Pandas dataframe and a SQLAlchemy engine to upload the data into a Postgres database.

Author:
    Cooper Perry

Date:
    2021-06-30

Requires:
    Python packages:
      - pip3 install -r requirements.txt

'''

#Import required packages
import traceback
import base64
from configparser import ConfigParser
from sqlalchemy import create_engine
import sqlalchemy
import json
import os
import pandas as pd
import requests
import sys
import time
import logging
from requests import api



try:

   # Set logging
    #In this section add logging that documents the name of this job and the version
    logging.basicConfig(filename='API-to-DB/api2db.log', encoding='utf-8', level=logging.DEBUG)
    logging.info("soagateway-serviceresponse-buildings")
    logging.info("soagateway-serviceresponse-buildings version 2")

    
    #Check reference to config
    config = ConfigParser()
    config.read('API-to-DB/config/config.ini')
    

    # Set Vairables needed to call API (query string, headers, url)
    apiurl = config['api']['url']
    apikey = config['api']['key']
    querystr = config['api']['query']

    # Set Variables need to connect to the Database (db conn string, table name)
    dbconnstr = config['postgres']['conn']
    schemanm = config['postgres']['schemanm']
    schema_tablenm = config['postgres']['schema_tablenm']
    tablenm = config['postgres']['tablenm']
 

    # Make API Call(s)
    api_start_time = time.time()
    logging.info("Beginning API Calls from ServiceResponse")

    r = requests.get(apiurl + apikey)
    data = r.json()
    recordCount = len(data['ServiceResponse']['Buildings'])

    logging.info("Record Count: " + str(recordCount))
    logging.info("Processing api calls took {:.2f} seconds.".format(time.time() - api_start_time))


    #Start Processing Records into Dataframe
    logging.info("Start load dataframe ....")
    df_load_start_time = time.time()

    dfAPI = pd.DataFrame(data['ServiceResponse']['Buildings'])
    dfAPI.columns = ["address_3", "site", "status", "building", "description", "addr1_alias", "msag_alias", "building_abbr", "usage_description", "address_1", "address_2", "longitude", "building_prose", "latitude", "historical_alias", "historical_name"]

    logging.info("Processing load dataframe took {:.2f} seconds.".format(time.time() - df_load_start_time))

    #Load information into Table
    engine = create_engine(dbconnstr)   
    connection = engine.connect()
    truncate_query = sqlalchemy.text("TRUNCATE TABLE {}".format(tablenm))
    connection.execution_options(autocommit=True).execute(truncate_query)    
    dfAPI.to_sql(tablenm, engine,index = False, if_exists='append')
    connection.close()

    # Confirm that number of records in your API call match the number of records inserted
    inserted = len(dfAPI.index)
    logging.info("Records Inserted: " + str(inserted))
    if (recordCount != inserted):
        logging.warning("Job Failed, rows from API and rows inserted do not match")
        sys.exit(1)
    else:
        logging.info("Job completed successfully")
        
    # Would like commit to occur only if job is successful

except: # catch *all* exceptions
    logging.error(traceback.print_exc())
    sys.exit(1)

sys.exit(0)    
