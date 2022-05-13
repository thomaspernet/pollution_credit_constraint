from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim, GoogleV3
import warnings
import pandas as pd
import numpy as np
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from pathlib import Path
import os
import re
import json
import time
import glob
import shutil
import tempfile
from tqdm import tqdm
import pickle
import logging
import io
import boto3
import requests
from bs4 import BeautifulSoup
# Connect to Cloud providers
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
# AWS
con = aws_connector.aws_instantiate(credential=path_cred,
                                    region=region)
client = con.client_boto()
s3 = service_s3.connect_S3(client=client,
                           bucket=bucket, verbose=True)
# Copy destination in S3 without bucket and "/" at the end
PATH_S3 = "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES"

# CREATE LIST CALL
nb_calls = [int(i) for i in list(np.arange(0, 227270, 10))]
# SEND TO S3 -> to trigger Lambda


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


list_calls = list(chunks(nb_calls, 100))
len(list_calls)
PATH_S3_TRIGGER = "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES/TRIGGER_LIST"
j = 0
for i in tqdm(list_calls):
    json_data = {
        'range': i
    }
    FILENAME = "data_{}.json".format(j)
    PATH_LOCAL_RAW = os.path.join(tempfile.gettempdir(), FILENAME)
    with open(PATH_LOCAL_RAW, 'w') as outfile:
        json.dump(json_data, outfile)
    j += 1
    s3.upload_file(PATH_LOCAL_RAW, PATH_S3_TRIGGER)
    os.remove(PATH_LOCAL_RAW)
    time.sleep(15)
# RETRIEVE THE DATA

#j = 228
start_time = time.time()
while len(differences) > 0:
    list_calls = list(chunks(differences, 100))
    for i in tqdm(list_calls):
        json_data = {
            'range': i
        }
        FILENAME = "data_{}.json".format(j)
        PATH_LOCAL_RAW = os.path.join(tempfile.gettempdir(), FILENAME)
        with open(PATH_LOCAL_RAW, 'w') as outfile:
            json.dump(json_data, outfile)
        s3.upload_file(PATH_LOCAL_RAW, PATH_S3_TRIGGER)
        os.remove(PATH_LOCAL_RAW)
        time.sleep(15)
        j += 1
    # check missing
    list_s3 = s3.list_all_files_with_prefix(
        prefix=os.path.join(PATH_S3, 'RAW_JSON'))
    differences = list(set(nb_calls) -
                       set([int(re.findall(r'\d+', i.split("/")[-1])[0])
                            for i in list_s3])
                       )

end_time = time.time() - start_time
print(end_time / 3600)

# read files
warnings.filterwarnings("ignore")
client_ = boto3.resource('s3')
list_s3 = s3.list_all_files_with_prefix(
    prefix=os.path.join(PATH_S3, 'RAW_JSON'))


def convert_to_df(key):
    obj = client_.Object(bucket, key)
    body = obj.get()['Body'].read()
    fix_bytes_value = body.replace(b"'", b'"')
    my_json = json.load(io.BytesIO(fix_bytes_value))
    return pd.DataFrame(my_json)


# append to df -> bit slow: about 200k files -> we could have done it with Athena
df = pd.concat(map(convert_to_df, list_s3))
df.shape
df.to_csv('list_firms.csv', index=False)
################################################################################
################################################################################
################################################################################
# Download extra information
import warnings
warnings.filterwarnings('ignore')
df = pd.read_csv('list_firms.csv', dtype={'id': 'str'})
df.shape

# DOWNLOAD CITIES
query = """
SELECT * FROM "chinese_lookup"."china_city_code_normalised"
"""
df_cities = s3.run_query(
    query=query,
    database="chinese_lookup",
    s3_output='SQL_OUTPUT_ATHENA',
    filename="chinese_cities",  # Add filename to print dataframe
    destination_key='SQL_OUTPUT_ATHENA/CSV',  # Use it temporarily
)

def find_city(x):
    regex = '|'.join(list(df_cities['citycn_correct'].drop_duplicates())) + "|" + \
    '$|'.join(list(df_cities['cityen_correct'].drop_duplicates().str.lower()))
    match = " ".join(re.findall(regex, x))
    if len(match) > 0:
        return match
    else:
        return np.nan


#list_candidates = list(df_cities['citycn_correct'].drop_duplicates()) + \
#list(df_cities['cityen_correct'].drop_duplicates().str.lower())

geolocator = GoogleV3(api_key="AIzaSyA0wx-R4_J3EloZi8HSCch26YELrPBaT3k")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
####
tqdm.pandas()
test = (
    df
    .assign(
        city=lambda x: x.apply(
            lambda x: find_city(x['fullName']), axis=1
        )
    )
)

##### Test NLP 
from polyfuzz.models import TFIDF
from polyfuzz import PolyFuzz

#### TFIDF approach
tfidf = TFIDF(n_gram_range=(3, 3), min_similarity=0, matcher_id="TF-IDF")

tqdm.pandas()
empty_city = test.loc[lambda x: x['city'].isin([np.nan])]
empty_city.shape
### to avoid multiple calls
df_cities_all_missing = pd.DataFrame()
for i in tqdm(range(0, empty_city.shape[0])):
    df_temp = empty_city.iloc[i:i+1,:]
    df_temp['location'] = df_temp['fullName'].apply(geocode)
    #### save it to S3
    FILENAME = "data_id_{}.csv".format(df_temp['id'].values[0])
    PATH_LOCAL_RAW = os.path.join(tempfile.gettempdir(), FILENAME)
    df_temp.to_csv(PATH_LOCAL_RAW, index = False)
    s3.upload_file(
    PATH_LOCAL_RAW,
    "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES_CITY"
    )
    os.remove(PATH_LOCAL_RAW)
    df_cities_all_missing = df_cities_all_missing.append(df_temp)

list_s3 = s3.list_all_files_with_prefix(
    prefix=os.path.join("DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES_CITY"))
len(list_s3)
df_cities_all_missing.shape
df_cities_all_missing.tail()

####
glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://",
bucket,
"DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES_CITY")
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "china"
TablePrefix = 'temp_'  # add "_" after prefix, ex: hello_


glue.create_table_glue(
    target_S3URI,
    name_crawler,
    Role,
    DatabaseName,
    TablePrefix,
    from_athena=False,
    update_schema=False,
)

df_cities_all_missing = pd.concat(map(s3.read_df_from_s3, list_s3))
df_cities_all_missing.head(1000)['location'].unique()


# DOWNLOAD branches
query = """
SELECT * FROM "china"."temp_bank_branches_city"
"""
df_cities_all_missing = s3.run_query(
    query=query,
    database="chinese_lookup",
    s3_output='SQL_OUTPUT_ATHENA',
    filename="chinese_cities",  # Add filename to print dataframe
    destination_key='SQL_OUTPUT_ATHENA/CSV',  # Use it temporarily
)

####

df_cities_all_missing.shape
#### find name
geolocator2 = Nominatim(user_agent="geoapiExercises")
geocode2 = RateLimiter(geolocator2.reverse, min_delay_seconds=1)
def city_state_country(Latitude, Longitude):
    coord = f"{Latitude}, {Longitude}"
    try:
        location = geocode2(coord, exactly_one=True)
        address = location.raw['address']
        city = address.get('city', '')
    except:
        city = None
    return city

df_final_cities_missing_all = pd.DataFrame()
list_missing = []
i

df_cities_all_missing.tail(20)

for i in tqdm(range(65834, df_cities_all_missing.shape[0])):
    df_temp_all_missing = df_cities_all_missing.iloc[i:i+1,:]
    if (df_temp_all_missing['location'].values[0] != None):
        df_final_cities_missing = (
            df_temp_all_missing
            .assign(city_temp =lambda x: x.apply(
                lambda x:
                [i.replace(" ", "") for i in x['location'][0].lower().split(",")],
                 axis=1
            ),
            points = lambda x: x.apply(lambda x: list(x['location'].point) if x['location'] else None, axis =1),
            lat = lambda x: x.apply(lambda x: x['points'][0] if len(x['points'])> 1 else None, axis =1),
            lon= lambda x: x.apply(lambda x: x['points'][1] if len(x['points'])> 1 else None, axis =1),
            altitude = lambda x: x.apply(lambda x: x['points'][2] if len(x['points'])> 1 else None, axis =1),
            test = lambda x: x.apply(lambda x: city_state_country(x['lat'], x['lon']), axis =1),
            city=lambda x: x.apply(
                lambda x: find_city(x['test']), axis=1
                )
            )
        )
        #### save it to S3
        FILENAME = "data_id_reverse_{}.csv".format(df_final_cities_missing['id'].values[0])
        PATH_LOCAL_RAW = os.path.join(tempfile.gettempdir(), FILENAME)
        df_final_cities_missing.to_csv(PATH_LOCAL_RAW, index = False)
        s3.upload_file(
        PATH_LOCAL_RAW,
        "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES_CITY_REVERSE"
        )
        os.remove(PATH_LOCAL_RAW)
        df_final_cities_missing_all = df_final_cities_missing_all.append(df_final_cities_missing)
    else:
        list_missing.append(df_temp_all_missing['id'].values[0])


# temporary save csv
df.iloc[0]
df.to_csv('temp.csv', index=False)

var.to_csv(input_path, index=False)
# SAVE S3
s3.upload_file(input_path, PATH_S3)
# os.remove(input_path)

# ADD SHCEMA
# for i in pd.io.json.build_table_schema(var)['fields']:
#    if i['type'] in ['number', 'integer']:
#        i['type'] = 'int'
#    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':''}))
schema = [
]

# ADD DESCRIPTION
description = ''

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://", bucket, PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = ""
TablePrefix = ''  # add "_" after prefix, ex: hello_


glue.create_table_glue(
    target_S3URI,
    name_crawler,
    Role,
    DatabaseName,
    TablePrefix,
    from_athena=False,
    update_schema=schema,
)

# Add tp ETL parameter files
filename = 'XX.py'
path_to_etl = os.path.join(
    str(Path(path).parent.parent.parent), 'utils', 'parameters_ETL_XX.json')
with open(path_to_etl) as json_file:
    parameters = json.load(json_file)
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name']), '', path))[1:],
    filename
)
table_name = '{}{}'.format(TablePrefix, os.path.basename(target_S3URI).lower())
json_etl = {
    'description': description,
    'schema': schema,
    'partition_keys': [],
    'metadata': {
        'DatabaseName': DatabaseName,
        'TablePrefix': TablePrefix,
        'TableName': table_name,
        'target_S3URI': target_S3URI,
        'from_athena': 'False',
        'filename': filename,
        'github_url': github_url
    }
}


with open(path_to_etl) as json_file:
    parameters = json.load(json_file)

# parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)

index_to_remove = next(
    (
        index
        for (index, d) in enumerate(parameters['TABLES']['CREATION']['ALL_SCHEMA'])
        if d['metadata']['filename'] == filename
    ),
    None,
)
if index_to_remove != None:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(index_to_remove)

parameters['TABLES']['CREATION']['ALL_SCHEMA'].append(json_etl)

with open(path_to_etl, "w")as outfile:
    json.dump(parameters, outfile)
