import pandas as pd
import numpy as np
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from pathlib import Path
import os
import re
import json
from tqdm import tqdm

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
PATH_S3 = "DATA/ECON/CHINA/PROVINCE/MACRO_DATA"  # Copy destination in S3 without bucket and "/" at the end
# GCP
auth = authorization_service.get_authorization(
    #path_credential_gcp=os.path.join(parent_path, "creds", "service.json"),
    path_credential_drive=os.path.join(parent_path, "creds"),
    verbose=False,
    scope=['https://www.googleapis.com/auth/spreadsheets.readonly',
           "https://www.googleapis.com/auth/drive"]
)
gd_auth = auth.authorization_drive(path_secret=os.path.join(
    parent_path, "creds", "credentials.json"))
drive = connect_drive.drive_operations(gd_auth)

# DOWNLOAD DATA TO temporary_local_data folder
FILENAME_DRIVE = 'base_city.dta'
FILEID = drive.find_file_id(FILENAME_DRIVE, to_print=False)

var = (
    drive.download_file(
        filename=FILENAME_DRIVE,
        file_id=FILEID,
        local_path=os.path.join(parent_path, "00_data_catalog", "temporary_local_data"))
)

# READ DATA
input_path = os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", FILENAME_DRIVE)

# preprocess data
var = (
    pd.read_stata(input_path)
    .replace(',', '', regex=True)
    # .replace('#N/A|#VALUE!|\?\?\?', np.nan, regex=True)
    # .replace('', np.nan)
    # .replace('\n', ' ', regex=True)
)

var.columns = (var.columns
               .str.strip()
               .str.replace(' ', '_')
               .str.replace('-', '_')
               .str.replace('.', '', regex=True)
               .str.lower()
               .str.replace('\n', '')
               .str.replace("\%", '_pct', regex=True)
               .str.replace('_#', '')
               .str.replace('\_\(', '_', regex=True)
               .str.replace('\)', '', regex=True)
               .str.replace('_\+_', '', regex=True)
               .str.replace('\/', '_', regex=True)
               .str.replace('__', '_')
               )

var = var.assign(**{
    "{}".format(col): var[col]
    .astype(str)
    .str.replace('\,', '', regex=True)
    .str.replace(r'\/|\(|\)|\?|\.|\:|\-', '', regex=True)
    .str.replace('__', '_')
    .str.replace('\\n', '', regex=True)
    .str.replace('\"','', regex=True)
    .str.upper() for col in var.select_dtypes(include="object").columns
}
)

var.to_csv(input_path, index=False)
# SAVE S3
s3.upload_file(input_path, PATH_S3)
os.remove(input_path)

# ADD SHCEMA
#for i in pd.io.json.build_table_schema(var)['fields']:
#    if i['type'] in ['number', 'integer']:
#        i['type'] = 'int'
#    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':i['name']}))
schema = [
{'Name': 'index', 'Type': 'int', 'Comment': 'index'},
{'Name': 'provincecn', 'Type': 'string', 'Comment': 'provincecn'},
{'Name': 'provincecode', 'Type': 'int', 'Comment': 'provincecode'},
{'Name': 'citycn', 'Type': 'string', 'Comment': 'citycn'},
{'Name': 'cityen', 'Type': 'string', 'Comment': 'cityen'},
{'Name': 'year', 'Type': 'int', 'Comment': 'year'},
{'Name': 'gdp', 'Type': 'int', 'Comment': 'gdp'},
{'Name': 'gdpindex', 'Type': 'int', 'Comment': 'gdpindex'},
{'Name': 'ratio1', 'Type': 'int', 'Comment': 'ratio1'},
{'Name': 'ratio2', 'Type': 'int', 'Comment': 'ratio2'},
{'Name': 'ratio3', 'Type': 'int', 'Comment': 'ratio3'},
{'Name': 'population', 'Type': 'int', 'Comment': 'population'},
{'Name': 'nonagri_population', 'Type': 'int', 'Comment': 'nonagri_population'},
{'Name': 'popgrowth', 'Type': 'int', 'Comment': 'popgrowth'},
{'Name': 'popdensity', 'Type': 'int', 'Comment': 'popdensity'},
{'Name': 'employment', 'Type': 'int', 'Comment': 'employment'},
{'Name': 'urbanindiworker', 'Type': 'int', 'Comment': 'urbanindiworker'},
{'Name': 'employratio1', 'Type': 'int', 'Comment': 'employratio1'},
{'Name': 'employratio2', 'Type': 'int', 'Comment': 'employratio2'},
{'Name': 'employratio3', 'Type': 'int', 'Comment': 'employratio3'},
{'Name': 'totalworker', 'Type': 'int', 'Comment': 'totalworker'},
{'Name': 'totalworkerwage', 'Type': 'int', 'Comment': 'totalworkerwage'},
{'Name': 'averageworkerwage', 'Type': 'int', 'Comment': 'averageworkerwage'},
{'Name': 'totalworkerexpenditure', 'Type': 'int', 'Comment': 'totalworkerexpenditure'},
{'Name': 'fixedasset', 'Type': 'int', 'Comment': 'fixedasset'},
{'Name': 'realestateinvest', 'Type': 'int', 'Comment': 'realestateinvest'},
{'Name': 'budgetincome', 'Type': 'int', 'Comment': 'budgetincome'},
{'Name': 'budgetexpend', 'Type': 'int', 'Comment': 'budgetexpend'},
{'Name': 'budgetsicence', 'Type': 'int', 'Comment': 'budgetsicence'},
{'Name': 'budgeteducation', 'Type': 'int', 'Comment': 'budgeteducation'},
{'Name': 'urbanresidentsaving', 'Type': 'int', 'Comment': 'urbanresidentsaving'},
{'Name': 'totalcityarea', 'Type': 'int', 'Comment': 'totalcityarea'},
{'Name': 'urbanarea', 'Type': 'int', 'Comment': 'urbanarea'},
{'Name': 'agrilandarea', 'Type': 'int', 'Comment': 'agrilandarea'},
{'Name': 'agrilandperson', 'Type': 'int', 'Comment': 'agrilandperson'},
{'Name': 'meatproduction', 'Type': 'int', 'Comment': 'meatproduction'},
{'Name': 'aquaticproduction', 'Type': 'int', 'Comment': 'aquaticproduction'},
{'Name': 'vegetableproduction', 'Type': 'int', 'Comment': 'vegetableproduction'},
{'Name': 'fruitproduction', 'Type': 'int', 'Comment': 'fruitproduction'},
{'Name': 'meatprodperson', 'Type': 'int', 'Comment': 'meatprodperson'},
{'Name': 'milkprodperson', 'Type': 'int', 'Comment': 'milkprodperson'},
{'Name': 'aquaticprodperson', 'Type': 'int', 'Comment': 'aquaticprodperson'},
{'Name': 'vegetableprodperson', 'Type': 'int', 'Comment': 'vegetableprodperson'},
{'Name': 'eadproduction', 'Type': 'int', 'Comment': 'eadproduction'},
{'Name': 'eadnumber', 'Type': 'int', 'Comment': 'eadnumber'},
{'Name': 'eadsale', 'Type': 'int', 'Comment': 'eadsale'},
{'Name': 'eadtotaltax', 'Type': 'int', 'Comment': 'eadtotaltax'},
{'Name': 'eadtotalvatax', 'Type': 'int', 'Comment': 'eadtotalvatax'},
{'Name': 'eadfixassest', 'Type': 'int', 'Comment': 'eadfixassest'},
{'Name': 'eadliquidassest', 'Type': 'int', 'Comment': 'eadliquidassest'},
{'Name': 'totalpassenger', 'Type': 'int', 'Comment': 'totalpassenger'},
{'Name': 'totalrailwaypassenger', 'Type': 'int', 'Comment': 'totalrailwaypassenger'},
{'Name': 'totalroadpassenger', 'Type': 'int', 'Comment': 'totalroadpassenger'},
{'Name': 'totalfreight', 'Type': 'int', 'Comment': 'totalfreight'},
{'Name': 'totalrailwayfrieght', 'Type': 'int', 'Comment': 'totalrailwayfrieght'},
{'Name': 'totalroadfrieght', 'Type': 'int', 'Comment': 'totalroadfrieght'},
{'Name': 'socialcomsuptionsale', 'Type': 'int', 'Comment': 'socialcomsuptionsale'},
{'Name': 'fdicontracts', 'Type': 'int', 'Comment': 'fdicontracts'},
{'Name': 'fdicontractmoney', 'Type': 'int', 'Comment': 'fdicontractmoney'},
{'Name': 'fdi', 'Type': 'int', 'Comment': 'fdi'},
{'Name': 'watersupply', 'Type': 'int', 'Comment': 'watersupply'},
{'Name': 'watersupplyperson', 'Type': 'int', 'Comment': 'watersupplyperson'},
{'Name': 'electricityuse', 'Type': 'int', 'Comment': 'electricityuse'},
{'Name': 'electricityuseperson', 'Type': 'int', 'Comment': 'electricityuseperson'},
{'Name': 'householdcoalgasuse', 'Type': 'int', 'Comment': 'householdcoalgasuse'},
{'Name': 'householdgasuse', 'Type': 'int', 'Comment': 'householdgasuse'},
{'Name': 'totalroad', 'Type': 'int', 'Comment': 'totalroad'},
{'Name': 'totalroadperson', 'Type': 'int', 'Comment': 'totalroadperson'},
{'Name': 'publicbus', 'Type': 'int', 'Comment': 'publicbus'},
{'Name': 'publicbus10thousandperson', 'Type': 'int', 'Comment': 'publicbus10thousandperson'},
{'Name': 'publicbuspassenger', 'Type': 'int', 'Comment': 'publicbuspassenger'},
{'Name': 'taxi', 'Type': 'int', 'Comment': 'taxi'},
{'Name': 'postoffice', 'Type': 'int', 'Comment': 'postoffice'},
{'Name': 'college', 'Type': 'int', 'Comment': 'college'},
{'Name': 'collegeteacher', 'Type': 'int', 'Comment': 'collegeteacher'},
{'Name': 'collegestudent', 'Type': 'int', 'Comment': 'collegestudent'},
{'Name': 'middleschool', 'Type': 'int', 'Comment': 'middleschool'},
{'Name': 'middleschoolteacher', 'Type': 'int', 'Comment': 'middleschoolteacher'},
{'Name': 'middleschoolstudent', 'Type': 'int', 'Comment': 'middleschoolstudent'},
{'Name': 'primaryschool', 'Type': 'int', 'Comment': 'primaryschool'},
{'Name': 'primaryschoolteacher', 'Type': 'int', 'Comment': 'primaryschoolteacher'},
{'Name': 'primarystudents', 'Type': 'int', 'Comment': 'primarystudents'},
{'Name': 'humancapital', 'Type': 'int', 'Comment': 'humancapital'},
{'Name': 'publiclibarybook', 'Type': 'int', 'Comment': 'publiclibarybook'},
{'Name': 'publiclibarybookperson', 'Type': 'int', 'Comment': 'publiclibarybookperson'},
{'Name': 'hospital', 'Type': 'int', 'Comment': 'hospital'},
{'Name': 'hospitalbeds', 'Type': 'int', 'Comment': 'hospitalbeds'},
{'Name': 'doctors', 'Type': 'int', 'Comment': 'doctors'},
{'Name': 'gardengreenland', 'Type': 'int', 'Comment': 'gardengreenland'},
{'Name': 'urbangardengreenlandrate', 'Type': 'int', 'Comment': 'urbangardengreenlandrate'},
{'Name': 'regions', 'Type': 'int', 'Comment': 'regions'},
{'Name': 'firmdensity', 'Type': 'int', 'Comment': 'firmdensity'},
{'Name': 'urbanization', 'Type': 'int', 'Comment': 'urbanization'},
{'Name': 'exchangerate', 'Type': 'int', 'Comment': 'exchangerate'},
{'Name': 'fdirmb', 'Type': 'int', 'Comment': 'fdirmb'},
{'Name': 'interdistance', 'Type': 'int', 'Comment': 'interdistance'},
{'Name': 'geocode4_corr', 'Type': 'string', 'Comment': 'geocode4_corr'}
]

# ADD DESCRIPTION
description = 'Province chinese macro data'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://",bucket, PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "china"
TablePrefix = 'province_'  # add "_" after prefix, ex: hello_


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
filename = 'province.py'
path_to_etl = os.path.join(
    str(Path(path).parent.parent.parent), 'utils', 'parameters_ETL_pollution_credit_constraint.json')
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
