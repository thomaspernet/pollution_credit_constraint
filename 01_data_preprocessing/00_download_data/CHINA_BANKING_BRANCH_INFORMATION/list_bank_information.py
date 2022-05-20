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
PATH_S3 = "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_INFORMATION"  # Copy destination in S3 without bucket and "/" at the end
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
FILENAME_DRIVE = '银行基本信息143150497.zip'
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


import zipfile
with zipfile.ZipFile(input_path, 'r') as zip_ref:
    zip_ref.extractall(os.path.join(parent_path, "00_data_catalog",
                              "temporary_local_data"))
# preprocess data
file_path = os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data","BANK_Info.xlsx")
var = (
    pd.read_excel(file_path, skiprows = [1, 2])
    .replace(',', '.', regex=True)
    .replace('#N/A|#VALUE!|\?\?\?', np.nan, regex=True)
    .replace('', np.nan)
    .replace('\n', ' ', regex=True)
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
    .str.replace(r'\/|\(|\)|\?|\:|\-', '', regex=True)
    .str.replace('__', '_')
    .str.replace('\\n', '', regex=True)
    .str.replace('\"','', regex=True)
    .str.upper() for col in var.select_dtypes(include="object").columns
}
)

var.to_csv(os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", "bank_info.csv"), index=False)
# SAVE S3
s3.upload_file(os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", "bank_info.csv"), PATH_S3)
os.remove(os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", "BANK_Info.xlsx"))
os.remove(os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", "BANK_Info[DES][xlsx].txt"))
os.remove(os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", "版权声明.pdf"))
os.remove(os.path.join(parent_path, "00_data_catalog",
                          "temporary_local_data", "银行基本信息143150497.zip"))
# ADD SHCEMA
#for i in pd.io.json.build_table_schema(var)['fields']:
#    if i['type'] in ['number', 'integer']:
#        i['type'] = 'int'
#    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':''}))
schema = [
{'Name': 'index', 'Type': 'int', 'Comment': ''},
{'Name': 'bankcd', 'Type': 'int', 'Comment': 'Bank Code'},
{'Name': 'bnm_cn', 'Type': 'string', 'Comment': 'Bank full name in Chinese'},
{'Name': 'shortbnm', 'Type': 'string', 'Comment': 'Bank abbreviation in Chinese'},
{'Name': 'clsdt', 'Type': 'string', 'Comment': 'Updated'},
{'Name': 'bnature', 'Type': 'int', 'Comment': 'Bank nature'},
{'Name': 'provincename', 'Type': 'string', 'Comment': 'Province'},
{'Name': 'cityname', 'Type': 'string', 'Comment': 'City'},
{'Name': 'registeraddress', 'Type': 'string', 'Comment': 'address'},
{'Name': 'largshrhnm', 'Type': 'string', 'Comment': 'The name of the largest shareholder'},
{'Name': 'largshrhnt', 'Type': 'string', 'Comment': 'The nature of the largest shareholder'},
{'Name': 'largshrhshareholding', 'Type': 'int', 'Comment': 'Shareholding ratio of the largest shareholder'},
]

# ADD DESCRIPTION
description = 'list of bank information'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://",bucket, PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "china"
TablePrefix = 'china_'  # add "_" after prefix, ex: hello_


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
filename = 'list_bank_information.py'
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
