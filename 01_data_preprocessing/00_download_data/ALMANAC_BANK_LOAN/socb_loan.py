import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from pathlib import Path
import os, re
import json
from tqdm import tqdm

### Connect to Cloud providers
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)

#### AWS
con = aws_connector.aws_instantiate(credential=path_cred,
                                    region=region)
client = con.client_boto()
s3 = service_s3.connect_S3(client=client,
                           bucket=bucket, verbose=True)
PATH_S3 = "DATA/ECON/ALMANAC_OF_CHINA_FINANCE_BANKING/PROVINCES/SOCB_LOAN"  ### Copy destination in S3 without bucket and "/" at the end
### GCP
auth = authorization_service.get_authorization(
    #path_credential_gcp=os.path.join(parent_path, "creds", "service.json"),
    path_credential_drive=os.path.join(parent_path, "creds"),
    verbose=False,
    #scope = ['https://www.googleapis.com/auth/spreadsheets.readonly',
    #"https://www.googleapis.com/auth/drive"]
)
gd_auth = auth.authorization_drive(path_secret = os.path.join(parent_path, "creds", "credentials.json"))
drive = connect_drive.drive_operations(gd_auth)

### DOWNLOAD SPREADSHEET TO temporary_local_data folder
FILENAME_SPREADSHEET = "ALMANAC_BANK_LOAN"
spreadsheet_id = drive.find_file_id(FILENAME_SPREADSHEET, to_print=False)
sheetName = 'DATA_BANKS'
var = (
    drive.upload_data_from_spreadsheet(
        sheetID=spreadsheet_id,
        sheetName=sheetName,
        to_dataframe=True)
)

### READ DATA
var_to_keep = [
"year",
"abc_doc",
"icbc_doc",
"ccb_doc",
"boc_doc",
"province_cn",
"province_en",
"abc_loan",
"icbc_loan",
"ccb_loan",
"boc_loan",
"total_loan_big_four",
"abc_metric",
"icbc_metric",
"ccb_metric",
"boc_metric"
]

var = (
var
.reindex(columns = var_to_keep)
.drop_duplicates(subset = [
"province_cn",
"province_en",
"year"])
)
var.head()

### SAVE LOCALLY
input_path = os.path.join(parent_path,"00_data_catalog", "temporary_local_data",  FILENAME_SPREADSHEET + ".csv")
var.to_csv(input_path, index = False)


#### SAVE S3
s3.upload_file(input_path, PATH_S3)
#os.remove(input_path)
#for i in var.columns:
#    print({"Name":i, "Type":"string", "Comment":""})
### ADD SHCEMA
schema = [
{'Name': 'year', 'Type': 'string', 'Comment': 'year'},
{'Name': 'abc_doc', 'Type': 'string', 'Comment': 'google drive source file for abc'},
{'Name': 'icbc_doc', 'Type': 'string', 'Comment': 'google drive source file for icbc'},
{'Name': 'ccb_doc', 'Type': 'string', 'Comment': 'google drive source file for ccb'},
{'Name': 'boc_doc', 'Type': 'string', 'Comment': 'google drive source file for boc'},
{"Name":"province_cn", "Type":"string", "Comment":"province chinese name"},
{"Name":"province_en", "Type":"string", "Comment":"province english name"},
{'Name': 'abc_loan', 'Type': 'float', 'Comment': 'loan by abc 中国农业发展银行各地区贷款余额 in 亿'},
{'Name': 'icbc_loan', 'Type': 'float', 'Comment': 'loan by icbc 中国工商银行各地区人民币存款贷款余额 各项贷款合计 in 亿'},
{'Name': 'ccb_loan', 'Type': 'float', 'Comment': 'loan by ccb 中国建设银行各地区人民币存款 贷款余额 各项贷款合计 in 亿'},
{'Name': 'boc_loan', 'Type': 'float', 'Comment': 'loan by boc 中国银行各地区人民币存款贷款余额 各项贷款合计 in 亿'},
{'Name': 'total_loan_big_four', 'Type': 'float', 'Comment': 'abc_loan + icbc_loan + ccb_loan + boc_loan'},
{'Name': 'abc_metric', 'Type': 'string', 'Comment': 'metric display either 亿 or 万'},
{'Name': 'icbc_metric', 'Type': 'string', 'Comment': 'metric display either 亿 or 万'},
{'Name': 'ccb_metric', 'Type': 'string', 'Comment': 'metric display either 亿 or 万'},
{'Name': 'boc_metric', 'Type': 'string', 'Comment': 'metric display either 亿 or 万'}

]

### ADD DESCRIPTION
description = 'Download socb loan'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://datalake-datascience", PATH_S3)
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "almanac_bank_china"
TablePrefix = 'bank_' ## add "_" after prefix, ex: hello_


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
filename = 'socb_loan.py'
path_to_etl = os.path.join(str(Path(path).parent.parent.parent), 'utils','parameters_ETL_pollution_credit_constraint.json')
with open(path_to_etl) as json_file:
    parameters = json.load(json_file)
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name'])
               , '', path))[1:],
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
        'github_url':github_url
    }
}


with open(path_to_etl) as json_file:
    parameters = json.load(json_file)

#parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)

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
