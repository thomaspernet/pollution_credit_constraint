---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernel_info:
    name: python3
  kernelspec:
    display_name: SoS
    language: sos
    name: sos
---

<!-- #region kernel="SoS" -->
# US Name
Estimate SO2 emission as a function of  period and others variables


# Description

- Effect of credit supply

## Variables
### Target

SO2 emission

### Features

- credit_supply
- credit_supply_long_term
- credit_supply_short_ term
- share_ big bank_loan
- share_big_loan

## Complementary information

Title: Add credit supply
Use the new variables such as credit supply in the model to interact with FYP and period
Add tfp?

# Metadata

- Key: 171_Pollution_and_Credit_Constraint
- Epic: Models
- US: Credit supply
- Task tag: #data-analysis, #credit-supply
- Analytics reports: 

# Input Cloud Storage

## Table/file

**Name**

- https://github.com/thomaspernet/pollution_credit_constraint/blob/master/01_data_preprocessing/02_transform_tables/00_credit_constraint_industry.md

**Github**

- fin_dep_pollution_baseline_industry


<!-- #endregion -->

<!-- #region kernel="SoS" -->
# Connexion server
<!-- #endregion -->

```sos kernel="SoS"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
#import seaborn as sns
import os, shutil, json
import sys
import re
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```sos kernel="SoS"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
glue = service_glue.connect_glue(client = client) 
```

```sos kernel="SoS"
pandas_setting = True
if pandas_setting:
    #cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
os.environ['KMP_DUPLICATE_LIB_OK']='True'

```

```sos kernel="R"
change_target <- function(table){
    ## Regime
    check_target <- grep("periodTRUE:tso2_mandate_c:credit_constraint", rownames(table$coef))
    
    if (length(check_target) !=0) {
    ## SOE
    rownames(table$coefficients)[check_target] <- 'credit_constraint:periodTRUE:tso2_mandate_c'
    rownames(table$beta)[check_target] <- 'credit_constraint:periodTRUE:tso2_mandate_c'
    } 
    return (table)
}
```

<!-- #region kernel="SoS" -->
# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types
<!-- #endregion -->

```sos kernel="SoS"
db = 'environment'
table = 'fin_dep_pollution_baseline_industry'
```

```sos kernel="SoS"
dtypes = {}
schema = (glue.get_table_information(database = db,
                           table = table)
          ['Table']['StorageDescriptor']['Columns']
         )
for key, value in enumerate(schema):
    if value['Type'] in ['varchar(12)',
                         'varchar(3)',
                        'varchar(14)', 'varchar(11)']:
        format_ = 'string'
    elif value['Type'] in ['decimal(21,5)', 'double', 'bigint', 'int', 'float']:
        format_ = 'float'
    else:
        format_ = value['Type'] 
    dtypes.update(
        {value['Name']:format_}
    )
```

```sos kernel="SoS"
download_data = True
filename = 'df_{}'.format(table)
full_path_filename = 'SQL_OUTPUT_ATHENA/CSV/{}.csv'.format(filename)
path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalog/temporary_local_data")
df_path = os.path.join(path_local, filename + '.csv')
if download_data:
    
    s3 = service_s3.connect_S3(client = client,
                          bucket = bucket, verbose = False)
    query = """
    SELECT *,
    DENSE_RANK() OVER (
    ORDER BY 
      year
  ) AS trend,
  DENSE_RANK() OVER (
    ORDER BY 
      province_en, year
  ) as fe_p_t,
  DENSE_RANK() OVER (
    ORDER BY 
      province_en, ind2
  ) as fe_p_i
  
    FROM {}.{}
    WHERE so2_intensity > 0
    """.format(db, table)
    try:
        df = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
              #.assign(
              #    trend = lambda x: pd.factorize(x["year"].astype('str')
              #                         )[0] + 1
              #)
                )
    except:
        pass
    s3.download_file(
        key = full_path_filename
    )
    shutil.move(
        filename + '.csv',
        os.path.join(path_local, filename + '.csv')
    )
    s3.remove_file(full_path_filename)
    df.head()
    
```

<!-- #region kernel="SoS" -->
Pollution abatement equipment
<!-- #endregion -->

```sos kernel="SoS"
query = """
WITH aggregate_pol AS (
  SELECT 
    year, 
    geocode4_corr, 
    ind2, 
    SUM(tlssnl) as tlssnl,
    SUM(tdwastegas_equip) as tdwastegas_equip,
    SUM(tdso2_equip) as tdso2_equip,
    SUM(tfqzlssnl) as tfqzlssnl,
    SUM(ttlssnl) as ttlssnl
  FROM 
    (
      SELECT 
        year, 
        citycode, 
        geocode4_corr, 
        --cityen,
        --china_city_sector_pollution.cityen,
        -- indus_code,
        ind2, 
        tso2, 
        tlssnl,
        tdwastegas_equip,
        tdso2_equip,
        tfqzlssnl,
        ttlssnl,
        firmdum,
        tfirm
        -- lower_location, 
        --larger_location, 
        --coastal 
      FROM 
        environment.china_city_sector_pollution 
        INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr--,
            --cityen
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr--,
            --cityen
        ) as no_dup_citycode ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
    ) 
  GROUP BY 
    year, 
    geocode4_corr, 
    ind2
) 
SELECT *
FROM aggregate_pol
"""
df_pol = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
```

<!-- #region kernel="SoS" -->
Macro variables
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT no_dup_citycode.geocode4_corr,  year, 
gdp,
population,
gdp/population as gdp_cap,
employment as employment_macro,
fixedasset as fixedasset_macro
FROM china.province_macro_data
INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr
        ) as no_dup_citycode ON province_macro_data.geocode4_corr = no_dup_citycode.extra_code
"""
df_macro = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
```

<!-- #region kernel="SoS" -->
Bank deregulation
<!-- #endregion -->

```sos kernel="SoS"
def get_company_registration_type(x):
    regex = r'有限公司|信用合作联社|有限责任公司|旧市支行|族自治州分行|县支行|市支行|村支行|市分行|农村合作银行|自治区分行|支行|合作社联合社'\
'|分行|资金互助社|信用社联合社|股份公司|住宅金融事业部|国家开发银行|总行营业部|合作金融结算服务中心|信用合作社|信托投资公司'
    matches = re.findall(regex,x)
    if len(matches) > 0:
        return matches[0]
    else:
        return np.nan 
def get_type(x):
    if re.search(r"中国工商银行|中国建设银行|中国银行|中国农业银行|交通银行|中国邮政储蓄银行", str(x)):
        return (re.search(r"中国工商银行|中国建设银行|中国银行|中国农业银行|交通银行|中国邮政储蓄银行", 
                          str(x)).group(), "SOB")
    elif re.search(r"中国农业发展银行|国家开发银行|中国进出口银行", str(x)):
        return (re.search(r"中国农业发展银行|国家开发银行|中国进出口银行", str(x)).group(),
                "policy bank")
    elif re.search(r"股份制商业银行", str(x)):
        return (re.search(r"股份制商业银行|银行股份", str(x)).group(), "joint-stock commercial bank")
    elif re.search(r"城市商业银行", str(x)):
        return (re.search(r"城市商业银行", str(x)).group(), "city commercial bank")
    elif re.search(r"农村商业银行", str(x)):
        return (re.search(r"农村商业银行", str(x)).group(), "rural commercial bank")
    elif re.search(r"外资银行", str(x)):
        return (re.search(r"外资银行", str(x)).group(), "foreign bank")
    else:
        return np.nan
```

```sos kernel="SoS"
query = """
SELECT *
FROM china.branches_raw_csv
"""
df_bank = (
    s3.run_query(
        query=query,
        database=db,
        s3_output="SQL_OUTPUT_ATHENA",
        filename="bank",  # Add filename to print dataframe
        destination_key="SQL_OUTPUT_ATHENA/CSV",  # Use it temporarily
        dtype = {'id':'str','geocode4_corr':'str', 'lostReason':'str',
                       'location':'str', 'city_temp':'str', 'points':'str'}
    )
    .assign(
        setdate=lambda x: pd.to_datetime(x["setdate"].astype("Int64").astype(str), errors ='coerce'),
        printdate=lambda x: pd.to_datetime(x["printdate"].astype("Int64").astype(str), errors ='coerce'),
        year_setdate=lambda x: x["setdate"].dt.year.astype("Int64").astype(str),
        bank_temp=lambda x: x.apply(
            lambda x: get_company_registration_type(x["fullname"]), axis=1
        ),
        geocode4_corr = lambda x: x['geocode4_corr'].astype("Int64").astype(str)
    )
     .assign(
         registration_type=lambda x: x.apply(
            lambda x: np.nan if pd.isna(x["bank_temp"]) else x["bank_temp"], axis=1
        ),
        bank_full_name=lambda x: x.apply(
            lambda x: x["fullname"]
            if pd.isna(x["bank_temp"])
            else x["fullname"].split(x["registration_type"][0])[0],
            axis=1,
        ),
        list_bank_type=lambda x: x.apply(lambda x: get_type(x["bank_full_name"]), axis=1),
        bank_type=lambda x: x.apply(
            lambda x: np.nan if pd.isna(x["list_bank_type"]) else x["list_bank_type"][0], axis=1
        ),
        bank_type_adj=lambda x: x.apply(
            lambda x: np.nan if pd.isna(x["list_bank_type"]) else x["list_bank_type"][1], axis=1
        )
    )
    .drop(columns=["bank_temp"])
)
query = """
SELECT *
FROM china.china_bank_information
"""
df_bank_info = (
    s3.run_query(
        query=query,
        database=db,
        s3_output="SQL_OUTPUT_ATHENA",
        filename="bank",  # Add filename to print dataframe
        destination_key="SQL_OUTPUT_ATHENA/CSV",  # Use it temporarily
        dtype=dtypes,
    )
    .drop_duplicates(subset=["shortbnm"])
    .replace(
        {
            "bnature": {
                1: "政策性银行",
                2: "国有控股大型商业银行",
                3: "股份制商业银行",
                4: "城市商业银行",
                5: "农村商业银行",
                6: "外资银行",
                7: "其他",
                8: "农合行",
                9: "农信社",
                10: "三类新型农村金融机构",
            }
        }
    )
)
df_bank_info.shape
```

```sos kernel="SoS"
df_bank_info.head(2)
```

```sos kernel="SoS"
df_bank.head(2)
```

```sos kernel="SoS"
df_bank['registration_type'].value_counts()
```

```sos kernel="SoS"
df_bank['bank_type'].value_counts()
```

```sos kernel="SoS"
df_bank['bank_type_adj'].value_counts()
```

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: x['setdate'] < "2022"]
    .set_index(['setdate'])
    .groupby([pd.Grouper(freq='Y')])
    .agg({"id":"nunique"})
    .assign(cumsum = lambda x: x['id'].cumsum())
    .drop(columns = ['id'])
    .plot
    .line(title = 'total branches over time',figsize=(8,8))
    .legend(loc='center left',bbox_to_anchor=(1.0, 0.5))
    
)
```

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: x['setdate'] < "2022"]
    .set_index(['setdate'])
    .groupby([pd.Grouper(freq='Y')])
    .agg({"id":"nunique"})
    #.assign(cumsum = lambda x: x['id'].cumsum())
    #.drop(columns = ['id'])
    .plot
    .line(title = 'Accumulation branches over time',figsize=(8,8))
    .legend(loc='center left',bbox_to_anchor=(1.0, 0.5))   
)
```

<!-- #region kernel="SoS" -->
list of banks in China

- Policy banks: China has three policy banks. Among them, China Development Bank was incorporated in December 2008 and officially defined by the State Council as a development finance institution in March 2015
    - 中国农业发展银行
    - 国家开发银行
    - 中国进出口银行
- State-owned Commercial Banks: China has six state-owned commercial banks. These banks are ranked by their Tier 1 capital amount as of 2018. 
    - 中国工商银行
    - 中国建设银行
    - 中国银行
    - 中国农业银行
    - 交通银行
    - 中国邮政储蓄银行
- Commercial Banks: China has 12 national commercial banks. These banks are ordered by their Tier 1 capital amount as of 2018
    - 招商银行
    - 上海浦东发展银行
    - 兴业银行
    - 中信银行
    - 中国民生银行
    - 中国光大银行
    - 平安银行
    - 华夏银行
    - 广发银行
    - 浙商银行
    - 渤海银行
    - 恒丰银行
    
Source: https://en.wikipedia.org/wiki/List_of_banks_in_China. More info in CSMAR data (where we got the data)df_bank_info
<!-- #endregion -->

```sos kernel="SoS"
df_bank.shape
```

```sos kernel="SoS"
from polyfuzz import PolyFuzz
from polyfuzz.models import RapidFuzz
from polyfuzz.models import Embeddings
from flair.embeddings import TransformerWordEmbeddings
import seaborn as sns
sns.set_style("whitegrid",rc={'figure.figsize':(11.7,8.27)})
```

<!-- #region kernel="SoS" -->
We already know the status of 75% of our dataset. The remaining banks are mostly city banks, so we well use fuzzy string matching to find them from the CSMAR dataset
<!-- #endregion -->

```sos kernel="SoS"
df_bank['bank_type_adj'].dropna().shape[0]/df_bank.shape[0]
```

```sos kernel="SoS"
banks = (
    [ i.replace('（中国）','').strip() for i in 
     df_bank.loc[lambda x: x['bank_type_adj'].isin([np.nan])]['bank_full_name'].dropna().drop_duplicates().to_list()
     if len(i) > 1]
)
bank_info = df_bank_info['shortbnm'].to_list()
#bank_info_pinyin =  [pinyin.get(i, format="strip", delimiter=" ") for i in bank_info]
#model = PolyFuzz(rapidfuzz_matcher).match(banks, bank_info)
#model.get_matches()
```

<!-- #region kernel="SoS" -->
Let's make sure the city (or at least the location) is in the name; We can use this transformers https://huggingface.co/Davlan/xlm-roberta-base-wikiann-ner?text=%E6%9C%9D%E9%98%B3%E5%B8%82%E5%8F%8C%E5%A1%94%E5%8C%BA%E5%86%9C%E6%9D%91
<!-- #endregion -->

```sos kernel="SoS"
from transformers import AutoTokenizer, AutoModelForTokenClassification
from transformers import pipeline
tokenizer = AutoTokenizer.from_pretrained("Davlan/xlm-roberta-base-wikiann-ner")

model = AutoModelForTokenClassification.from_pretrained("Davlan/xlm-roberta-base-wikiann-ner")
```

```sos kernel="SoS"
def get_proba_city(x):
    ner = pipeline("ner", tokenizer=tokenizer,model=model,aggregation_strategy='simple')
    ner_results = ner(x)
    if len([i for i in ner_results if i['entity_group'] == 'LOC']) >0:
        dic = max(
            [i for i in ner_results if i['entity_group'] == 'LOC'],
            key=lambda x:x['score']
        )
        dic['name'] = x
        dic['applicable'] = True
    else:
        dic =  max(ner_results, key=lambda x:x['score'])
        dic['name'] = x
        dic['applicable'] = False
    return dic
```

```sos kernel="SoS"
from tqdm import tqdm
```

```sos kernel="SoS"
regex_loc = '|'.join(df_bank_info['cityname'].drop_duplicates().to_list()) + \
'|'+'|'.join(df_bank_info['provincename'].drop_duplicates().to_list()) + \
'|'+ '|'.join([i.replace('市',"").replace("省",'').replace('自治区',"")
 for i in df_bank_info['provincename'].drop_duplicates().to_list()]) + \
'|'+ '|'.join([i.replace('市',"").replace("省",'').replace('自治区',"")
 for i in df_bank_info['cityname'].drop_duplicates().to_list()])
```

```sos kernel="SoS"
true_loc = [i for i in banks if re.search(regex_loc,i)]
```

```sos kernel="SoS"
df_branch_to_check = pd.DataFrame([get_proba_city(i) for i in tqdm(banks) if i not in true_loc])
```

```sos kernel="SoS"
df_branch_to_check.shape
```

```sos kernel="SoS"
(
    df_branch_to_check
    .groupby(['applicable'])
    .agg({'name':'unique'})
)
```

<!-- #region kernel="SoS" -->
we can remove the branches which are not location, which mean we need to find 1774 differents banks
<!-- #endregion -->

```sos kernel="SoS"
banks_to_find = sorted(true_loc + df_branch_to_check.loc[lambda x: x['applicable'].isin([True])]['name'].to_list())
```

```sos kernel="SoS"
banks_to_find[:10]
```

```sos kernel="SoS"
to_find = True
if to_find:
    bert = TransformerWordEmbeddings('bert-base-multilingual-cased')
    bert_matcher = Embeddings(bert, min_similarity=0)
    models = PolyFuzz(bert_matcher).match(banks_to_find, bank_info)
    models.get_matches().to_csv('branch_matches.csv', index = False)
```

```sos kernel="SoS"
#bank_info
```

```sos kernel="SoS"
from polyfuzz.models import RapidFuzz
rapidfuzz_matcher = RapidFuzz(n_jobs=1)
model = PolyFuzz(rapidfuzz_matcher).match(banks_to_find, bank_info)
```

```sos kernel="SoS"
(
    model.get_matches()
    .sort_values(by = ['Similarity'])
    .loc[lambda x: x["Similarity"] > 0.85]
)
```

```sos kernel="SoS"
df_bank_branches = (
    pd.read_csv(
        "branch_matches.csv",
        dtype={
            "id": "str",
            "geocode4_corr": "str",
            "lostReason": "str",
            "location": "str",
            "city_temp": "str",
            "points": "str",
        },
    )
    .sort_values(by=["Similarity"])
    .loc[lambda x: x["Similarity"] > 0.85]
    .merge(
        df_bank_info.reindex(columns=["bnm_cn", "shortbnm", "bnature"]).rename(
            columns={"shortbnm": "To"}
        )
    )
    .replace(
        {
            "bnature": {
                "政策性银行": "policy bank",
                "国有控股大型商业银行": "sob",
                "股份制商业银行": "joint-stock commercial bank",
                "城市商业银行": "city commercial bank",
                "农村商业银行": "rural commercial bank",
                "外资银行": "foreign bank",
                "其他": "other",
                "农合行": "other",
                "农信社": "other",
                "三类新型农村金融机构": "other",
            }
        }
    )
    .rename(columns={"bnature": "bank_type_adj"})
    .merge(
        (
            df_bank.drop_duplicates(subset=["bank_full_name"])
            .reindex(
                columns=[
                    "id",
                    "setdate",
                    "year_setdate",
                    "fullname",
                    "bank_full_name",
                    "registration_type",
                    "city",
                    "geocode4_corr",
                    "bank_type",
                    # "bank_type_adj",
                ]
            )
            .assign(
                bank_full_name_=lambda x: x["bank_full_name"]
                .replace("（中国）", "")
                .str.strip()
            )
            .rename(columns={"bank_full_name_": "From"})
        )
    )
    .reindex(
        columns=[
            "id",
            "setdate",
            "year_setdate",
            "fullname",
            "bank_full_name",
            "registration_type",
            "city",
            "geocode4_corr",
            "bank_type",
            "bank_type_adj",
        ]
    )
)

df_bank_branches = pd.concat(
    [
        (
            df_bank.loc[
                lambda x: ~x["id"].isin(df_bank_branches["id"].to_list())
            ].reindex(
                columns=[
                    "id",
                    "setdate",
                    "year_setdate",
                    "fullname",
                    "bank_full_name",
                    "registration_type",
                    "city",
                    "geocode4_corr",
                    "bank_type",
                    "bank_type_adj",
                ]
            )
        ),
        df_bank_branches,
    ]
).assign(
    bank_type_adj_clean=lambda x: x.groupby(["bank_full_name"])[
        "bank_type_adj"
    ].transform(lambda x: x.ffill().bfill())
)
```

```sos kernel="SoS"
(
    df_bank_branches
    .loc[lambda x: x["bank_type_adj_clean"].isin(['city commercial bank'])]
    ['bank_full_name']
    .value_counts()
    .reset_index()
    .sort_values(by = ['index'])
)
```

<!-- #region kernel="SoS" -->
Example of banks without confident bank type
<!-- #endregion -->

```sos kernel="SoS"
(
    pd.read_csv(
        "branch_matches.csv",
        dtype={
            "id": "str",
            "geocode4_corr": "str",
            "lostReason": "str",
            "location": "str",
            "city_temp": "str",
            "points": "str",
        },
    )
    .sort_values(by=["From"])
    .loc[lambda x: x["Similarity"] <= 0.85]
)
```

```sos kernel="SoS"
df_bank_branches['bank_type_adj_clean'].value_counts()
```

```sos kernel="SoS"
#from matplotlib import font_manager as fm, rcParams
#import matplotlib.pyplot as plt
#import seaborn as sns
#import requests
#filename = Path('simhei.zip')
#url = 'http://legionfonts.com/download/simhei'
#response = requests.get(url)
#filename.write_bytes(response.content)
#import zipfile
#with zipfile.ZipFile(filename, 'r') as zip_ref:
#    zip_ref.extractall(os.path.join(rcParams["datapath"], "fonts/ttf/"))
```

```sos kernel="SoS"
temp= (
    df_bank_branches
    .loc[lambda x: ~x["bank_type_adj_clean"].isin(["SOB", "rural commercial bank",
                                                   'policy bank', 'other',
                                                   'joint-stock commercial bank','foreign bank'])]
    .loc[lambda x: x['setdate'] < "2013"]
    #
    #.set_index(['setdate'])
    .loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])]
    .dropna(subset = ['bank_type_adj_clean'])
    .groupby(['bank_type_adj_clean', 'bank_full_name'])
    .agg({
        "year_setdate":"min",
    })
    .reset_index()
    .groupby(['year_setdate','bank_type_adj_clean'])
    .agg({'bank_full_name':'count'})
    .reset_index()
    .assign(
        year_setdate = lambda x: pd.to_datetime(x['year_setdate']),
        bank_name_adjusted = lambda x: x.groupby('bank_type_adj_clean')['bank_full_name'].transform('cumsum')
    )
    .loc[lambda x: x['year_setdate'] >= "1995"]
)

sns.lineplot(data=temp, x="year_setdate", y="bank_name_adjusted", hue="bank_type_adj_clean")
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
```

```sos kernel="SoS"
temp = (
    df_bank_branches
    .loc[lambda x: ~x["bank_type_adj_clean"].isin(["SOB", "rural commercial bank",
                                                   'policy bank', 'other',
                                                   'joint-stock commercial bank','foreign bank'])]
    .loc[lambda x: x['setdate'] < "2013"]
    .loc[lambda x: x['setdate'] >= "1995"]
    #.loc[lambda x: x['bank_type'].isin(['COMMERCIAL BANKS'])]
    .set_index(['setdate'])
    .groupby([pd.Grouper(freq='Y'), 'bank_type_adj_clean'])
    .agg({"bank_full_name":"count"})
    .assign(bank_name_adjusted = lambda x: x.groupby('bank_type_adj_clean')['bank_full_name'].transform('cumsum'))
    .reset_index()
)
sns.lineplot(data=temp, x="setdate", y="bank_name_adjusted", hue="bank_type_adj_clean")
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
```

<!-- #region kernel="SoS" -->
Compute Herfhindal index
<!-- #endregion -->

```sos kernel="SoS"
(
    df_bank_branches
    .loc[lambda x: ~x["bank_type_adj_clean"].isin([#"SOB",
        "rural commercial bank",
        'policy bank', 'other',
        'joint-stock commercial bank','foreign bank'])]
    .loc[lambda x: x['setdate'] < "2013"]
    #
    #.set_index(['setdate'])
    .loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])]
    .dropna(subset = ['bank_type_adj_clean'])
    .groupby(['year_setdate', 'geocode4_corr'])
    .agg({'id':'count'})
    .sort_values(by = ["geocode4_corr", "year_setdate"])
    #
    .assign(
        totalBranch = lambda x: 
            x.groupby(['geocode4_corr'])['id'].transform('cumsum'),
        score = lambda x: (x['id']/x['totalBranch'])**2
           )
    #.sort_values(by = ["geocode4_corr", "year_setdate"])
    #.groupby(['year_setdate','geocode4_corr'])
    #.agg({'score':'sum'})
    #.sort_values(by = ['score'])
    #.assign(hhi = lambda x: 1- x['score'])
    #.reset_index()
    #.loc[lambda x: x['year_setdate'] > "2000"]
    #.agg({'score':'describe'})
)
```

<!-- #region kernel="SoS" -->
save new data
<!-- #endregion -->

```sos kernel="SoS"
(
    df
    .merge(
    df_pol, on = ['year', 'ind2', 'geocode4_corr'], how = 'left'
    )
    .merge(
    df_macro, on = ['year', 'geocode4_corr'], how = 'left'
    )
    .assign(
        tso2_eq_output = lambda x: (x['tdso2_equip'])/(x['output']/1000),
        tso2_eq_output_1 = lambda x: (x['tdso2_equip']+1)/(x['output']/1000),
        tso2_eq_asset = lambda x: (x['tdso2_equip'])/(x['total_asset']/1000),
        tso2_eq_asset_1 = lambda x: (x['tdso2_equip']+1)/(x['total_asset']/1000),
        constraint = lambda x: x['credit_constraint'] > -0.44,
        constraint_1 = lambda x: x['credit_constraint'] > -0.26,
        target = lambda x: np.where(x['tdso2_equip'] > 0, 1,0),
        equipment_s02 = lambda x: (x['tdso2_equip']/x['tso2'])*100,
        intensity=lambda x: x["ttlssnl"]/ x["sales"]
    )
    .to_csv(os.path.join(path_local, filename + '.csv'))
)
```

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
pd.DataFrame(schema)
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
## Schema Latex table

To rename a variable, please use the following template:

```
{
    'old':'XX',
    'new':'XX_1'
    }
```

if you need to pass a latex format with `\`, you need to duplicate it for instance, `\text` becomes `\\text:

```
{
    'old':'working\_capital\_i',
    'new':'\\text{working capital}_i'
    }
```

Then add it to the key `to_rename`
<!-- #endregion -->

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
add_to_dic = True
if add_to_dic:
    if os.path.exists("schema_table.json"):
        os.remove("schema_table.json")
    data = {'to_rename':[], 'to_remove':[]}
    dic_rename =  [
        {
        'old':'periodTRUE',
        'new':'\\text{period}'
        },
        {
        'old':'period',
        'new':'\\text{period}'
        },
        
        ### depd
        ###mandate
        {
        'old':'tso2\_mandate\_c',
        'new':'\\text{S02 mandate}_c'
        },
        {
        'old':'target\_reduction\_so2\_p',
        'new':'\\text{S02 mandate}_p'
        },
        {
        'old':'target\_reduction\_co2\_p',
        'new':'\\text{COD mandate}_p'
        },
        ### financial ratio
        {
        'old':'total\_asset',
        'new':'\\text{total asset}'
        },
        {
        'old':'tangible',
        'new':'\\text{tangible asset}'
        },
        {
        'old':'investment\_tot\_asset',
        'new':'\\text{investment to asset}'
        },
        {
        'old':'rd\_tot\_asset',
        'new':'\\text{rd to asset}'
        },
        {
        'old':'asset\_tangibility\_tot\_asset',
        'new':'\\text{asset tangibility}'
        },
        {
        'old':'d\_avg\_ij\_o\_city\_mandate',
        'new':'\\text{relative reduction mandate}_c'
        },
        ### ind
        {
        'old':'current\_ratio',
        'new':'\\text{current ratio}'
        },
        {
        'old':'lag\_current\_ratio',
        'new':'\\text{current ratio}'
        },
        {
        'old':'quick\_ratio',
        'new':'\\text{quick ratio}'
        },
        {
        'old':'lag\_liabilities\_tot\_asset',
        'new':'\\text{liabilities to asset}'
        },
        {
        'old':'liabilities\_tot\_asset',
        'new':'\\text{liabilities to asset}'
        },
        {
        'old':'sales\_tot\_asset',
        'new':'\\text{sales to asset}'
        },
        {
        'old':'lag\_sales\_tot\_asset',
        'new':'\\text{sales to asset}'
        },
        {
        'old':'cash\_tot\_asset',
        'new':'\\text{cash to asset}'
        },
        {
        'old':'cashflow\_tot\_asset',
        'new':'\\text{cashflow to asset}'
        },
        {
        'old':'cashflow\_to\_tangible',
        'new':'\\text{cashflow}'
        },
        {
        'old':'lag\_cashflow\_to\_tangible',
        'new':'\\text{cashflow}'
        },
        {
        'old':'d\_credit\_constraintBELOW',
        'new':'\\text{Fin dep}_{i}'
        },
        ## control
        {
        'old':'age + 1',
        'new':'\\text{age}'
        },
        {
        'old':'export\_to\_sale',
        'new':'\\text{export to sale}'
        },
        {
        'old':'labor\_capital',
        'new':'\\text{labor to capital}'
        },
        ### Supply demand external finance
        {
        'old':'supply\_all\_credit',
        'new':'\\text{all credit}'
        },
        {
        'old':'lag\_credit\_supply\_short\_term',
        'new':'\\text{Short term loan}_{pt}'
        },
        {
        'old':'lag\_credit\_supply',
        'new':'\\text{All loan}_{pt}'
        },
        {
        'old':'lag\_credit\_supply\_long\_term',
        'new':'\\text{Long-term loan}_{pt}'
        },
        {
        'old':'fin\_dev',
        'new':'\\text{financial development}_{pt}'
        },
        {
        'old':'credit\_constraint',
        'new':'\\text{credit constraint}'
        },
        {
        'old':'soe\_vs\_priPRIVATE',
        'new':'\\text{private}'
        },
        ## TFP
        {
        'old':'tfp\_cit',
        'new':'\\text{TFP}'
        },
        ### year
        {
        'old':'year1998',
        'new':'\\text{1998}'
        },
        {
        'old':'year1999',
        'new':'\\text{1999}'
        },
        {
        'old':'year2000',
        'new':'\\text{2000}'
        },
        {
        'old':'year2001',
        'new':'\\text{2001}'
        },
        {
        'old':'year2002',
        'new':'\\text{2002}'
        },
        {
        'old':'year2003',
        'new':'\\text{2003}'
        },
        {
        'old':'year2004',
        'new':'\\text{2004}'
        },
        {
        'old':'year2005',
        'new':'\\text{2005}'
        },
        {
        'old':'year2006',
        'new':'\\text{2006}'
        },
        {
        'old':'year2007',
        'new':'\\text{2007}'
        },
        
        
    ]
    

    data['to_rename'].extend(dic_rename)
    with open('schema_table.json', 'w') as outfile:
        json.dump(data, outfile)
```

```sos kernel="SoS"
sys.path.append(os.path.join(parent_path, 'utils'))
import latex.latex_beautify as lb
#%load_ext autoreload
#%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(tidyverse)
library(lfe)
#library(lazyeval)
library('progress')
path = "../../../utils/latex/table_golatex.R"
source(path)
```

```sos kernel="R"
%get df_path
df_final <- read_csv(df_path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(
    year = relevel(as.factor(year), ref='2001'),
    period = relevel(as.factor(period), ref='FALSE'),
    polluted_d50i = relevel(as.factor(polluted_d50i), ref='BELOW'),
    polluted_d75i = relevel(as.factor(polluted_d75i), ref='BELOW'),
    polluted_d80i = relevel(as.factor(polluted_d80i), ref='BELOW'),
    polluted_d85i = relevel(as.factor(polluted_d85i), ref='BELOW'),
    polluted_d90i = relevel(as.factor(polluted_d90i), ref='BELOW'),
    polluted_d95i = relevel(as.factor(polluted_d95i), ref='BELOW'),
    polluted_mi = relevel(as.factor(polluted_mi), ref='BELOW'),
    d_avg_ij_o_city_mandate = relevel(as.factor(d_avg_ij_o_city_mandate), ref="FALSE"),
    fin_dev = 1- share_big_loan,
    lag_fin_dev = 1- lag_share_big_loan,
)
```

```sos kernel="R"
head(df_final)
```

<!-- #region kernel="R" -->
Aggregate at the province-industry-year level
<!-- #endregion -->

```sos kernel="R"
df_agg <- df_final %>%
group_by(province_en, ind2, year, period, fe_p_i , fe_t_i , fe_p_t) %>%
summarize(
    tso2 = sum(tso2),
    tcod = sum(tcod),
    twaste_water = sum(twaste_water),
    output = sum(output),
    employment = sum(employment),
    capital = sum(capital),
    target_reduction_so2_p = max(target_reduction_so2_p),
    target_reduction_co2_p = max(target_reduction_co2_p),
    lag_credit_supply = max(lag_credit_supply),
    lag_credit_supply_long_term = max(lag_credit_supply_long_term),
    fin_dev = max(fin_dev),
    lag_fin_dev = max(lag_fin_dev),
    credit_constraint = max(credit_constraint),
) %>%
ungroup()%>%
mutate(
    year = relevel(as.factor(year), ref='2005'),
    year1 = relevel(as.factor(year), ref='1998')
)

head(df_agg)
```

<!-- #region kernel="R" -->
## Variables Definition

1. credit_supply: Province-year supply all loans over GDP
2. credit_supply_long_term: Province-year supply long term loans over GDP
3. fin_dev: Share of non-4-SOCBs' share in credit
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Table 1:Financial development over time

$$
\begin{aligned}
\text{$S O 2_{c k t}=\alpha$ Financial Dependencies $_{k} \times \text{credit supply}_{pt} +\beta X_{c k t}+\mu_{c t}+\gamma_{k t}+\delta_{c k}+\epsilon_{c k t}$}
\end{aligned}
$$

$$
\begin{aligned}
\text{$S O 2_{c k t}=\alpha$ Financial Dependencies $_{k} \times \text{Fin.Dev}_{pt} + +\beta X_{c k t}+\mu_{c t}+\gamma_{k t}+\delta_{c k}+\epsilon_{c k t}$}
\end{aligned}
$$

1. Table 1: SO2 emission reduction, credit supply and financial development
  1. All loan interacted with credit constraint
  2. Long term loan with credit constraint
  3. Financial development with credit constraint

**Message**

* An increase of supply of credit or improvement of financial development (deregulation of banking sectors) is beneficial for the reduction of SO2 emission
  * Deregulation has a stronger effect on constraint sectors than non-constraint → backed by the theory that credit openness is more beneficial for constraint sectors
<!-- #endregion -->

<!-- #region kernel="R" -->
### City level
<!-- #endregion -->

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="SoS"
query = """
SELECT ind2, SUM(tso2) as sum_tso2
FROM fin_dep_pollution_baseline_industry 
WHERE year = '1998'
GROUP BY ind2
ORDER BY sum_tso2
"""
list_polluted = s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename='polluted',  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        )
list_polluted
```

```sos kernel="R"
summary(felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            #intensity + 
            log(lag_credit_supply) * credit_constraint 
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500),
            exactDOF = TRUE))
```

```sos kernel="R"
summary(felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            intensity + 
            log(lag_credit_supply) * financial_dep_us 
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500),
            exactDOF = TRUE))
```

```sos kernel="R"
%get path table
to_remove <- c(
    24, 21, 23,#, 42,18,
    33, 32, 26#, 31,17
)
## SO2
t_0 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_final%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(
               tso2 > 500 & !(ind2 %in% to_remove)),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500 & !(ind2 %in% to_remove)),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_final%>% filter(tso2 > 500 & !(ind2 %in% to_remove)),
            exactDOF = TRUE)
            
dep <- "Dependent variable: Pollution emission"
fe1 <- list(
    c("City-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"
     ),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"
     ),
    c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"
     )
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emission, credit supply and financial development",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(1). " \
"All loan is the share of total loan normalised by the GDP. " \
"Long-term loan is the share of long term loan normalised by the GDP. " \
"Financial development is defined as the share of non-4-SOCBs' share in credit) " \
"Columns 4 to 6 exclude the top and bottom 3 most polluted sectors in 1998 " \
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." \
"Heteroskedasticity-robust standard errors " \
"clustered at the city-industry level appear in parentheses."

lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

<!-- #region kernel="SoS" -->
## Table 2: heterogeneous effect: SOE vs Private

City ownership are available for the following variables:

- output
- capital
- employment
- sales

**How is it constructed**

- city ownership public vs private
    - Aggregate output by ownership and city
    - A given city will have SOE asset tangibility and PRIVATE asset tangibility [output, employment, capital and sales]
    - If asset tangibility SOE above Private then city is dominated by SOE

Notebook reference: 

https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/02_data_analysis/01_model_estimation/00_estimate_fin_ratio/03_so2_fin_ratio_sector.md#table-3-heterogeneity-effect-city-ownership-public-vs-private-domestic-vs-foreign
<!-- #endregion -->

```sos kernel="SoS"
query = """
WITH test AS (
  SELECT 
    *,
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2,
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri,
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
  
) 
SELECT year, soe, geocode4_corr, indu_2,SUM(output) as output, SUM(employ) as employ, SUM(captal) as capital
FROM (
SELECT *,
CASE WHEN ownership in ('SOE') THEN 'SOE' ELSE 'PRIVATE' END AS soe
FROM test 
  )
  GROUP BY soe, geocode4_corr, year, indu_2
"""
df = (s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename="test",  # Add filename to print dataframe
        destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        dtype = dtypes
    )
     )
```

```sos kernel="SoS"
import janitor
```

```sos kernel="SoS"
for v in ['output','employ', 'capital']:
    for t in [.5, .4, .3, .2, .1]:
        df_ = (
            df
            .set_index(['year','indu_2', 'soe', 'geocode4_corr'])
            .unstack(-2)
            .assign(
                soe_dominated = lambda x: x[(v, 'SOE')] > x[(v, 'PRIVATE')],
                share_soe = lambda x: x[(v, 'SOE')] / (x[(v, 'SOE')] + x[(v, 'PRIVATE')])
            )
            #.loc[lambda x: x['soe_dominated'].isin([True])]
            .collapse_levels("_")
            .reset_index()
            [['year','geocode4_corr', 'indu_2', "soe_dominated", 
             'share_soe'
             ]]
            .loc[lambda x: x['year'].isin(["2002"])]
            .drop(columns = ['year'])
            .rename(columns = {'indu_2':'ind2'})
            .loc[lambda x: x['share_soe']> t]
            #.groupby(['soe_dominated'])
            #.agg({'share_soe':'describe'})
            .to_csv('list_city_soe_{}_{}.csv'.format(v, t))
        )
```

```sos kernel="SoS"
query = """
WITH test AS (
  SELECT 
    *,
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2,
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri,
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
  
) 
SELECT year, foreign, geocode4_corr, indu_2,SUM(output) as output, SUM(employ) as employ, SUM(captal) as capital
FROM (
SELECT *,
CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS foreign
FROM test 
  )
  GROUP BY foreign, geocode4_corr, year, indu_2

"""
df = (s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename="test",  # Add filename to print dataframe
        destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        dtype = dtypes
    )
     )
```

```sos kernel="SoS"
for v in ['output','employ', 'capital']:
    for t in [.5, .4, .3, .2, .1]:
        (
            df
            .set_index(['year','indu_2', 'foreign', 'geocode4_corr'])
            .unstack(-2)
            .assign(
                for_dominated = lambda x: x[(v, 'FOREIGN')] > x[(v, 'DOMESTIC')],
                share_for = lambda x: x[(v, 'FOREIGN')] / (x[(v, 'FOREIGN')] + x[(v, 'DOMESTIC')])
            )
            .collapse_levels("_")
            .reset_index()
            [['year','geocode4_corr', 'indu_2', "for_dominated", 
             'share_for'
             ]]
            .loc[lambda x: x['year'].isin(["2002"])]
            .drop(columns = ['year'])
            .rename(columns = {'indu_2':'ind2'})
            .loc[lambda x: x['share_for']> t]
            #.groupby(['soe_dominated'])
            #.agg({'share_soe':'describe'})
            .to_csv('list_city_for_{}_{}.csv'.format(v, t))
        )
```

<!-- #region kernel="SoS" -->
### SO2
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 2
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
df_soe <- df_final %>% inner_join(read_csv('list_city_soe_employ_0.3.csv'))
df_priv <- df_final %>% left_join(read_csv('list_city_soe_employ_0.3.csv')) %>% filter(is.na(share_soe))
print(dim(df_soe)[1] + dim(df_priv)[1] == dim(df_final)[1])
### CITY DOMINATED
t_0 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_soe%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_priv%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_soe%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_priv%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_soe%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_priv%>% filter(tso2 > 500),
            exactDOF = TRUE)
dep <- "Dependent variable: Pollution emission"
fe1 <- list(
    c("City-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emission, credit supply and financial development, City dominated public/private",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "All loan is the share of total loan normalised by the GDP. " \
"Long-term loan is the share of long term loan normalised by the GDP. " \
"Financial development is defined as the share of non-4-SOCBs' share in credit. " \
"Cities split are based on the presence of SOEs firms. 'Above' means the total output produced by "\
"SOEs firms exceed the output of private firms. " \
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." \
"Heteroskedasticity-robust standard errors " \
"clustered at the city-industry level appear in parentheses."\
 
#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Above', 'Below','Above', 'Below', 'Above', 'Below' ]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

<!-- #region kernel="SoS" -->
## Table 3: heterogeneous effect: Domestic vs Foreign
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### SO2
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 3
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
df_for <- df_final %>% inner_join(read_csv('list_city_for_employ_0.3.csv'))
df_dom <- df_final %>% left_join(read_csv('list_city_for_employ_0.3.csv')) %>% filter(is.na(share_for))
print(dim(df_for)[1] + dim(df_dom)[1] == dim(df_final)[1])
### CITY DOMINATED
t_0 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_for%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_dom%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_for%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_dom%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_for%>% filter(tso2 > 500),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_dom%>% filter(tso2 > 500),
            exactDOF = TRUE)
dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="SO2 emission reduction, credit supply and financial development, City dominated Domestic/Foreign",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "All loan is the share of total loan normalised by the GDP. " \
"Long-term loan is the share of long term loan normalised by the GDP. " \
"Financial development is defined as the share of non-4-SOCBs' share in credit. " \
"Cities split are based on the presence of foreign's firms. 'Above' means the total output produced by "\
"foreign's firms exceed the output of domestic firms. " \
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." \
"Heteroskedasticity-robust standard errors " \
"clustered at the city-industry level appear in parentheses."\

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Above', 'Below','Above', 'Below', 'Above', 'Below' ]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

<!-- #region kernel="SoS" -->
## Table 5: TCZ & SPZ policy
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 4
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
#for ext in ['.txt', '.tex', '.pdf']:
#    x = [a for a in os.listdir(folder) if a.endswith(ext)]
#    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500 & tcz == 0),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply) * credit_constraint 
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500 & tcz == 1),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500 & tcz == 0),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(lag_credit_supply_long_term) * credit_constraint 
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr + ind2, df_final%>% filter(tso2 > 500 & tcz == 1),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint 
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_final%>% filter(tso2 > 500 & tcz == 0),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(fin_dev) * credit_constraint 
           |  fe_c_i + fe_t_i + fe_c_t|0 | geocode4_corr+ ind2, df_final%>% filter(tso2 > 500 & tcz == 1),
            exactDOF = TRUE)
dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="SO2 emission reduction, credit supply and financial development",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "All loan is the share of total loan normalised by the GDP. " \
"Long-term loan is the share of long term loan normalised by the GDP. " \
"Financial development is defined as the share of non-4-SOCBs' share in credit. " \
"The list of TCZ cities is provided by the State Council, 1998 " \
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." \
"Heteroskedasticity-robust standard errors" \
"clustered at the city-industry level appear in parentheses."

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& No TCZ', 'TCZ','No TCZ', 'TCZ', 'No TCZ', 'TCZ' ]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

<!-- #region kernel="SoS" -->
## Table 6: Environmental policy and financial development

$$
\begin{aligned}
\text{$S O 2_{p k t}=\alpha$ Financial Dependencies $_{k} \times \text{credit supply}_{pt} \times \text{policy mandate}_p +\mu_{p t}+\gamma_{k t}+\delta_{p k}+\epsilon_{p k t}$}
\end{aligned}
$$

Environmental policy and financial development:  Aggregate at the province-industry-year level
1. Evaluate the effect of credit constraint in provinces with stringent environmental policy
  1. SO2
    1. credit_constraint * target_reduction_so2_p * period
    2. lag_credit_supply*credit_constraint * target_reduction_so2_p * period
  2. ~COD~
      1. ~credit_constraint * target_reduction_co2_p * period~
      2. ~lag_credit_supply*credit_constraint * target_reduction_co2_p * period~

Message 
* Capital within financially constrained industries have been relocated toward investment less harming for the environment (i.e. lower emission) in province with stringent environmental policy
* One channel is the increase of the credit access → more loan availability in constraint industry
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 5
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
#for ext in ['.txt', '.tex', '.pdf']:
#    x = [a for a in os.listdir(folder) if a.endswith(ext)]
#    [os.remove(os.path.join(folder, i)) for i in x]
```

<!-- #region kernel="SoS" -->
### Province level
<!-- #endregion -->

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~  
            credit_constraint * target_reduction_so2_p * period
           |  fe_p_i + fe_t_i + fe_p_t|0 | province_en +ind2, df_agg%>% 
             filter( target_reduction_so2_p > 0),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~  
            log(lag_credit_supply)*credit_constraint * target_reduction_so2_p * period
           |  fe_p_i + fe_t_i + fe_p_t|0 | province_en +ind2, df_agg%>% 
             filter( target_reduction_so2_p > 0),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~  
            log(lag_credit_supply_long_term)*credit_constraint * target_reduction_so2_p * period
           |  fe_p_i + fe_t_i + fe_p_t|0 | province_en +ind2, df_agg%>% 
             filter( target_reduction_so2_p > 0),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~  
            credit_constraint * target_reduction_so2_p * period
           |  fe_p_i + fe_t_i + fe_p_t|0 | province_en +ind2, df_agg%>% 
             filter( target_reduction_so2_p > 0 & !(ind2 %in% to_remove)),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~  
            log(lag_credit_supply)*credit_constraint * target_reduction_so2_p * period
           |  fe_p_i + fe_t_i + fe_p_t|0 | province_en +ind2, df_agg%>% 
             filter( target_reduction_so2_p > 0 & !(ind2 %in% to_remove)),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~  
            log(lag_credit_supply_long_term)*credit_constraint * target_reduction_so2_p * period
           |  fe_p_i + fe_t_i + fe_p_t|0 | province_en +ind2, df_agg%>% 
             filter( target_reduction_so2_p > 0 & !(ind2 %in% to_remove)),
            exactDOF = TRUE)

dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("Province-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Province-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2#, t_3, t_4, t_5
),
    title="SO2 emission reduction, credit constraint and policy mandate",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(2). " \
" SO2 city mandate measures the total amount of SO2 a city needs to reduce by the end of the 11th FYP. " \
"All loan is the share of total loan normalised by the GDP. " \
"Long-term loan is the share of long term loan normalised by the GDP. " \
"Financial development is defined as the share of non-4-SOCBs' share in credit. " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

#multicolumn ={
#    'Eligible': 2,
#    'Non-Eligible': 1,
#    'All': 1,
#    'All benchmark': 1,
#}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']

reorder = {
    6:0,
    3:5
    #0:2,
    #1:3
    #9:2,
    #0:3,
    #6:5
    #14:5
}

lb.beautify(table_number = table_nb,
            reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

<!-- #region kernel="SoS" -->
## Table 7: parallel trend
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 6
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
#for ext in ['.txt', '.tex', '.pdf']:
#    x = [a for a in os.listdir(folder) if a.endswith(ext)]
#    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~  
           credit_constraint * target_reduction_so2_p * year
           |  fe_p_i + fe_t_i + fe_p_t|0 | province_en +ind2, df_agg%>% 
             filter( target_reduction_so2_p > 0),
            exactDOF = TRUE)


dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("Province-industry", "Yes"),
    c("Province-industry", "Yes"),
    c("Province-Time", "Yes")
             )

table_1 <- go_latex(list(
    t_0
),
    title="parallel trend",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "The year 2005 is the omitted category. "\
" SO2 city mandate measures the total amount of SO2 a city needs to reduce by the end of the 11th FYP. " \
"All loan is the share of total loan normalised by the GDP. " \
"Long-term loan is the share of long term loan normalised by the GDP. " \
"Financial development is defined as the share of non-4-SOCBs' share in credit. " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder,
            parallel = True)
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
# Generate reports
<!-- #endregion -->

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
import sys
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
import create_report
```

```sos kernel="python3"
name_json = 'parameters_ETL_pollution_credit_constraint.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
notebookname = "00_credit_supply.ipynb"
```

```sos kernel="python3"
create_report.create_report(extension = "html", keep_code = False, notebookname =  notebookname)
```

```sos kernel="python3"
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          #os.path.join(str(Path(path).parent), "00_download_data_from"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "00_statistical_exploration"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "01_model_estimation"),
         ]:
    try:
        os.remove(os.path.join(p, 'README.md'))
    except:
        pass
    path_parameter = os.path.join(parent_path,'utils', name_json)
    md_lines =  make_toc.create_index(cwd = p, path_parameter = path_parameter)
    md_out_fn = os.path.join(p,'README.md')
    
    if p == parent_path:
    
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = True, path_parameter = path_parameter)
    else:
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = False)
```
