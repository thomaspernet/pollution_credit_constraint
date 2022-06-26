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
from matplotlib import cm
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os, shutil, json
import sys
import re
import janitor
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
filename = "data"
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

<!-- #region kernel="SoS" -->
#### Pollution abatement equipment
<!-- #endregion -->

```sos kernel="SoS"
query = """
WITH aggregate_pol AS (
  SELECT 
    year, 
    province_en,
    geocode4_corr, 
    --indus_code as cic,
    ind2, 
    --SUM(ttoutput) as output,
    SUM(tso2) as tso2, 
    SUM(twaste_water) as twaste_water, 
    SUM(tcod) as tcod, 
    SUM(tlssnl) as tlssnl,
    SUM(tdwastegas_equip) as tdwastegas_equip,
    SUM(tdso2_equip) as tdso2_equip,
    SUM(dwastewater_equip) as dwastewater_equip, 
    SUM(total_industrialwater_used) as total_industrialwater_used, 
    SUM(total_freshwater_used) as total_freshwater_used, 
    SUM(total_repeatedwater_used) as total_repeatedwater_used, 
    SUM(total_coal_used) as total_coal_used, 
    SUM(clean_gas_used) as clean_gas_used,
    SUM(trlmxf) as trlmxf,
    SUM(tylmxf) as tylmxf,
    SUM(tfqzlssnl) as tfqzlssnl,
    SUM(ttlssnl) as ttlssnl,
    AVG(tdwastegas_equip_output) as tdwastegas_equip_output,
    AVG(tdso2_equip_output) as tdso2_equip_output
  FROM 
    (
      SELECT 
        year, 
        citycode, 
        province_en,
        geocode4_corr, 
        ind2, 
        --indus_code,
        --ttoutput,
        tso2, 
        twaste_water, 
        tcod, 
        tlssnl,
        tdwastegas_equip,
        tdso2_equip,
        dwastewater_equip, 
        total_industrialwater_used, 
        total_freshwater_used, 
        total_repeatedwater_used, 
        total_coal_used, 
        clean_gas_used,
        trlmxf,
        tylmxf,
        tfqzlssnl,
        ttlssnl,
        tdwastegas_equip_output,
        tdso2_equip_output
      FROM 
        environment.china_city_sector_pollution 
        INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr,
            province_en
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr,
            province_en
        ) as no_dup_citycode ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
    ) 
  GROUP BY 
    year, 
    province_en,
    geocode4_corr, 
    ind2
    --indus_code
) 
SELECT *
FROM aggregate_pol
"""
df_pol = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename='pollution',  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
df_pol.head()
```

```sos kernel="SoS"
df_pol.shape
```

<!-- #region kernel="SoS" -->
#### Macro variables
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT no_dup_citycode.geocode4_corr,  year, 
gdp,
population,
gdp/population as gdp_cap,
employment as employment_macro,
fixedasset as fixedasset_macro,
fdi
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
df_macro.head()
```

<!-- #region kernel="SoS" -->
#### Credit supply
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT 
      province_loan_and_credit.year, 
      province_loan_and_credit.province_en, 
      CAST(
        total_long_term_loan AS DECIMAL(16, 5)
      )/ CAST(
        total_gdp AS DECIMAL(16, 5)
      ) AS credit_supply_long_term, 
      CAST(
        total_short_term AS DECIMAL(16, 5)
      )/ CAST(
        total_gdp AS DECIMAL(16, 5)
      ) AS credit_supply_short_term 
    FROM 
      almanac_bank_china.province_loan_and_credit
"""
df_credit_supply = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
df_credit_supply.head()
```

<!-- #region kernel="SoS" -->
#### Asif
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT 
      year, 
      geocode4_corr, 
      --cic,
      indu_2,
      SUM(output) AS output,
      SUM(employ) AS employment, 
      SUM(captal) AS capital,
      SUM(sales) as sales,
      AVG(c125) as interest_rate
FROM(
SELECT 
  firm, 
  year, 
  geocode4_corr, 
  --cic,
 CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
    '0', 
    substr(cic, 1, 1)
  ) END AS indu_2, 
  output, 
  employ, 
  captal, 
  sales, 
  toasset,
  c125
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
  WHERE 
      year in (
      '1998', '1999', '2000',
        '2001', '2002', '2003', '2004', '2005', 
        '2006', '2007'
      ) 
    GROUP BY 
    year, 
      geocode4_corr, 
      --cic
      indu_2
"""
df_asif = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
df_asif.head()
```

<!-- #region kernel="SoS" -->
#### Asset tangibility 
<!-- #endregion -->

```sos kernel="SoS"
query= """
WITH test AS (
  SELECT 
    *, 
    cic,
     CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    c80 + c81 + c82 + c79 as current_asset, 
    c91 + c92 AS intangible, 
    tofixed - cudepre  AS tangible, 
    tofixed - cudepre + (c91 + c92) AS net_non_current, 
    (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) AS error, 
    c95 + c97 as total_liabilities, 
    CASE WHEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) > 0 THEN (c95 + c97 + c99) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (c95 + c97 + c99) END AS total_right, 
    CASE WHEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) < 0 THEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) END AS total_asset, 
    (c131 - c134) + cudepre as cashflow 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr, 
        province_en 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        province_en, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code 
  WHERE 
    c95 > 0 -- current liabilities
    AND c97 > 0 -- long term liabilities
    AND c98 > 0 -- total liabilities
    AND c99 > 0 -- equity
    AND c80 + c81 + c82 + c79 > 0 
    AND tofixed > 0 
    AND output > 0 
    and employ > 0
) 
SELECT 
  * 
FROM 
  (
    WITH ratio AS (
      SELECT 
        year, 
        -- cic, 
        indu_2, 
        geocode4_corr, 
        province_en, 
        CAST(
          SUM(output) AS DECIMAL(16, 5)
        ) AS output, 
        CAST(
          SUM(sales) AS DECIMAL(16, 5)
        ) AS sales, 
        CAST(
          SUM(employ) AS DECIMAL(16, 5)
        ) AS employment, 
        CAST(
          SUM(captal) AS DECIMAL(16, 5)
        ) AS capital, 
        SUM(current_asset) AS current_asset, 
        SUM(tofixed) AS tofixed, 
        SUM(error) AS error, 
        SUM(total_liabilities) AS total_liabilities, 
        SUM(total_asset) AS total_asset, 
        SUM(total_right) AS total_right, 
        SUM(intangible) AS intangible, 
        SUM(tangible) AS tangible, 
        CAST(
          SUM(c84) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS investment_tot_asset, 
        CAST(
          SUM(rdfee) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS rd_tot_asset, 
        CAST(
          SUM(tangible) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) asset_tangibility_tot_asset

      FROM 
        test 
      WHERE 
        year in (
          '1997','1998','1999','2000','2001', '2002', '2003', '2004', '2005', 
          '2006', '2007'
        ) 
        AND total_asset > 0 
        AND tangible > 0 
      GROUP BY 
        province_en, 
        geocode4_corr, 
        --cic,
        indu_2, 
        year 
      
    ) 
    SELECT 
      year, 
      --cic,
      indu_2 as ind2, 
      geocode4_corr, 
      tangible, 
      total_asset,
      investment_tot_asset, 
      rd_tot_asset, 
      asset_tangibility_tot_asset
    FROM 
      ratio
  )
"""
df_output = s3.run_query(
                    query=query,
                    database="firm_survey",
                    s3_output='SQL_OUTPUT_ATHENA/CSV',
    filename = 'example_1'
                )
df_output.head()
```

<!-- #region kernel="SoS" -->
#### TCZ/SPZ
<!-- #endregion -->

```sos kernel="SoS"
query ="""
SELECT no_dup_citycode.geocode4_corr, tcz, spz FROM "policy"."china_city_tcz_spz"
INNER JOIN (
    SELECT 
      extra_code, 
      geocode4_corr 
    FROM 
      chinese_lookup.china_city_code_normalised 
    GROUP BY 
      extra_code, 
      geocode4_corr
  ) as no_dup_citycode ON china_city_tcz_spz.geocode4_corr = no_dup_citycode.extra_code
"""
df_tcz = (
    s3.run_query(
                    query=query,
                    database="policy",
                    s3_output='SQL_OUTPUT_ATHENA/CSV',
    filename = 'example_1'
                )
    .assign(spz = lambda x: x['spz'].fillna(0).astype('Int64'))
)
df_tcz.head()
```

```sos kernel="SoS"
df_tcz.dtypes
```

<!-- #region kernel="SoS" -->
#### Credit constraint
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT 
      cic as ind2,
      financial_dep_china AS credit_constraint--,
      --financial_dep_us,
      --liquidity_need_us,
      --rd_intensity_us 
    FROM 
      industry.china_credit_constraint
"""
df_credit_constraint = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
df_credit_constraint.head()
```

<!-- #region kernel="SoS" -->
#### Innovation
<!-- #endregion -->

```sos kernel="SoS"
query = """
WITH temp AS (
SELECT year, no_dup_citycode.geocode4_corr,-- indus_code as cic, 
substr(indus_code, 1,2) as ind2,
innovation_index
FROM innovation_city_industry
INNER JOIN (
    SELECT 
      extra_code, 
      geocode4_corr 
    FROM 
      chinese_lookup.china_city_code_normalised 
    GROUP BY 
      extra_code, 
      geocode4_corr
  ) as no_dup_citycode ON innovation_city_industry.geocode4_corr = no_dup_citycode.extra_code
  )
  SELECT year, geocode4_corr, 
  ind2,
  --cic,
  approx_percentile(innovation_index, ARRAY[0.50])[1] as innovation_index
  -- SUM(innovation_index) as innovation_index
  FROM temp
  GROUP BY year, geocode4_corr,
  --cic
  ind2
"""
df_innovation = (s3.run_query(
            query=query,
            database="china",
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
df_innovation.head()
```

```sos kernel="SoS"
df_innovation['innovation_index'].describe()
```

<!-- #region kernel="SoS" -->
#### Bank deregulation

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
    #elif re.search(r"城市商业银行", str(x)):
    #    return (re.search(r"城市商业银行", str(x)).group(), "city commercial bank")
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
    .assign(
        bank_type_1 = lambda x: x['certcode'].str.slice(stop = 1),
        bank_code = lambda x: x['certcode'].str.slice(stop = 7),
        bank_type_details = lambda x: x['certcode'].str.slice(start = 5, stop = 6),
        citycode = lambda x: x['certcode'].str.slice(start = 7, stop = 11),
        unknown_code = lambda x: x['certcode'].str.slice(start = 11),
    )
    .drop(columns=["bank_temp"])
)
```

```sos kernel="SoS"
df_bank.loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])].shape[0]/df_bank.shape[0]
```

```sos kernel="SoS"
df_bank.shape
```

```sos kernel="SoS"
import seaborn as sns
sns.set_style("whitegrid",rc={'figure.figsize':(15,10)})
```

<!-- #region kernel="SoS" -->
We already know the status of 75% of our dataset.
<!-- #endregion -->

```sos kernel="SoS"
df_bank['bank_type_adj'].dropna().shape[0]/df_bank.shape[0]
```

<!-- #region kernel="SoS" -->
It seems we can derive some information from the certcode:

- bank type: first digit
- bank code: first 5 digits
- bank type details: 5 and 6th digits
- city code: 7 to 11th digits
- unknown: last digits starting at the 11th position
<!-- #endregion -->

<!-- #region kernel="SoS" -->
It seems "S" is the branch

Below is an example of knowk bank type
<!-- #endregion -->

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: ~x['bank_type_adj'].isin([np.nan])]
    .loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])]
    .loc[lambda x: x['bank_type_1'].isin(['B'])]#['bank_type'].value_counts()
    .reindex(columns = ['id', 'flowno', 'certcode', 'fullname','citycode','geocode4_corr',
                       'bank_type_adj', 'bank_type_1','bank_type', 'bank_code', 'bank_type_details','test2','year_setdate',
       'registration_type', 'bank_full_name', 'list_bank_type'])
    [['bank_type_details','bank_type_adj']].value_counts()
)
```

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: ~x['bank_type_adj'].isin([np.nan])]
    .loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])]
    .loc[lambda x: x['bank_type_1'].isin(['B'])]#['bank_type'].value_counts()
    .reindex(columns = ['id', 'flowno', 'certcode', 'fullname','citycode','geocode4_corr',
                       'bank_type_adj', 'bank_type_1','bank_type', 'bank_code', 'bank_type_details','test2','year_setdate',
       'registration_type', 'bank_full_name', 'list_bank_type'])
    .loc[lambda x: x['bank_type_details'].isin(['S', 'A', 'U'])]
    .groupby(['bank_type_details'])
    .agg({'fullname':'unique'})
)
```

<!-- #region kernel="SoS" -->
Below is an example of unknown bank type(
    df_bank
    .loc[lambda x: x['bank_type_adj'].isin([np.nan])]
    .loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])]
    .loc[lambda x: x['bank_type_1'].isin(['B'])]
<!-- #endregion -->

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: x['bank_type_adj'].isin([np.nan])]
    .loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])]
    .loc[lambda x: x['bank_type_1'].isin(['B'])]
    ['bank_type_details'].value_counts()
)
```

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: x['bank_type_adj'].isin([np.nan])]
    .loc[lambda x: ~x['geocode4_corr'].isin(["<NA>"])]
    .loc[lambda x: x['bank_type_1'].isin(['B'])]#['bank_type'].value_counts()
    .reindex(columns = ['id', 'flowno', 'certcode', 'fullname','citycode','geocode4_corr',
                       'bank_type_adj', 'bank_type_1','bank_type', 'bank_code', 'bank_type_details','test2','year_setdate',
       'registration_type', 'bank_full_name', 'list_bank_type'])
    .loc[lambda x: x['bank_type_details'].isin(['S', 'B','L', 'U'])]
    .groupby(['bank_type_details'])
    .agg({'fullname':'unique'})
)
```

<!-- #region kernel="SoS" -->
Rule:

- If `bank_type_adj` is known, then keep "S"
- If `bank_type_adj` is unknown, then keep "S" and assume it is a city commercial bank & bank type equals to B

For instance, we [know](http://www.asianbanks.net/HTML/Links/china-city-commercial-rural%20banks.htm) that 长沙银行 is a city commercial bank. 

If we check in the database, we can see the bank has different types (S, X, B, L, H, G). The difference is subtile
<!-- #endregion -->

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: x['bank_type_adj'].isin([np.nan])]
    .loc[lambda x: x['fullname'].str.contains('长沙银行')]
    .reindex(columns = ['id', 'flowno', 'certcode', 'fullname','citycode','geocode4_corr',
                       'bank_type_adj', 'bank_type_1','bank_type', 'bank_code', 'bank_type_details','test2','year_setdate',
       'registration_type', 'bank_full_name', 'list_bank_type'])
    [['bank_type_1','bank_type_details']].value_counts()
)
```

```sos kernel="SoS"
(
    df_bank
    .loc[lambda x: x['bank_type_adj'].isin([np.nan])]
    .loc[lambda x: x['fullname'].str.contains('长沙银行')]
    .reindex(columns = ['id', 'flowno', 'certcode', 'fullname','citycode','geocode4_corr',
                       'bank_type_adj', 'bank_type_1','bank_type', 'bank_code', 'bank_type_details','test2','year_setdate',
       'registration_type', 'bank_full_name', 'list_bank_type'])
    .loc[lambda x: x['bank_type_details'].isin(["S",'X', 'B', 'L', 'H', 'G'])]
    .drop_duplicates(subset = ['bank_type_details'])
)
```

<!-- #region kernel="SoS" -->
Let's reconstruct the data:

Use city code instead of geocode
<!-- #endregion -->

```sos kernel="SoS"
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
df_bank_info.loc[lambda x: x['registeraddress'].str.contains('北京')].loc[lambda x: x['bnature'].isin(['城市商业银行'])]
```

```sos kernel="SoS"
#### no city bank
df_bank_concat = (
    pd.concat(
    [
        (
            df_bank.loc[lambda x: ~x["bank_type_adj"].isin([np.nan])]
            .loc[lambda x: x["bank_type_details"].isin(["S"])]
            .reindex(
                columns=[
                    "id",
                    "certcode",
                    "bank_type_adj",
                    "bank_type_1",
                    "bank_type",
                    "bank_type_details",
                    "unknown_code",
                    "fullname",
                    "registration_type",
                    "bank_full_name",
                    "citycode",
                    "bank_code",
                    "year_setdate",
                ]
            )
            .assign(status="no CCB",)
        ),
        ### rural
        (
            df_bank.loc[lambda x: x["bank_type_adj"].isin([np.nan])]
            .loc[lambda x: x["bank_type_details"].isin(["S"])]
            .loc[lambda x: x['fullname'].str.contains('村镇')]
            .assign(bank_type="农村商业银行", status="no CCB")
            .reindex(
                columns=[
                    "id",
                    "certcode",
                    "bank_type_adj",
                    "bank_type_1",
                    "bank_type",
                    "bank_type_details",
                    "unknown_code",
                    "fullname",
                    "registration_type",
                    "bank_full_name",
                    "citycode",
                    "geocode4_corr",
                    "bank_code",
                    "year_setdate",
                    "status",
                ]
            )
        )
    ]
)
    .rename(
    columns = {
        #'citycode':'geocode4_corr',
        'year_setdate':'year'
    })
)
```

<!-- #region kernel="SoS" -->
For the CCB, we need to process them independently using the CSMAR dataset. The dataset inform us of the city banks -> about 173 banks. 
<!-- #endregion -->

```sos kernel="SoS"
df_bank_info.loc[lambda x: x['bnature'].isin(['城市商业银行'])].head()
```

```sos kernel="SoS"
### CCB
temp_city_branch = (
    df_bank.loc[lambda x: x["bank_type_adj"].isin([np.nan])]
    .loc[lambda x: x["bank_type_details"].isin(["S"])]
    .loc[lambda x: x["bank_type_1"].isin(["B", "L"])]
    .loc[lambda x: ~x["fullname"].str.contains("村镇")]
    .assign(bank_type="城市商业银行", status="CCB")
    .reindex(
        columns=[
            "id",
            "certcode",
            "bank_type_adj",
            "bank_type_1",
            "bank_type",
            "bank_type_details",
            "unknown_code",
            "fullname",
            "registration_type",
            "bank_full_name",
            "citycode",
            'geocode4_corr',
            "bank_code",
            "year_setdate",
            "status",
        ]
    )
    .rename(columns={
        #"citycode": "geocode4_corr",
        "year_setdate": "year"})
)
```

```sos kernel="SoS"
df_bank_info['shortbnm'].nunique()
```

<!-- #region kernel="SoS" -->
We can adjust the bank name using the CSMAR datasetfrom polyfuzz.models import RapidFuzz
rapidfuzz_matcher = RapidFuzz(n_jobs=1)
<!-- #endregion -->

```sos kernel="SoS"
from polyfuzz.models import RapidFuzz
from polyfuzz import PolyFuzz
rapidfuzz_matcher = RapidFuzz(n_jobs=1)
```

<!-- #region kernel="SoS" -->
Clean no CCB
<!-- #endregion -->

```sos kernel="SoS"
banks_no_ccb = (
    [ i.replace('（中国）','').strip() for i in 
     (
    df_bank_concat['bank_full_name'].dropna().drop_duplicates().to_list()
)
     if len(i) > 1]
)
bank_info = df_bank_info.loc[lambda x: ~x['bnature'].isin(['城市商业银行'])]['shortbnm'].to_list()
no_ccb = PolyFuzz(rapidfuzz_matcher).match(banks_no_ccb, bank_info)
```

```sos kernel="SoS"
len(no_ccb.get_matches().sort_values(by = ['Similarity']).loc[lambda x: x["Similarity"] > 0.80]['From'].to_list())
```

<!-- #region kernel="SoS" -->
CCB
<!-- #endregion -->

```sos kernel="SoS"
banks = (
    [ i.replace('（中国）','').strip() for i in 
     (
    temp_city_branch['bank_full_name'].dropna().drop_duplicates().to_list()
)
     if len(i) > 1]
)
bank_info = df_bank_info.loc[lambda x: x['bnature'].isin(['城市商业银行'])]['shortbnm'].to_list()
model = PolyFuzz(rapidfuzz_matcher).match(banks, bank_info)
```

<!-- #region kernel="SoS" -->
Among the 183 banks, we found 113 with a strong similarity
<!-- #endregion -->

```sos kernel="SoS"
len(model.get_matches().sort_values(by = ['Similarity']).loc[lambda x: x["Similarity"] > 0.80]['From'].to_list())
```

<!-- #region kernel="SoS" -->
we can concatenate the results
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT 
        extra_code, 
        geocode4_corr, 
        province_en 
      FROM 
        chinese_lookup.china_city_code_normalised
"""
df_citycode = (s3.run_query(
            query=query,
            database="china",
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
df_citycode.head()
```

```sos kernel="SoS"
df_bank_status = (
    pd.concat(
        [
            (
                df_bank_concat.merge(
                    no_ccb.get_matches()
                    .sort_values(by=["Similarity"])
                    .loc[lambda x: x["Similarity"] > 0.80]
                    .rename(columns={"From": "bank_full_name"})
                )
            ),
            (
                temp_city_branch.merge(
                    model.get_matches()
                    .sort_values(by=["Similarity"])
                    .loc[lambda x: x["Similarity"] > 0.80]
                    .rename(columns={"From": "bank_full_name"})
                )
            ),
        ]
    )
    .rename(columns={"To": "bank_name"})
    .assign(
        geocode4_corr=lambda x: np.where(
            x["geocode4_corr"].isin(["<NA>", np.nan]), x["citycode"], x["geocode4_corr"]
        )
    )
    .assign(
        bank_type_adj=lambda x: np.where(
            np.logical_and(
                x["bank_type_adj"].isin([np.nan]), x["bank_type"] == "农村商业银行"
            ),
            "rural commercial bank",
            x["bank_type_adj"],
        )
    )
    .assign(
        bank_type_adj=lambda x: np.where(
            np.logical_and(
                x["bank_type_adj"].isin([np.nan]), x["bank_type"] == "城市商业银行"
            ),
            "city commercial bank",
            x["bank_type_adj"],
        )
    )
    .merge(
        df_citycode.drop_duplicates()
        .assign(extra_code=lambda x: x["extra_code"].astype(str))
        .rename(columns={"geocode4_corr": "geocode4_corr_adj"}),
        right_on=["extra_code"],
        left_on=["geocode4_corr"],
    )
    .drop(columns = ['geocode4_corr','extra_code'])
    .rename(columns={"geocode4_corr_adj": "geocode4_corr"})
)
df_bank_status.head()
```

```sos kernel="SoS"
#df_bank_concat.loc[lambda x: x['bank_code'].isin(["A0002S2", "A0002S3"])]
```

<!-- #region kernel="SoS" -->
We can know get the following information:

- Number of registration per year
- Number of total branches
<!-- #endregion -->

```sos kernel="SoS"
(
    df_bank_status
    .loc[lambda x: x['year'] < "2022"]
    .groupby(['bank_code'])
    .agg({'bank_name':['nunique','unique', 'count']})
    .sort_values(by = [('bank_name','count')])
)
```

```sos kernel="SoS"
x1 = (
    df_bank_status
    .loc[lambda x: x['status'].isin(['CCB'])]
    .pivot_table(
            values="id",
            index=["bank_name"],
            columns="year",
            aggfunc='nunique',
            fill_value=0,
        )
    .stack()
    .reset_index()
    .rename(columns={0: "count"})
    #.loc[lambda x :x['bank_name'].isin(['上海银行'])]
    .assign(
            count=lambda x: x.groupby(["bank_name"])[
                "count"
            ].transform("cumsum"),
            active = lambda x: np.where(x['count']> 0,1, 0)
        )
    .groupby(['year'])
    .agg({'active':'sum'})
    #.plot
    #.line(title = 'Accumulation CCB over time',figsize=(10,6))
    #.legend(loc='center left',bbox_to_anchor=(1.0, 0.5)) 
)
x2 = (
    df_bank_status
    .loc[lambda x: x['status'].isin(['CCB'])]
    .pivot_table(
            values="id",
            index=["bank_name"],
            columns="year",
            aggfunc='nunique',
            fill_value=0,
        )
    .stack()
    .reset_index()
    .rename(columns={0: "count"})
    #.loc[lambda x :x['bank_name'].isin(['上海银行'])]
    .assign(
            count=lambda x: x.groupby(["bank_name"])[
                "count"
            ].transform("cumsum"),
            active = lambda x: np.where(x['count']> 0,1, 0)
        )
    .groupby(['year'])
    .agg({'count':'sum'})
)
```

```sos kernel="SoS"
ax1 = plt.subplot()
l1, = ax1.plot(x1, color='red')
ax1.set_xticklabels(x1.index, rotation=45)
ax2 = ax1.twinx()
l2, = ax2.plot(x2, color='orange')

plt.legend([l1, l2], ["Nb of CCB", "Nb of branches"],loc='center left',bbox_to_anchor=(1.1, .9))
plt.show()
```

<!-- #region kernel="SoS" -->
Let's see the data is 3D:

- year
- province
- number of branches
<!-- #endregion -->

```sos kernel="SoS"
#df_bank_status.loc[lambda x: x['bank_name'].str.contains('上海')]
```

```sos kernel="SoS"
test = (
    df_bank_status
    .loc[lambda x: x['status'].isin(['CCB'])]
    .pivot_table(
            values="id",
            index=["bank_name", 'province_en','geocode4_corr'],
            columns="year",
            aggfunc='nunique',
            fill_value=0,
        )
    .stack()
    .reset_index()
    .rename(columns={0: "count"})
    .assign(
            count=lambda x: x.groupby(["bank_name", 'province_en','geocode4_corr'])[
                "count"
            ].transform("cumsum"),
            active = lambda x: np.where(x['count']> 0,1, 0)
        )
    .groupby(['province_en', 'year'])
    .agg({'active':'sum'}) 
    .reset_index()
    .assign(
    mx_ = lambda x: x.groupby(['province_en'])['active'].transform('max')
    )
    .sort_values(by = ['mx_','province_en', 'year'], ascending= True)
    .drop(columns ='mx_')
)
test.head()
```

```sos kernel="SoS"
x = np.array([np.repeat(i, test['year'].nunique()) for i,j in enumerate(test['province_en'].unique())],dtype = 'float')
y = np.array([np.repeat(i, test['province_en'].nunique()) for i,j in enumerate(test['year'].unique())],dtype = 'float').T
z = (
    test
    .set_index(['province_en', 'year'])
    .unstack()
    .sort_values(by = [('active','2022')], ascending = False)
    .to_numpy()
    .astype(float)
)
y_tick = list(
    test
    .set_index(['province_en', 'year'])
    .unstack()
    .sort_values(by = [('active','2022')], ascending = False)
    .index
)
x_tick = list(
    test
    .set_index(['province_en', 'year'])
    .unstack()
    .sort_values(by = [('active','2022')], ascending = False)
    .droplevel(axis =1, level=0)
    .columns
)
```

```sos kernel="SoS"
fig = plt.figure(figsize=(20,10))

# add axes
ax = fig.add_subplot(111,projection='3d')
# plot the plane
ax.plot_surface(y,x,z,cmap=cm.coolwarm)
ax.view_init(30, 150)
ax.set_xticks(ticks = np.arange(0, len(x_tick)))
ax.set_xticklabels(x_tick,rotation=70, fontsize = 7)
ax.set_yticks(ticks = np.arange(0, len(y_tick)))
ax.set_yticklabels(y_tick,rotation=90, fontsize = 7)
plt.title('Number of CCB per year and province in China')
plt.show()
```

<!-- #region kernel="SoS" -->
Largest CCB
<!-- #endregion -->

```sos kernel="SoS"
(
    df_bank_status.loc[lambda x: x['status'].isin(['CCB'])]['bank_name'].value_counts()
)
```

<!-- #region kernel="SoS" -->
Compute Herfhindal index:

$$
H H I_{c, t}=\sum_{k=1}^{N}\left(\frac{\text { Number of } \text { Branches }_{k t}}{\sum_{k=1}^{N} \text { Number of } \text { Branches }_{k t }}\right)^{2} .
$$

represents the HHI of the local banking market in city $c$ and
is calculated on the basis of the number of bank branches of all banks k locally present for each year (with
N the total number of banks in the country).

We need to make a panel for each bank because the data only shows when a new branch is added, but in the case where there is no new branch, the bank has still a market share.

See example below, the bank "上海银行股份" is in business since 2009, and created 2 other branches in 2011. In 2010, the bank was still opearting the branch, so we need to create this new row

The bank regulation index:

$$
\operatorname{Bank}_{\mathrm{HHI}_{c, t}}=1-\mathrm{HHI}_{c, t}
$$

It ranges from zero to one with zero indicating the most stringent regulation and one indicating the most deregulation.
<!-- #endregion -->

<!-- #region kernel="SoS" -->
download exiting branches
<!-- #endregion -->

```sos kernel="SoS"
#import warnings
#import boto3
#import io
#warnings.filterwarnings("ignore")
#client_ = boto3.resource('s3')
#PATH_S3= "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES"
#list_s3 = s3.list_all_files_with_prefix(prefix=os.path.join(PATH_S3, 'RAW_JSON_EXIT'))
#def convert_to_df(key):
#    obj = client_.Object(bucket, key)
#    body = obj.get()['Body'].read()
#    fix_bytes_value = body.replace(b"'", b'"')
#    my_json = json.load(io.BytesIO(fix_bytes_value))
#    return pd.DataFrame(my_json)

#df_exit = pd.concat(map(convert_to_df, list_s3))
```

<!-- #region kernel="SoS" -->
Concentration ratio

$$
\mathrm{CR}_{\text {bigfour }_{c, t}}=\left(\operatorname{Branch}_{c, t}^{\mathrm{BOC}}+\operatorname{Branch}_{c, t}^{\mathrm{ICBC}}+\operatorname{Branch}_{c, t}^{\mathrm{CCB}}+\operatorname{Branch}_{c, t}^{\mathrm{ABC}}\right) / \text { TotalBranch }_{c, t}
$$

with

Bank of China (BOC), Agricultural Bank of China (ABC), China Construction Bank (CCB) and Industrial and Commercial Bank of China (ICBC).
<!-- #endregion -->

```sos kernel="SoS"
def big_four(x):
    if x == "中国农业银行股份":
        return "中国农业银行股份"
    elif x == "中国工商银行股份":
        return "中国工商银行股份"
    elif x == "中国建设银行股份":
        return "中国建设银行股份"
    elif x == "中国银行股份":
        return "中国银行股份"
    else:
        return 'other'
```

<!-- #region kernel="SoS" -->
We need to construct the following index:

- Number of CCB per city
- HHI
- Concentration ratio
<!-- #endregion -->

```sos kernel="SoS"
df_ccb = (
    #### Number of CCB per city
    df_bank_status.pivot_table(
        values="id",
        index=["status", "bank_name", "geocode4_corr"],
        columns="year",
        aggfunc="nunique",
        fill_value=0,
    )
    .stack()
    .reset_index()
    .rename(columns={0: "count"})
    .assign(
        count=lambda x: x.groupby(["status", "bank_name", "geocode4_corr"])[
            "count"
        ].transform("cumsum"),
        active=lambda x: np.where(x["count"] > 0, 1, 0),
    )
    .groupby(["year", "geocode4_corr", "status"])
    .agg({"count": "sum", "active": "sum"})
    .unstack(-1)
    .collapse_levels(sep="_")
    .rename(columns={"count_no CCB": "count_no_CCB", "active_no CCB": "active_no_CCB",})
    .fillna(0)
    .assign(
        share_count_ccb=lambda x: (x["count_CCB"] / (x["count_CCB"] + x["count_no_CCB"])),
        share_active_ccb=lambda x: (x["active_CCB"] / (x["active_CCB"] + x["active_no_CCB"])),
    )
    .fillna(0)
)
df_ccb.shape
```

```sos kernel="SoS"
#df_bank_status.loc[lambda x: x['geocode4_corr'].isin([1412])]
```

```sos kernel="SoS"
df_ccb.loc[lambda x: x['share_count_ccb'].isin([np.nan])]
```

```sos kernel="SoS"
df_hhi = (
            #### HHI
            df_bank_status.pivot_table(
                values="id",
                index=["bank_name", "geocode4_corr"],
                columns="year",
                aggfunc="nunique",
                fill_value=0,
            )
            .stack()
            .reset_index()
            .rename(columns={0: "count"})
            .assign(
                count=lambda x: x.groupby(["bank_name", "geocode4_corr"])[
                    "count"
                ].transform("cumsum"),
                active=lambda x: np.where(x["count"] > 0, 1, 0),
                total_city=lambda x: x.groupby(["year", "geocode4_corr"])[
                    "count"
                ].transform("sum"),
                total_city_active=lambda x: x.groupby(["year", "geocode4_corr"])[
                    "active"
                ].transform("sum"),
                score_count=lambda x: (x["count"] / x["total_city"]) ** 2,
                score_active=lambda x: (x["active"] / x["total_city_active"]) ** 2,
            )
            .groupby(["year", "geocode4_corr"])
            .agg({"score_count": "sum", "score_active": "sum"})
            .assign(
                hhi_branches=lambda x: 1 - x["score_count"],
                hhi_branches_name=lambda x: 1 - x["score_active"],
            )
        )
df_hhi.shape
```

```sos kernel="SoS"
df_hhi.isna().sum()
```

```sos kernel="SoS"
df_concentration = (
            df_bank_status.assign(
                sob=lambda x: np.where(x['bank_type_adj'].isin(['SOB']), True, False)
            )
            .groupby(["year", "geocode4_corr", "sob"])
            .agg({"id": "count"})
            .sort_values(by=["sob", "geocode4_corr", "year"])
            .reset_index()
            .pivot_table(
                values="id",
                index=["sob", "geocode4_corr"],
                columns="year",
                aggfunc=np.sum,
                fill_value=0,
            )
            .stack()
            .reset_index()
            .rename(columns={0: "count"})
            .sort_values(by = ['year',  'sob','geocode4_corr'])
            .assign(
                count=lambda x: x.groupby(["sob",  "geocode4_corr"])[
                    "count"
                ].transform("cumsum")
            )
            .set_index(['year', 'geocode4_corr', 'sob'])
            .unstack(-1)
    .assign(
    concentration = lambda x: 1-(x[('count',True)] / ( x[('count',True)] +  x[('count',False)]))
    )
    .collapse_levels(sep='_')
        )
```

```sos kernel="SoS"
df_deregulation = pd.concat([df_ccb,df_hhi,df_concentration], axis =1)
df_deregulation = (
    df_deregulation.assign(
        **{
            "lag_{}".format(i): df_deregulation.groupby(["geocode4_corr"])[i].transform(
                "shift"
            )
            for i in df_deregulation.columns
        }
    )
)
df_deregulation.shape
```

```sos kernel="SoS"
df_deregulation = (
    df_deregulation.reindex(
        columns=[
            "count_CCB",
            "lag_count_CCB",
            "count_no_CCB",
            "lag_count_no_CCB",
            "active_CCB",
            "lag_active_CCB",
            "active_no_CCB",
            "lag_active_no_CCB",
            "share_count_ccb",
            "lag_share_count_ccb",
            "share_active_ccb",
            "lag_share_active_ccb",
            #"score_count",
            #"lag_score_count",
            #"score_active",
            #"lag_score_active",
            "hhi_branches",
            "lag_hhi_branches",
            "hhi_branches_name",
            "lag_hhi_branches_name",
            "count_False",
            "lag_count_False",
            "count_True",
            "lag_count_True",
            "concentration",
            "lag_concentration",
        ]
    )
    .reset_index()
    .loc[lambda x: x["year"] > "1997"]
)
df_deregulation.columns = (
    df_deregulation.columns.str.strip()
    .str.replace(" ", "_")
    .str.replace("-", "_")
    .str.lower()
)
df_deregulation.tail()
```

```sos kernel="SoS"
df_deregulation.isna().sum()
```

```sos kernel="SoS"
df_deregulation.shape
```

```sos kernel="SoS"
(
    df_deregulation
    .groupby("year")
    .agg({
        "hhi_branches": "mean",
        "concentration":"mean",
        #'share_branch':'mean'
    })
    .plot.line(title="concentration SOB", figsize=(15, 10))
    .legend(loc="center left", bbox_to_anchor=(1.0, 0.5))
)
plt.show()
```

<!-- #region kernel="SoS" -->
### Merge data
<!-- #endregion -->

```sos kernel="SoS"
df_pol.shape
```

```sos kernel="SoS"
df_bank_dereg_final = (
    df_pol.merge(df_macro)
    .assign(
        province_en=lambda x: x["province_en"].str.strip(),
        geocode4_corr=lambda x: x["geocode4_corr"].astype(str),
        #ind2 = lambda x: x['cic'].astype(str).str.slice(stop = 2)
    )
    .merge(
        (
            df_credit_supply.sort_values(by=["province_en", "year"])
            .assign(
                lag_credit_supply_long_term=lambda x: x.groupby(["province_en"])[
                    "credit_supply_long_term"
                ].transform("shift")
            )
            .reindex(columns=["year", "province_en", "lag_credit_supply_long_term",'credit_supply_long_term'])
        )
    )
    .merge(df_credit_constraint#.assign(ind2=lambda x: x["ind2"].astype(str))
          )
     .merge(
        df_asif.assign(geocode4_corr=lambda x: x["geocode4_corr"].astype(str)).rename(columns={"indu_2": "ind2"})
    )
    .merge(
        df_deregulation.rename(columns={"year_setdate": "year"}).assign(
            year=lambda x: x["year"].astype(int),
            geocode4_corr=lambda x: x["geocode4_corr"].astype(str),
        ),
        how="inner",
        on=["year", "geocode4_corr"],
    )
    .merge(df_innovation.assign(
        geocode4_corr=lambda x: x["geocode4_corr"].astype(str),
    ), on=["year", "geocode4_corr", 'ind2'], how = 'left')
    .merge(df_output.assign(
        geocode4_corr=lambda x: x["geocode4_corr"].astype(str),
    ), on=["year", "geocode4_corr", 'ind2'], how = 'left')
    .merge(df_tcz.drop_duplicates().assign(
        geocode4_corr=lambda x: x["geocode4_corr"].astype(str)),how = 'left'
          )
    .assign(
        tcz = lambda x: x['tcz'].fillna(0),
        spz = lambda x: x['spz'].fillna(0),
        gdp_pop=lambda x: x["gdp"] / x["population"],
        fdi_gdp=lambda x: x["fdi"] / x["gdp"],
        fe_t_i=lambda x: pd.factorize(
            x["year"].astype("string") + x["ind2"].astype("string")
        )[0],
        fe_c_t=lambda x: pd.factorize(
            x["geocode4_corr"].astype("string") + x["year"].astype("string")
        )[0],
        fe_c_i=lambda x: pd.factorize(
            x["geocode4_corr"].astype("string") + x["ind2"].astype("string")
        )[0],
    )
)
df_bank_dereg_final.shape
```

```sos kernel="SoS"
df_bank_dereg_final.head()
```

```sos kernel="SoS"
df_bank_dereg_final.isna().sum().loc[lambda x: x>0].sort_values()
```

```sos kernel="SoS"
(
    df_bank_dereg_final
    .to_csv(
    os.path.join("df_fin_dep_pollution_baseline_industry" + '.csv')
    )
)
```

```sos kernel="SoS"
#(
#    df_bank_dereg_final
#.assign(
#               **{
#            '{}'.format(i) : 
#        df_bank_dereg_final.groupby(['year','province_en'])[i].transform(lambda x: x.fillna(x.mean())) for i 
#            in [
#              'city_commercial_bank',
#     'foreign_bank', 'other', 'policy_bank',
#       'rural_commercial_bank', 'deregulation', 'deregulation_ct', 'total_sob',
#       'concentration_sob', 'concentration_sob_ct', 'totalBranchBank',
#       'totalBranchCity', 'share_branch', 'share_commercial_branch',
#       'lag_city_commercial_bank', 'lag_share_commercial_branch',
#       'lag_totalBranchBank', 'lag_totalBranchCity', 'lag_deregulation_ct',
#       'lag_concentration_sob_ct', 'lag_share_branch']
#        }
#    )   
#)
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
#sys.path.append(os.path.join(parent_path, 'utils'))
#import latex.latex_beautify as lb
#%load_ext autoreload
#%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(fixest)
library(tidyverse)
#library(lfe)
#library(lazyeval)
library('progress')
path = "../../../utils/latex/table_golatex.R"
source(path)
```

```sos kernel="R"
%get df_path
df_final <- read_csv("df_fin_dep_pollution_baseline_industry.csv") %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(tcz = relevel(as.factor(tcz), ref=1))%>%
filter(!is.na(lag_credit_supply_long_term)) %>%
      #filter(!is.na(lag_concentration)) %>%
      #filter(!is.na(lag_totalBranchCity))%>%
      #filter(lag_concentration_sob_ct > 0) %>%
      filter(!is.na(fdi_gdp)) %>%
      filter(!is.na(gdp_pop)) %>%
filter(fdi_gdp  > 0) %>%
filter(employment  > 0) %>%
filter(capital  > 0)

#mutate(
#    tso2_out  = tso2/output,
#    year = relevel(as.factor(year), ref='2001'),
#    fin_dev = 1- share_big_loan,
#    lag_fin_dev = 1- lag_share_big_loan,
#    gdp_pop = gdp/population,
#    fdi_gdp = fdi/gdp
    #gdp	population	gdp_cap	employment_macro	fixedasset_macro
#) 

```

<!-- #region kernel="R" -->
Aggregate at the province-industry-year level
<!-- #endregion -->

<!-- #region kernel="R" heading_collapsed="true" -->
## Variables Definition

1. credit_supply: Province-year supply all loans over GDP
2. credit_supply_long_term: Province-year supply long term loans over GDP
3. fin_dev: Share of non-4-SOCBs' share in credit
<!-- #endregion -->

<!-- #region kernel="R" -->
### Statistic
<!-- #endregion -->

```sos kernel="SoS"
import janitor
import matplotlib
```

```sos kernel="SoS"
query = """
SELECT * 
FROM "almanac_bank_china"."province_loan_and_credit"
LEFT JOIN (
SELECT province_en, larger_location, coastal
FROM "chinese_lookup"."geo_chinese_province_location"
) as prov on province_loan_and_credit.province_en = prov.province_en
"""
df_credit = (
    s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename='fig_2',  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = {'year':'string', 'fin_dep':'float'}
)
    .groupby(['year'])
    .agg(
        {
            'total_long_term_loan':'sum',
            'total_gdp':'sum',
            
        })
    .assign(
        supply_long_term = lambda x: x['total_long_term_loan']/x['total_gdp'],
    )
    .drop(columns = ['total_long_term_loan','total_gdp'])
)
df_credit
```

```sos kernel="SoS"
graph = (
    (
        df_bank_branches
        # .loc[lambda x: ~x["geocode4_corr"].isin(["<NA>"])]
        .merge(df_bank.reindex(columns=["id", "certcode"]))
        .assign(
            bank_full_name=lambda x: x["certcode"].astype(str).str.slice(stop=5),
            city_id=lambda x: x["certcode"].astype(str).str.slice(start=7, stop=11),
            is_equal=lambda x: x["city_id"] == x["geocode4_corr"],
            geocode4_corr=lambda x: np.where(
                x["is_equal"], x["geocode4_corr"], x["city_id"]
            ),
            types=lambda x: x["certcode"].astype(str).str.slice(start=5, stop=6),
        )
        .loc[lambda x: x["types"].isin(["S"])]
        .assign(
            bank_type_adj_clean=lambda x: x["bank_type_adj_clean"].fillna(
                "city commercial bank"
            )
        )
        # .dropna(subset=["bank_type_adj_clean", "bank_type_adj"])
        .pivot_table(
            values="id",
            index=["bank_type_adj_clean", "geocode4_corr"],
            columns="year_setdate",
            aggfunc=len,
            fill_value=0,
        )
        .stack()
        .reset_index()
        .rename(columns={0: "count"})
        .assign(
            count=lambda x: x.groupby(["bank_type_adj_clean", "geocode4_corr"])[
                "count"
            ].transform("cumsum")
        )
        .set_index(["bank_type_adj_clean", "geocode4_corr", "year_setdate"])
        .unstack(0)
        .fillna(0)
        .droplevel(axis=1, level=0)
        .reset_index()
        .loc[lambda x: ~x["year_setdate"].isin(["<NA>"])]
    )
    .groupby(['year_setdate'])
    .agg({
        'SOB':'sum',
        'city commercial bank':'sum',
        'foreign bank':'sum',
        'policy bank':'sum',
        'rural commercial bank':'sum',
    })
    .loc[lambda x: x.index > '1996']
    .loc[lambda x: x.index < '2008']
    #.assign(year = lambda x: x.index)
    #.merge(df_credit.reset_index().assign(year = lambda x: x['year'].astype(str))
    #       ,on = ['year'])
)
graph
```

```sos kernel="SoS"
sns.set(color_codes=True)
sns.set_style("white")
```

```sos kernel="SoS"
temp = (
            df_bank_branches.assign(
                sob=lambda x: x.apply(lambda x: big_four(x["bank_full_name"]), axis=1)
            )
            .loc[lambda x: ~x["geocode4_corr"].isin(["<NA>"])]
            .dropna(subset=["bank_type_adj_clean", "bank_type_adj"])
            .groupby(["year_setdate", "geocode4_corr", "sob"])
            .agg({"id": "count"})
            .sort_values(by=["sob", "geocode4_corr", "year_setdate"])
            .reset_index()
            .pivot_table(
                values="id",
                index=["sob", "geocode4_corr"],
                columns="year_setdate",
                aggfunc=np.sum,
                fill_value=0,
            )
            .stack()
            .reset_index()
            .rename(columns={0: "count"})
            .assign(
                temp=lambda x: x.groupby(["sob", "geocode4_corr"])[
                    "count"
                ].transform("cumsum"),
                temp_1=lambda x: x.groupby(["sob", "geocode4_corr", "temp"])[
                    "year_setdate"
                ]
                .transform("min")
                .fillna("2222")
                .astype("int"),
                first_entry=lambda x: x.groupby(["sob", "geocode4_corr"])[
                    "temp_1"
                ].transform("min"),
                count=lambda x: x["count"].fillna(0),
            )
            .loc[lambda x: x["year_setdate"].astype("int") >= x["first_entry"]]
            .drop(columns=["temp", "temp_1", "first_entry"])
            .assign(
                totalBranchBank=lambda x: x.groupby(["sob", "geocode4_corr"])[
                    "count"
                ].transform("cumsum"),
                totalBranchCity=lambda x: x.groupby(["geocode4_corr", "year_setdate"])[
                    "totalBranchBank"
                ].transform("sum"),
                #    score = lambda x: (x['totalBranchBank']/x['totalBranchCity'])**2
            )
            .loc[lambda x: x["sob"] != "other"]
            .set_index(["sob", "geocode4_corr", "year_setdate", "totalBranchCity"])
            .drop(columns=["count"])
            .unstack(0)
            .assign(total_sob=lambda x: x.sum(axis=1))
            .reset_index(["totalBranchCity"])
            #.groupby('year_setdate')
            #.apply(sum)
            .assign(
                concentration_sob=lambda x: x[("total_sob", "")]
                / x[("totalBranchCity", "")]
            )
            .reindex(columns=[("total_sob", ""), ("concentration_sob", "")])
            .droplevel(axis=1, level=1)
            .reset_index()
            .dropna(subset=["concentration_sob"])
            .assign(concentration_sob_ct=lambda x: 1 - x["concentration_sob"]
                   )
    .loc[lambda x: x['year_setdate'] > '1996']
    .loc[lambda x: x['year_setdate'] < '2008']
    .groupby(['year_setdate'])
    .agg({'concentration_sob_ct':'mean'})
        )
```

```sos kernel="SoS"
#df_bank_branches
```

```sos kernel="SoS"
temp = (
    (
        df_bank_branches
        # .loc[lambda x: ~x["geocode4_corr"].isin(["<NA>"])]
        .merge(df_bank.reindex(columns=["id", "certcode"]))
        .assign(
            bank_full_name=lambda x: x["certcode"].astype(str).str.slice(stop=5),
            city_id=lambda x: x["certcode"].astype(str).str.slice(start=7, stop=11),
            is_equal=lambda x: x["city_id"] == x["geocode4_corr"],
            geocode4_corr=lambda x: np.where(
                x["is_equal"], x["geocode4_corr"], x["city_id"]
            ),
            types=lambda x: x["certcode"].astype(str).str.slice(start=5, stop=6),
        )
        .loc[lambda x: x["types"].isin(["S"])]
        .assign(
            bank_type_adj_clean=lambda x: x["bank_type_adj_clean"].fillna(
                "city commercial bank"
            )
        )
    )
    .loc[lambda x: x['bank_type_adj_clean'].isin(['city commercial bank'])]
    .pivot_table(
            values="id",
            index=["bank_type_adj_clean", "geocode4_corr", 'bank_full_name'],
            columns="year_setdate",
            aggfunc=len,
            fill_value=0,
        )
    .stack()
        .reset_index()
        .rename(columns={0: "count"})
        .assign(
            count=lambda x: x.groupby(["bank_type_adj_clean", "geocode4_corr",'bank_full_name'])[
                "count"
            ].transform("cumsum")
        )
        .set_index(["bank_type_adj_clean", "geocode4_corr",'bank_full_name', "year_setdate"])
        .unstack(0)
        .fillna(0)
        .droplevel(axis=1, level=0)
        .reset_index()
        .loc[lambda x: ~x["year_setdate"].isin(["<NA>"])]
        .set_index(["geocode4_corr","bank_full_name","year_setdate"], append = True)
        #.loc[lambda x: x>0]
)
```

```sos kernel="SoS"
(
    temp
    .assign(sum_ = lambda x: x.sum(axis =1))
    .loc[lambda x: x['sum_']>0]
    .reset_index()
    .groupby(['year_setdate'])
    .agg({'geocode4_corr':'nunique'})
    .reset_index()
    .loc[lambda x: x['year_setdate'] > '1996']
    .loc[lambda x: x['year_setdate'] < '2008']
)
```

```sos kernel="SoS"
fig, ax = plt.subplots(figsize=(10, 8))
ax.plot(df_credit.index,
              df_credit['supply_long_term'],label = 'Supply of long term credit')
ax.plot(df_credit.index,
              temp['concentration_sob_ct'], color ='green',label = 'Concentration ratio')
ax.set_ylabel("Share")
ax2 = ax.twinx()
ax2.plot(df_credit.index,
               graph['city commercial bank'], color = "red", label = 'Number of city branches')
ax2.set_ylabel("Number of city branches")
plt.xlabel('Year')
plt.ylabel("Share of long term credit supply over GDP")
#ax.spines['right'].set_visible(False)
#ax.spines['top'].set_visible(False)
ax.legend(bbox_to_anchor=(1.08, 1), loc=2, borderaxespad=0.)
ax2.legend(bbox_to_anchor=(1.08, .9), loc=2, borderaxespad=0.)
plt.xticks(df_credit.index,rotation=30)
#plt.title('Evolution of share of non-state bank in total loan')
plt.show()
#plt.savefig("Figures/fig_5.png",
#            bbox_inches='tight',
#            dpi=600)
```

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

<!-- #region kernel="R" -->
- https://cran.r-project.org/web/packages/fixest/vignettes/standard_errors.html
- https://lrberge.github.io/fixest/index.html
<!-- #endregion -->

```sos kernel="R"
dim(df_final)
```

```sos kernel="SoS"
df_bank_dereg_final.columns
```

<!-- #region kernel="SoS" -->
- 'count_ccb',
- 'lag_count_ccb' : OK
- active_ccb
- lag_active_ccb: OK
- share_count_ccb
- lag_share_count_ccb: OK
- share_active_ccb
- lag_share_active_ccb: OK
- hhi_branches
- lag_hhi_branches: OK
- hhi_branches_name
- lag_hhi_branches_name: OK
- concentration
- lag_concentration: OK
<!-- #endregion -->

<!-- #region kernel="SoS" -->
- 'lag_count_ccb' : NOT OK
- lag_active_ccb: NOT OK
- lag_share_count_ccb: NOT OK
- lag_share_active_ccb: NOT OK
- lag_hhi_branches: NOT OK
- lag_hhi_branches_name: NOT OK
- lag_concentration: NOT OK
<!-- #endregion -->

```sos kernel="SoS"
#df_bank_dereg_final[['tso2']].describe(percentiles = np.arange(0,1,.05))
```

```sos kernel="R"
%get path table
#### S02
t0 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tso2 > 0)
      ,
      vcov = 'iid'
              )

t1 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tso2 > 0)
      ,
      vcov = 'iid')
#### COD
t2 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tcod > 40)
      ,
      vcov = 'iid'
              )

t3 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tcod > 40)
      ,
      vcov = 'iid'
              )

#### WASTE WATER
t4 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(twaste_water > 0)
      ,
      vcov = 'iid'
              )
t5 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(twaste_water > 0)
      ,
      vcov = 'iid'
              )
#table_1 <- go_latex(list(
#    t_0,t_1, t_2, t_3, t_4#, t_5
#),
#    title="Pollution emission, credit supply and financial development",
#    dep_var = dep,
#    addFE=fe1,
#    save=TRUE,
#    note = FALSE,
#    name=path
#) 
```

```sos kernel="R"
t3
```

```sos kernel="R"
etable(t0,t1, t2,t3, t4,t5,
         #vcov = "iid",
       headers = c("1", "2", "3", "4", "5", "6"),
       tex = TRUE,
       digits = 3,
      digits.stats = 3
)
```

<!-- #region kernel="R" -->
## Polluting vs less polluting

![image.png](attachment:4e155f34-2699-4c04-ad30-e9f076b69c51.png)
<!-- #endregion -->

```sos kernel="R"
list_polluting = list(
    6,8,10, 
    #14
    15, 17, 19,
    #22,
    25,26,
    #27,
    31
    #,32, 33, 44
)
```

```sos kernel="R"
%get path table
###### TSO2
### CITY DOMINATED
t0 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )


t1 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final%>%filter(!ind2 %in% list_polluting)  %>%filter(tso2 > 0)
      , 
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t2 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t3 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(!ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TCOD
t4 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final%>%filter(ind2 %in% list_polluting)  %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t5 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(!ind2 %in% list_polluting) %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t6 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(ind2 %in% list_polluting) %>%filter(tcod > 40)
      , 
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t7 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(!ind2 %in% list_polluting) %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TWASTE WATER
t8 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final%>%filter(ind2 %in% list_polluting)  %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t9 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final%>%filter(!ind2 %in% list_polluting)  %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t10 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(ind2 %in% list_polluting) %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t11 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(!ind2 %in% list_polluting) %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

```sos kernel="R"
etable(t0,t1, t2,t3, t4,t5,t6,t7, t8,t9, t10,t11,
         #vcov = "iid",
       headers = c("1", "2", "3", "4", "5", "6"),
       tex = TRUE,
       digits = 3,
      digits.stats = 3
)
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
SELECT year, soe, geocode4_corr,SUM(output) as output, SUM(employ) as employ, SUM(captal) as capital
FROM (
SELECT *,
CASE WHEN ownership in ('SOE') THEN 'SOE' ELSE 'PRIVATE' END AS soe
FROM test 
  )
  GROUP BY soe, geocode4_corr, year
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
            .set_index(['year',
                        #'indu_2',
                        'soe', 
                        'geocode4_corr'])
            .unstack(-2)
            .assign(
                soe_dominated = lambda x: x[(v, 'SOE')] > x[(v, 'PRIVATE')],
                share_soe = lambda x: x[(v, 'SOE')] / (x[(v, 'SOE')] + x[(v, 'PRIVATE')])
            )
            #.loc[lambda x: x['soe_dominated'].isin([True])]
            .collapse_levels("_")
            .reset_index()
            [['year','geocode4_corr', 
              #'indu_2',
              "soe_dominated", 
             'share_soe'
             ]]
            .loc[lambda x: x['year'].isin(["2000"])]
            .drop(columns = ['year'])
            #.rename(columns = {'indu_2':'ind2'})
            .loc[lambda x: x['share_soe']> t]
            .to_csv('list_city_soe_{}_{}.csv'.format(v, t), index = False)
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
SELECT year, foreign, geocode4_corr,SUM(output) as output, SUM(employ) as employ, SUM(captal) as capital
FROM (
SELECT *,
CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS foreign
FROM test 
  )
  GROUP BY foreign, geocode4_corr, year

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
            .set_index(['year',
                        #'indu_2',
                        'foreign', 'geocode4_corr'])
            .unstack(-2)
            .assign(
                for_dominated = lambda x: x[(v, 'FOREIGN')] > x[(v, 'DOMESTIC')],
                share_for = lambda x: x[(v, 'FOREIGN')] / (x[(v, 'FOREIGN')] + x[(v, 'DOMESTIC')])
            )
            .collapse_levels("_")
            .reset_index()
            [['year','geocode4_corr', 
              #'indu_2',
              "for_dominated", 
             'share_for'
             ]]
            .loc[lambda x: x['year'].isin(["2000"])]
            .drop(columns = ['year'])
            #.rename(columns = {'indu_2':'ind2'})
            .loc[lambda x: x['share_for']> t]
            #.groupby(['soe_dominated'])
            #.agg({'share_soe':'describe'})
            .to_csv('list_city_for_{}_{}.csv'.format(v, t), index = False)
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
df_soe <- df_final %>% inner_join(read_csv('list_city_soe_output_0.5.csv'))
df_priv <- df_final %>% left_join(read_csv('list_city_soe_output_0.5.csv'))
df_dom <- df_final %>% left_join(read_csv('list_city_for_output_0.5.csv')) %>% filter(is.na(share_for))
print(dim(df_soe)[1] + dim(df_priv)[1] == dim(df_final)[1])
###### TSO2
### CITY DOMINATED
t0 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_soe %>%filter(ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )


t1 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_priv%>%filter(!ind2 %in% list_polluting)  %>%filter(tso2 > 0)
      , 
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t2 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_soe %>%filter(ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t3 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_priv %>%filter(!ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TCOD
t4 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_soe%>%filter(ind2 %in% list_polluting)  %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t5 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_priv %>%filter(!ind2 %in% list_polluting) %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t6 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_soe %>%filter(ind2 %in% list_polluting) %>%filter(tcod > 40)
      , 
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t7 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_priv %>%filter(!ind2 %in% list_polluting) %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TWASTE WATER
t8 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_soe%>%filter(ind2 %in% list_polluting)  %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t9 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_priv%>%filter(!ind2 %in% list_polluting)  %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t10 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_soe %>%filter(ind2 %in% list_polluting) %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t11 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_priv %>%filter(!ind2 %in% list_polluting) %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

```sos kernel="R"
etable(t0,t1, t2,t3, t4,t5,t6,t7, t8, t9, t10, t11,
         #vcov = "iid",
       headers = c("1", "2", "3", "4", "5", "6"),
       tex = TRUE,
       digits = 3,
      digits.stats = 3
      )
```

<!-- #region kernel="R" -->
Foreign vs dom
<!-- #endregion -->

```sos kernel="R"
df_for <- df_final %>% inner_join(read_csv('list_city_for_output_0.5.csv'))
df_dom <- df_final %>% left_join(read_csv('list_city_for_output_0.5.csv')) %>% filter(is.na(share_for))
t0 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>%filter(ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )


t1 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom%>%filter(!ind2 %in% list_polluting)  %>%filter(tso2 > 0)
      , 
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t2 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>%filter(ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t3 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>%filter(!ind2 %in% list_polluting) %>%filter(tso2 > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TCOD
t4 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for%>%filter(ind2 %in% list_polluting)  %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t5 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>%filter(!ind2 %in% list_polluting) %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t6 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>%filter(ind2 %in% list_polluting) %>%filter(tcod > 40)
      , 
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t7 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>%filter(!ind2 %in% list_polluting) %>%filter(tcod > 40)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TWASTE WATER
t8 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for%>%filter(ind2 %in% list_polluting)  %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t9 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom%>%filter(!ind2 %in% list_polluting)  %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t10 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>%filter(ind2 %in% list_polluting) %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t11 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>%filter(!ind2 %in% list_polluting) %>%filter(twaste_water > 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

<!-- #region kernel="R" -->
# Mechanism

 - CF paper Bank Deregulation and Corporate Environmental Performance , page 18
 - Investment in pollution abatement equipment
   - equipment to abate wastewater
   - equipment to abate SO2
 - Technology upgrading
   - total water usage during production.
   - water usage per output value
   - patent value-adjusted index
 - The asset mix channel
   - tangible asset
 - Greening through finance
   - Reduce output
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Investment in pollution abatement equipment

- tdso2_equip
- tdwastegas_equip
- dwastewater_equip
- tdwastegas_equip_output
- tdso2_equip_output
<!-- #endregion -->

```sos kernel="R"
summary(feols(
    tdso2_equip  ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint+
            lag_count_ccb * credit_constraint 
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% inner_join(df_final %>%filter(tso2 > 0) %>% select(...1)), 
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              ))
```

```sos kernel="R"
feols(dwastewater_equip ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% inner_join(df_final %>%filter(twaste_water > 0) %>% select(...1)),
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

```sos kernel="R"
feols(tdwastegas_equip ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% inner_join(df_final %>%filter(twaste_water > 0) %>% select(...1))
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

<!-- #region kernel="R" -->
### Technology upgrading

 • 'total_industrialwater_used',
 • 'total_freshwater_used',
 • 'total_repeatedwater_used',
 • 'total_coal_used',
 • 'clean_gas_used'
 • 'trlmxf',
 • 'tylmxf'
 • innovation_index
<!-- #endregion -->

<!-- #region kernel="R" -->
Fresh water
<!-- #endregion -->

```sos kernel="R"
feols(log(total_freshwater_used_o) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% inner_join(df_final %>%filter(tcod > 40) %>% select(...1)) %>% 
      mutate(total_freshwater_used_o = total_freshwater_used/sales) 
      ,
      cluster = ~geocode4_corr + ind2#
      #vcov = 'iid'
              )
```

```sos kernel="R"
feols(log(total_freshwater_used_o) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% inner_join(df_final %>%filter(tcod > 40) %>% select(...1)) %>% 
      mutate(total_freshwater_used_o = total_freshwater_used/sales) 
      ,
      cluster = ~geocode4_corr + ind2#
      #vcov = 'iid'
              )
```

<!-- #region kernel="R" -->
Innovation index
<!-- #endregion -->

```sos kernel="R"
feols(innovation_index ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_count_ccb * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% mutate(innovation_index = ifelse(is.na(innovation_index), 0, innovation_index))
      %>% inner_join(df_final %>%filter(tcod > 40) %>% select(...1))
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

```sos kernel="R"
feols(innovation_index ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            lag_credit_supply_long_term * credit_constraint +
            lag_hhi_branches * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% mutate(innovation_index = ifelse(is.na(innovation_index), 0, innovation_index))
      %>% inner_join(df_final %>%filter(tcod > 40) %>% select(...1))
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

<!-- #region kernel="R" heading_collapsed="true" -->
### Asset mix
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT 
      year, 
      geocode4_corr, 
      indu_2,
      SUM(output) AS output,
      SUM(employ) AS employment, 
      SUM(captal) AS capital,
      SUM(sales) as sales,
      SUM(toasset) as toasset
FROM(
SELECT 
  firm, 
  year, 
  geocode4_corr, 
  CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
    '0', 
    substr(cic, 1, 1)
  ) END AS indu_2, 
  output, 
  employ, 
  captal, 
  sales, 
  toasset 
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
  GROUP BY year, indu_2 ,geocode4_corr
"""
df_size  = (s3.run_query(
            query=query,
            database="firms_survey",
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
                )
df_size.head()
```

```sos kernel="SoS"
temp = (
    df_size.groupby([
        "year",
        "indu_2",
        'geocode4_corr'
    ])
    .agg(
        {
            "output": "sum",
            "employment": "sum",
            "capital": "sum",
            "sales": "sum",
            "toasset": "sum",
        }
    )
    .reset_index()
    .collapse_levels(sep="_")
    .merge(
        (
            df_size.groupby([
                "year", "indu_2"
                ,'geocode4_corr'
                            ])
            .agg("sum")
            .groupby(["year"
                      ,'geocode4_corr'
                     ])
            .agg(
                {
                    "output": ["mean", "median"],
                    "employment": ["mean", "median"],
                    "capital": ["mean", "median"],
                    "sales": ["mean", "median"],
                    "toasset": ["mean", "median"],
                }
            )
            .reset_index()
            .collapse_levels(sep="_")
        )
    )
    .merge(
     (
            df_size.groupby(["year", "indu_2"
                             ,'geocode4_corr'
                            ])
            .agg("sum")
            .groupby(["year"
                      ,'geocode4_corr'
                     ])
            .quantile(.75)
            .reset_index()
            .collapse_levels(sep="_")
            .rename(
            columns = {
                'output':'output_75',
                'employment':'employment_75',
                'capital':'capital_75',
                'sales':'sales_75',
                'toasset':'toasset_75'
            }
            )
        )
    )
)
temp = (
    temp
    .assign(
    **{
        "large_{}_mean".format(i) : temp[i] > temp['{}_mean'.format(i)]
        for i in ['output','employment','capital','sales', 'toasset']
    },
        **{
        "large_{}_median".format(i) : temp[i] > temp['{}_median'.format(i)]
        for i in ['output','employment','capital','sales', 'toasset']
        },
        **{
        "large_{}_75".format(i) : temp[i] > temp['{}_75'.format(i)]
        for i in ['output','employment','capital','sales', 'toasset']
        }
    )
    .drop(columns = [
        #'geocode4_corr',
        'output', 'employment', 'capital', 'sales', 'toasset'])
    .rename(columns = {'indu_2':'ind2'})
)
temp.to_csv('size.csv')
temp.head()
```

```sos kernel="SoS"
for i in ['output','employment','capital','sales', 'toasset']:
    for j in ['mean', 'median','75']:
        print(temp["large_{}_{}".format(i, j)].value_counts())
```

```sos kernel="SoS"
df_bank_dereg_final.columns
```

<!-- #region kernel="R" heading_collapsed="true" -->
### Scale effect
<!-- #endregion -->

<!-- #region kernel="R" heading_collapsed="true" -->
# Archive
<!-- #endregion -->

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
df_for <- df_final %>% inner_join(read_csv('list_city_for_output_0.5.csv'))
df_dom <- df_final %>% left_join(read_csv('list_city_for_output_0.5.csv')) %>% filter(is.na(share_for))
###### TSO2
### CITY DOMINATED
t0 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>%filter(tso2 > 300)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t1 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>%filter(tso2 > 300)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t2 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>% filter(tso2 > 300)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t3 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>% filter(tso2 > 300)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TCOD
t4 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>%filter(tcod > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t5 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>%filter(tcod > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t6 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>% filter(tcod > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t7 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>% filter(tcod > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TWASTE WATER
t8 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>%filter(twaste_water > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t9 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>%filter(twaste_water > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t10 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_for %>% filter(twaste_water > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t11 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +  
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_dom %>% filter(twaste_water > 100)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

```sos kernel="R"
t1
```

```sos kernel="R"
etable(t0,t1, t2,t3, t4,t5,t6,t7, t8, t9, t10, t11,
         #vcov = "iid",
       headers = c("1", "2", "3", "4", "5", "6"),
       tex = TRUE,
       digits = 3,
      digits.stats = 3
      )
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
### TCZ & SPZ
###### TSO2

###### TSO2
### CITY DOMINATED
t0 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tso2 > 300& tcz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t1 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tso2 > 300& tcz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t2 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tso2 > 300& tcz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t3 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tso2 > 300& tcz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TCOD
t4 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tcod > 100& tcz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t5 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tcod > 100& tcz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t6 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tcod > 100& tcz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t7 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tcod > 100& tcz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TWASTE WATER
t8 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(twaste_water > 100& tcz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t9 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(twaste_water > 100& tcz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t10 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(twaste_water > 100& tcz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t11 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(twaste_water > 100& tcz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

```sos kernel="R"
etable(t0,t1, t2,t3, t4,t5,t6,t7, t8, t9, t10, t11,
         #vcov = "iid",
       headers = c("1", "2", "3", "4", "5", "6"),
       tex = TRUE,
       digits = 3,
      digits.stats = 3
      )
```

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

```sos kernel="R"
%get path table
### TCZ & SPZ
###### TSO2
### CITY DOMINATED
t0 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tso2 > 300& spz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t1 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tso2 > 300& spz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t2 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tso2 > 300& spz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

t3 <- feols(log(tso2) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tso2 > 300& spz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TCOD
t4 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tcod > 100& spz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t5 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(tcod > 100& spz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t6 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tcod > 100& spz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t7 <- feols(log(tcod) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(tcod > 100& spz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )

###### TWASTE WATER
t8 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(twaste_water > 100& spz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t9 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            #intensity + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_concentration_sob_ct) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>%filter(twaste_water > 100& spz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t10 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint + 
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(twaste_water > 100& spz == 0)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
t11 <- feols(log(twaste_water) ~  
            log(output) + log(employment) + log(capital) + 
            log(gdp_pop) * credit_constraint +
            log(fdi_gdp) * credit_constraint +
            log(lag_credit_supply_long_term) * credit_constraint +
            log(lag_totalBranchCity) * credit_constraint
           | fe_t_i + fe_c_t + fe_c_i,
      df_final %>% filter(twaste_water > 100& spz == 1)
      ,
      #cluster = ~geocode4_corr + ind2#
      vcov = 'iid'
              )
```

```sos kernel="R"
etable(t0,t1, t2,t3, t4,t5,t6,t7, t8, t9, t10, t11,
         #vcov = "iid",
       headers = c("1", "2", "3", "4", "5", "6"),
       tex = TRUE,
       digits = 3,
      digits.stats = 3
      )
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
