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
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# US Name

Transform fin dep pollution baseline industry table by merging china_city_reduction_mandate and others variables


# Description

Step : Merge dataset to construct indsutry baseline table
Follow this notebook to construct the table https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/04_fin_dep_pol_baseline_city.md and https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/01_fin_dep_pol_baseline.md for the industry level (less accurate)

## Variables

1. Compute sob loan share
2. Compute credit supply
3. merge credit with baseline
    - Currently, table has province Chinese name only
        1. Merge credit supply using year and province
        2. Merge big bank share using year and province
    - up to 2004

## Merge

- china_city_reduction_mandate
- china_tcz_spz
- china_city_code_normalised
- ind_cic_2_name
- china_credit_constraint
- asif_city_characteristics_ownership
- asif_industry_characteristics_ownership
- china_sector_pollution_threshold
- asif_tfp_firm_level
- province_credit_constraint

### Complementary information

**Reminder** 



# Target

- The file is saved in S3:
- bucket: datalake-datascience
- path: DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/INDUSTRY
- Glue data catalog should be updated
- database: environment
- Table prefix: fin_dep_pollution_baseline_
- table name: fin_dep_pollution_baseline_industry
- Analytics
- HTML: ANALYTICS/HTML_OUTPUT/fin_dep_pollution_baseline_industry
- Notebook: ANALYTICS/OUTPUT/fin_dep_pollution_baseline_industry

# Metadata

- Key: 167_Pollution and Credit Constraint
- Epic: Dataset transformation
- US: Transform city-industry-year baseline table
- Task tag: #data-preparation, #data-transformation, #industry
- Analytics reports: 

# Input Cloud Storage

## Table/file

**Name**

- asif_industry_financial_ratio_industry
- china_city_reduction_mandate
- china_tcz_spz
- china_city_code_normalised
- ind_cic_2_name
- china_credit_constraint
- asif_city_characteristics_ownership
- asif_industry_characteristics_ownership
- china_sector_pollution_threshold

**Github**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/00_asif_financial_ratio.md
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_CREDIT_CONSTRAINT/financial_dependency.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/07_dominated_city_ownership.md
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/08_dominated_industry_ownership.md
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/02_data_analysis/01_model_estimation/00_estimate_fin_ratio/00_so2_fin_ratio.md

# Destination Output/Delivery

## Table/file

**Name**

fin_dep_pollution_baseline_industry

**GitHub**

- https://github.com/thomaspernet/pollution_credit_constraint/blob/master/01_data_preprocessing/02_transform_tables/00_credit_constraint_industry.md
<!-- #endregion -->
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json, re

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True) 
glue = service_glue.connect_glue(client = client) 
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# Prepare query 

Write query and save the CSV back in the S3 bucket `datalake-datascience` 


# Steps


## Example step by step

```python
DatabaseName = 'environment'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

```python
query= """
WITH aggregate_pol AS (
SELECT 
    year, 
    geocode4_corr, 
    province_en, 
    cityen, 
    -- indus_code AS cic,
    ind2, 
    SUM(tso2) as tso2, 
    lower_location, 
    larger_location, 
    coastal 
  FROM 
    (
      SELECT 
        year, 
        province_en, 
        citycode, 
        geocode4_corr, 
        china_city_sector_pollution.cityen, 
        --indus_code,
        ind2,  
        tso2, 
        lower_location, 
        larger_location, 
        coastal 
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
    cityen, 
    --indus_code,
    ind2, 
    lower_location, 
    larger_location, 
    coastal
) 
SELECT 
  aggregate_pol.year, 
  CASE WHEN aggregate_pol.year in (
    '1998','1999','2000','2001', '2002', '2003', '2004', '2005'
  ) THEN 'FALSE' WHEN aggregate_pol.year in ('2006', '2007') THEN 'TRUE' END AS period, 
  aggregate_pol.province_en, 
  cityen, 
  aggregate_pol.geocode4_corr, 
  CASE WHEN tcz IS NULL THEN '0' ELSE tcz END AS tcz, 
  CASE WHEN spz IS NULL OR spz = '#N/A' THEN '0' ELSE spz END AS spz, 
  --aggregate_pol.cic, 
  aggregate_pol.ind2, 
  CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short, 
  polluted_d50i, 
  polluted_d75i, 
  polluted_d80i, 
  polluted_d85i, 
  polluted_d90i, 
  polluted_d95i, 
  polluted_mi,
  polluted_d50_cit, 
  polluted_d75_cit, 
  polluted_d80_cit, 
  polluted_d85_cit, 
  polluted_d90_cit, 
  polluted_d95_cit, 
  polluted_m_cit,
  tso2, 
  CAST(
    tso2 AS DECIMAL(16, 5)
  ) / CAST(
    output AS DECIMAL(16, 5)
  ) AS so2_intensity, 
  tso2_mandate_c,
  target_reduction_so2_p,
  above_threshold_mandate,
  above_average_mandate,
  avg_ij_o_city_mandate,
  CASE WHEN d_avg_ij_o_city_mandate IS NULL THEN 'FALSE' ELSE d_avg_ij_o_city_mandate END AS d_avg_ij_o_city_mandate,
  in_10_000_tonnes, 
  credit_constraint,
  financial_dep_us,
  liquidity_need_us,
  rd_intensity_us,
  1/supply_all_credit as supply_all_credit,
  1/supply_long_term_credit as supply_long_term_credit,
  share_big_bank_loan,
  lag_share_big_bank_loan,
  share_big_loan,
  lag_share_big_loan,
  credit_supply,
  lag_credit_supply,
  credit_supply_long_term,
  lag_credit_supply_long_term,
  credit_supply_short_term,
  lag_credit_supply_short_term,
  output,
  sales,
  employment,
  capital,
  current_asset,
  tofixed,
  total_liabilities,
  total_asset,
  tangible,
  cashflow,
  current_ratio,
  lag_current_ratio,
  liabilities_tot_asset,
  sales_tot_asset,
  lag_sales_tot_asset,
  asset_tangibility_tot_asset,
  lag_liabilities_tot_asset,
  cashflow_to_tangible,
  lag_cashflow_to_tangible,
  cashflow_tot_asset,
  lag_cashflow_tot_asset,
  return_to_sale,
  lag_return_to_sale,
  lower_location, 
  larger_location, 
  coastal,
  dominated_output_soe_c,
  dominated_employment_soe_c,
  dominated_sales_soe_c,
  dominated_capital_soe_c,
  dominated_output_for_c,
  dominated_employment_for_c,
  dominated_sales_for_c,
  dominated_capital_for_c,  
  dominated_output_i,
  dominated_employ_i,
  dominated_sales_i,
  dominated_capital_i,
  dominated_output_soe_i,
  dominated_employment_soe_i,
  dominated_sales_soe_i,
  dominated_capital_soe_i,
  dominated_output_for_i,
  dominated_employment_for_i,
  dominated_sales_for_i,
  dominated_capital_for_i,  
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      aggregate_pol.ind2
  ) AS fe_c_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      aggregate_pol.year, 
      aggregate_pol.ind2
  ) AS fe_t_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      aggregate_pol.year
  ) AS fe_c_t 
FROM 
  aggregate_pol 
  INNER JOIN (
  SELECT 
  indu_2,
  current_asset,
  tofixed,
  total_liabilities,
  total_asset,
  tangible,
  cashflow,
  current_ratio,
  lag_current_ratio,
  liabilities_tot_asset,
  sales_tot_asset,
  lag_sales_tot_asset,
  asset_tangibility_tot_asset,
  lag_liabilities_tot_asset,
  cashflow_to_tangible,
  lag_cashflow_to_tangible,
  cashflow_tot_asset,
  lag_cashflow_tot_asset,
  return_to_sale,
  lag_return_to_sale
    
  FROM firms_survey.asif_industry_financial_ratio_industry 
  WHERE year = '2002'
  ) AS asif_industry_financial_ratio_industry
  ON 
  aggregate_pol.ind2 = asif_industry_financial_ratio_industry.indu_2 
  INNER JOIN (
    SELECT 
  geocode4_corr, 
  tso2_mandate_c, 
  target_reduction_so2_p,
  in_10_000_tonnes, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      transform(
        sequence(1, 4), 
        x -> tso2_mandate_c
      ), 
      tso2_mandate_c_pct, 
      (x, y) -> x < y
    )
  ) AS above_threshold_mandate, 
  CASE WHEN tso2_mandate_c > tso2_mandate_c_avg THEN 'ABOVE' ELSE 'BELOW' END AS above_average_mandate 
FROM 
  (
    (
      SELECT 
        'TEMP' as temp, 
        approx_percentile(
          tso2_mandate_c, ARRAY[.5,.75,.90, 
          .95]
        ) AS tso2_mandate_c_pct, 
        AVG(tso2_mandate_c) AS tso2_mandate_c_avg 
      FROM 
        policy.china_city_reduction_mandate
    ) as percentile 
    LEFT JOIN (
      SELECT 
        'TEMP' as temp, 
        citycn, 
        tso2_mandate_c, 
        target_reduction_so2_p,
        in_10_000_tonnes 
      FROM 
        policy.china_city_reduction_mandate
    ) as mandate ON percentile.temp = mandate.temp
  ) as map_mandate
  INNER JOIN chinese_lookup.china_city_code_normalised ON map_mandate.citycn = china_city_code_normalised.citycn 
    WHERE 
      extra_code = geocode4_corr
  ) as city_mandate ON aggregate_pol.geocode4_corr = city_mandate.geocode4_corr 
  LEFT JOIN policy.china_city_tcz_spz ON aggregate_pol.geocode4_corr = china_city_tcz_spz.geocode4_corr 
  LEFT JOIN chinese_lookup.ind_cic_2_name ON aggregate_pol.ind2 = ind_cic_2_name.cic 
  LEFT JOIN (
    SELECT 
      ind2, 
      year,
      polluted_d50i, 
      polluted_d75i, 
      polluted_d80i, 
      polluted_d85i, 
      polluted_d90i, 
      polluted_d95i, 
      polluted_mi
    FROM 
      "environment"."china_sector_pollution_threshold" 

  ) as polluted_sector ON aggregate_pol.ind2 = polluted_sector.ind2 
  and aggregate_pol.year = polluted_sector.year 
  LEFT JOIN (
    SELECT 
      ind2, 
      year,
      geocode4_corr,
      polluted_d50_cit, 
      polluted_d75_cit, 
      polluted_d80_cit, 
      polluted_d85_cit, 
      polluted_d90_cit, 
      polluted_d95_cit, 
      polluted_m_cit
    FROM 
      "environment"."china_city_sector_year_pollution_threshold" 

  ) as polluted_sector_cit ON aggregate_pol.ind2 = polluted_sector_cit.ind2 
  and aggregate_pol.year = polluted_sector_cit.year 
  and aggregate_pol.geocode4_corr = polluted_sector_cit.geocode4_corr 
  LEFT JOIN (
    SELECT 
      cic, 
      financial_dep_china AS credit_constraint,
      financial_dep_us,
      liquidity_need_us,
      rd_intensity_us 
    FROM 
      industry.china_credit_constraint
  ) as cred_constraint ON aggregate_pol.ind2 = cred_constraint.cic 
  LEFT JOIN (
    SELECT 
      year, 
      geocode4_corr, 
      indu_2,
      SUM(output) AS output,
      SUM(employ) AS employment, 
      SUM(captal) AS capital,
      SUM(sales) as sales
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
  WHERE 
      year in (
      '1998', '1999', '2000',
        '2001', '2002', '2003', '2004', '2005', 
        '2006', '2007'
      ) 
    GROUP BY 
    year, 
      geocode4_corr, 
      indu_2
  ) as agg_output ON aggregate_pol.geocode4_corr = agg_output.geocode4_corr 
  AND aggregate_pol.ind2 = agg_output.indu_2 
  AND aggregate_pol.year = agg_output.year 
  LEFT JOIN firms_survey.asif_industry_characteristics_ownership
    ON aggregate_pol.geocode4_corr = asif_industry_characteristics_ownership.geocode4_corr
    AND aggregate_pol.ind2 = asif_industry_characteristics_ownership.indu_2
  LEFT JOIN firms_survey.asif_city_characteristics_ownership
    ON aggregate_pol.geocode4_corr = asif_city_characteristics_ownership.geocode4_corr
  LEFT JOIN chinese_lookup.province_credit_constraint ON aggregate_pol.province_en = province_credit_constraint.Province
  LEFT JOIN (
    SELECT geocode4_corr, avg_ij_o_city_mandate, d_avg_ij_o_city_mandate 
FROM "policy"."china_spatial_relocation"
    ) as relocation
    ON aggregate_pol.geocode4_corr = relocation.geocode4_corr
LEFT JOIN (
    SELECT 
  year, 
  province_en, 
  share_big_bank_loan, 
  LAG(share_big_bank_loan, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_share_big_bank_loan, 
  share_big_loan, 
  LAG(share_big_loan, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_share_big_loan, 
  credit_supply, 
  LAG(credit_supply, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_credit_supply, 
  credit_supply_long_term, 
  LAG(credit_supply_long_term, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_credit_supply_long_term, 
  credit_supply_short_term, 
  LAG(credit_supply_short_term, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_credit_supply_short_term 
FROM 
  (
    SELECT 
      province_loan_and_credit.year, 
      province_loan_and_credit.province_en, 
      CAST(
        total_loan_big_four AS DECIMAL(16, 5)
      )/ CAST(
        total_bank_loan AS DECIMAL(16, 5)
      ) AS share_big_bank_loan, 
      CAST(
        total_loan_big_four AS DECIMAL(16, 5)
      )/ CAST(
        total_loan AS DECIMAL(16, 5)
      ) AS share_big_loan, 
      CAST(
        total_bank_loan AS DECIMAL(16, 5)
      )/ CAST(
        total_gdp AS DECIMAL(16, 5)
      ) AS credit_supply, 
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
      LEFT JOIN (
        SELECT 
          province_en, 
          year, 
          share[ '中国农业' ] + share[ '中国银行' ] + share[ '中国工商' ] + share[ '中国建设' ] as total_loan_big_four 
        FROM 
          (
            SELECT 
              province_en, 
              year, 
              map_agg(bank, loan) as share 
            FROM 
              "almanac_bank_china"."bank_socb_loan" 
            GROUP BY 
              province_en, 
              year
          )
      ) as big_four ON province_loan_and_credit.year = big_four.year 
      AND province_loan_and_credit.province_en = big_four.province_en
    -- 
  )
  ) as credit ON aggregate_pol.year = credit.year 
  AND aggregate_pol.province_en = credit.province_en 
WHERE 
  tso2 > 0 
  AND output > 0 
  and capital > 0 
  and employment > 0
  and current_ratio > 0
  and cashflow_to_tangible > 0
  AND aggregate_pol.ind2 != '43'
  ORDER BY year
LIMIT 
  10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

# Table `fin_dep_pollution_baseline_industry`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/INDUSTRY'
table_name = 'fin_dep_pollution_baseline_industry'
```

First, we need to delete the table (if exist)

```python
try:
    response = glue.delete_table(
        database=DatabaseName,
        table=table_name
    )
    print(response)
except Exception as e:
    print(e)
```

Clean up the folder with the previous csv file. Be careful, it will erase all files inside the folder

```python
s3.remove_all_bucket(path_remove = s3_output)
```

```python
%%time
query = """
CREATE TABLE {0}.{1} WITH (format = 'PARQUET') AS
WITH aggregate_pol AS (
SELECT 
    year, 
    geocode4_corr, 
    TRIM(province_en) AS province_en, 
    cityen, 
    -- indus_code AS cic,
    ind2, 
    SUM(tso2) as tso2, 
    lower_location, 
    larger_location, 
    coastal 
  FROM 
    (
      SELECT 
        year, 
        province_en, 
        citycode, 
        geocode4_corr, 
        china_city_sector_pollution.cityen, 
        --indus_code,
        ind2,  
        tso2, 
        lower_location, 
        larger_location, 
        coastal 
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
    cityen, 
    --indus_code,
    ind2, 
    lower_location, 
    larger_location, 
    coastal
) 
SELECT 
  aggregate_pol.year, 
  CASE WHEN aggregate_pol.year in (
    '1998','1999','2000','2001', '2002', '2003', '2004', '2005'
  ) THEN 'FALSE' WHEN aggregate_pol.year in ('2006', '2007') THEN 'TRUE' END AS period, 
  aggregate_pol.province_en, 
  cityen, 
  aggregate_pol.geocode4_corr, 
  CASE WHEN tcz IS NULL THEN '0' ELSE tcz END AS tcz, 
  CASE WHEN spz IS NULL OR spz = '#N/A' THEN '0' ELSE spz END AS spz, 
  --aggregate_pol.cic, 
  aggregate_pol.ind2, 
  CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short, 
  polluted_d50i, 
  polluted_d75i, 
  polluted_d80i, 
  polluted_d85i, 
  polluted_d90i, 
  polluted_d95i, 
  polluted_mi,
  polluted_d50_cit, 
  polluted_d75_cit, 
  polluted_d80_cit, 
  polluted_d85_cit, 
  polluted_d90_cit, 
  polluted_d95_cit, 
  polluted_m_cit,
  tso2, 
  CAST(
    tso2 AS DECIMAL(16, 5)
  ) / CAST(
    output AS DECIMAL(16, 5)
  ) AS so2_intensity, 
  tso2_mandate_c,
  target_reduction_so2_p,
  above_threshold_mandate,
  above_average_mandate,
  avg_ij_o_city_mandate,
  CASE WHEN d_avg_ij_o_city_mandate IS NULL THEN 'FALSE' ELSE d_avg_ij_o_city_mandate END AS d_avg_ij_o_city_mandate,
  in_10_000_tonnes, 
  credit_constraint,
  financial_dep_us,
  liquidity_need_us,
  rd_intensity_us,
  1/supply_all_credit as supply_all_credit,
  1/supply_long_term_credit as supply_long_term_credit,
  share_big_bank_loan,
  lag_share_big_bank_loan,
  share_big_loan,
  lag_share_big_loan,
  credit_supply,
  lag_credit_supply,
  credit_supply_long_term,
  lag_credit_supply_long_term,
  credit_supply_short_term,
  lag_credit_supply_short_term,
  output,
  sales,
  employment,
  capital,
  current_asset,
  tofixed,
  total_liabilities,
  total_asset,
  tangible,
  cashflow,
  current_ratio,
  lag_current_ratio,
  liabilities_tot_asset,
  sales_tot_asset,
  lag_sales_tot_asset,
  asset_tangibility_tot_asset,
  lag_liabilities_tot_asset,
  cashflow_to_tangible,
  lag_cashflow_to_tangible,
  cashflow_tot_asset,
  lag_cashflow_tot_asset,
  return_to_sale,
  lag_return_to_sale,
  lower_location, 
  larger_location, 
  coastal,
  dominated_output_soe_c,
  dominated_employment_soe_c,
  dominated_sales_soe_c,
  dominated_capital_soe_c,
  dominated_output_for_c,
  dominated_employment_for_c,
  dominated_sales_for_c,
  dominated_capital_for_c,  
  dominated_output_i,
  dominated_employ_i,
  dominated_sales_i,
  dominated_capital_i,
  dominated_output_soe_i,
  dominated_employment_soe_i,
  dominated_sales_soe_i,
  dominated_capital_soe_i,
  dominated_output_for_i,
  dominated_employment_for_i,
  dominated_sales_for_i,
  dominated_capital_for_i,  
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      aggregate_pol.ind2
  ) AS fe_c_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      aggregate_pol.year, 
      aggregate_pol.ind2
  ) AS fe_t_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      aggregate_pol.year
  ) AS fe_c_t 
FROM 
  aggregate_pol 
  INNER JOIN (
  SELECT 
  indu_2,
  current_asset,
  tofixed,
  total_liabilities,
  total_asset,
  tangible,
  cashflow,
  current_ratio,
  lag_current_ratio,
  liabilities_tot_asset,
  sales_tot_asset,
  lag_sales_tot_asset,
  asset_tangibility_tot_asset,
  lag_liabilities_tot_asset,
  cashflow_to_tangible,
  lag_cashflow_to_tangible,
  cashflow_tot_asset,
  lag_cashflow_tot_asset,
  return_to_sale,
  lag_return_to_sale
    
  FROM firms_survey.asif_industry_financial_ratio_industry 
  WHERE year = '2002'
  ) AS asif_industry_financial_ratio_industry
  ON 
  aggregate_pol.ind2 = asif_industry_financial_ratio_industry.indu_2 
  INNER JOIN (
    SELECT 
  geocode4_corr, 
  tso2_mandate_c, 
  target_reduction_so2_p,
  in_10_000_tonnes, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      transform(
        sequence(1, 4), 
        x -> tso2_mandate_c
      ), 
      tso2_mandate_c_pct, 
      (x, y) -> x < y
    )
  ) AS above_threshold_mandate, 
  CASE WHEN tso2_mandate_c > tso2_mandate_c_avg THEN 'ABOVE' ELSE 'BELOW' END AS above_average_mandate 
FROM 
  (
    (
      SELECT 
        'TEMP' as temp, 
        approx_percentile(
          tso2_mandate_c, ARRAY[.5,.75,.90, 
          .95]
        ) AS tso2_mandate_c_pct, 
        AVG(tso2_mandate_c) AS tso2_mandate_c_avg 
      FROM 
        policy.china_city_reduction_mandate
    ) as percentile 
    LEFT JOIN (
      SELECT 
        'TEMP' as temp, 
        citycn, 
        tso2_mandate_c, 
        target_reduction_so2_p,
        in_10_000_tonnes 
      FROM 
        policy.china_city_reduction_mandate
    ) as mandate ON percentile.temp = mandate.temp
  ) as map_mandate
  INNER JOIN chinese_lookup.china_city_code_normalised ON map_mandate.citycn = china_city_code_normalised.citycn 
    WHERE 
      extra_code = geocode4_corr
  ) as city_mandate ON aggregate_pol.geocode4_corr = city_mandate.geocode4_corr 
  LEFT JOIN policy.china_city_tcz_spz ON aggregate_pol.geocode4_corr = china_city_tcz_spz.geocode4_corr 
  LEFT JOIN chinese_lookup.ind_cic_2_name ON aggregate_pol.ind2 = ind_cic_2_name.cic 
  LEFT JOIN (
    SELECT 
      ind2, 
      year,
      polluted_d50i, 
      polluted_d75i, 
      polluted_d80i, 
      polluted_d85i, 
      polluted_d90i, 
      polluted_d95i, 
      polluted_mi
    FROM 
      "environment"."china_sector_pollution_threshold" 

  ) as polluted_sector ON aggregate_pol.ind2 = polluted_sector.ind2 
  and aggregate_pol.year = polluted_sector.year 
  LEFT JOIN (
    SELECT 
      ind2, 
      year,
      geocode4_corr,
      polluted_d50_cit, 
      polluted_d75_cit, 
      polluted_d80_cit, 
      polluted_d85_cit, 
      polluted_d90_cit, 
      polluted_d95_cit, 
      polluted_m_cit
    FROM 
      "environment"."china_city_sector_year_pollution_threshold" 

  ) as polluted_sector_cit ON aggregate_pol.ind2 = polluted_sector_cit.ind2 
  and aggregate_pol.year = polluted_sector_cit.year 
  and aggregate_pol.geocode4_corr = polluted_sector_cit.geocode4_corr 
  LEFT JOIN (
    SELECT 
      cic, 
      financial_dep_china AS credit_constraint,
      financial_dep_us,
      liquidity_need_us,
      rd_intensity_us 
    FROM 
      industry.china_credit_constraint
  ) as cred_constraint ON aggregate_pol.ind2 = cred_constraint.cic 
  LEFT JOIN (
    SELECT 
      year, 
      geocode4_corr, 
      indu_2,
      SUM(output) AS output,
      SUM(employ) AS employment, 
      SUM(captal) AS capital,
      SUM(sales) as sales
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
  WHERE 
      year in (
      '1998', '1999', '2000',
        '2001', '2002', '2003', '2004', '2005', 
        '2006', '2007'
      ) 
    GROUP BY 
    year, 
      geocode4_corr, 
      indu_2
  ) as agg_output ON aggregate_pol.geocode4_corr = agg_output.geocode4_corr 
  AND aggregate_pol.ind2 = agg_output.indu_2 
  AND aggregate_pol.year = agg_output.year 
  LEFT JOIN firms_survey.asif_industry_characteristics_ownership
    ON aggregate_pol.geocode4_corr = asif_industry_characteristics_ownership.geocode4_corr
    AND aggregate_pol.ind2 = asif_industry_characteristics_ownership.indu_2
  LEFT JOIN firms_survey.asif_city_characteristics_ownership
    ON aggregate_pol.geocode4_corr = asif_city_characteristics_ownership.geocode4_corr
  LEFT JOIN (
  SELECT TRIM(province) AS province, 
  supply_all_credit, supply_long_term_credit
  FROM chinese_lookup.province_credit_constraint) as province_credit_constraint
  ON aggregate_pol.province_en = province_credit_constraint.Province
  LEFT JOIN (
    SELECT geocode4_corr, avg_ij_o_city_mandate, d_avg_ij_o_city_mandate 
FROM "policy"."china_spatial_relocation"
    ) as relocation
    ON aggregate_pol.geocode4_corr = relocation.geocode4_corr
LEFT JOIN (
    SELECT 
  year, 
  province_en, 
  share_big_bank_loan, 
  LAG(share_big_bank_loan, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_share_big_bank_loan, 
  share_big_loan, 
  LAG(share_big_loan, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_share_big_loan, 
  credit_supply, 
  LAG(credit_supply, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_credit_supply, 
  credit_supply_long_term, 
  LAG(credit_supply_long_term, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_credit_supply_long_term, 
  credit_supply_short_term, 
  LAG(credit_supply_short_term, 1) OVER (
    PARTITION BY province_en 
    ORDER BY 
      year
  ) as lag_credit_supply_short_term 
FROM 
  (
    SELECT 
      province_loan_and_credit.year, 
      province_loan_and_credit.province_en, 
      CAST(
        total_loan_big_four AS DECIMAL(16, 5)
      )/ CAST(
        total_bank_loan AS DECIMAL(16, 5)
      ) AS share_big_bank_loan, 
      CAST(
        total_loan_big_four AS DECIMAL(16, 5)
      )/ CAST(
        total_loan AS DECIMAL(16, 5)
      ) AS share_big_loan, 
      CAST(
        total_bank_loan AS DECIMAL(16, 5)
      )/ CAST(
        total_gdp AS DECIMAL(16, 5)
      ) AS credit_supply, 
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
      LEFT JOIN (
        SELECT 
          province_en, 
          year, 
          share[ '中国农业' ] + share[ '中国银行' ] + share[ '中国工商' ] + share[ '中国建设' ] as total_loan_big_four 
        FROM 
          (
            SELECT 
              province_en, 
              year, 
              map_agg(bank, loan) as share 
            FROM 
              "almanac_bank_china"."bank_socb_loan" 
            GROUP BY 
              province_en, 
              year
          )
      ) as big_four ON province_loan_and_credit.year = big_four.year 
      AND province_loan_and_credit.province_en = big_four.province_en
  )
  ) as credit ON aggregate_pol.year = credit.year 
  AND aggregate_pol.province_en = credit.province_en 
WHERE 
  tso2 > 0 
  AND output > 0 
  and capital > 0 
  and employment > 0
  and current_ratio > 0
  and cashflow_to_tangible > 0
  AND aggregate_pol.ind2 != '43'
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output,
                )
output
```

```python
query_count = """
SELECT COUNT(*) AS CNT
FROM {}.{} 
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query_count,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

Check if missing share big four in 2005 to 2007

```python
query_big = """
SELECT year, COUNT(*) as count_missing
FROM environment.fin_dep_pollution_baseline_industry 
WHERE share_big_loan IS NULL
GROUP BY year
ORDER BY year
"""
output = s3.run_query(
                    query=query_big,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

# Validate query

This step is mandatory to validate the query in the ETL. If you are not sure about the quality of the query, go to the next step.


To validate the query, please fillin the json below. Don't forget to change the schema so that the crawler can use it.

1. Change the schema if needed. It is highly recommanded to add comment to the fields
2. Add a partition key:
    - Inform if there is group in the table so that, the parser can compute duplicate
3. Provide a description -> detail the steps 


1. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database = DatabaseName,
    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [{'Name': 'year', 'Type': 'string', 'Comment': 'year from 2001 to 2007'},
          {'Name': 'period',
              'Type': 'varchar(5)', 'Comment': 'False if year before 2005 included, True if year 2006 and 2007'},
          {'Name': 'province_en', 'Type': 'string', 'Comment': ''},
          {'Name': 'cityen', 'Type': 'string', 'Comment': ''},
          {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': ''},
          {'Name': 'tcz', 'Type': 'string',
              'Comment': 'Two control zone policy city'},
          {'Name': 'spz', 'Type': 'string',
              'Comment': 'Special policy zone policy city'},
          {'Name': 'ind2', 'Type': 'string', 'Comment': '2 digits industry'},
          {'Name': 'short', 'Type': 'string', 'Comment': ''},
          {'Name': 'polluted_d50i',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 50th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d80i',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 80th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d85i',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 85th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d90i',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 90th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d95i',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 95th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_mi',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly average of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d50_cit',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 50th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d80_cit',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 80th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d85_cit',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 85th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d90_cit',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 90th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_d95_cit',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 95th percentile of SO2 label as ABOVE else BELOW'},
          {'Name': 'polluted_m_cit',
           'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly average of SO2 label as ABOVE else BELOW'},
          {'Name': 'tso2', 'Type': 'bigint',
           'Comment': 'Total so2 city sector. Filtered values above  4863 (5% of the distribution)'},
          {'Name': 'so2_intensity',
           'Type': 'decimal(21,5)', 'Comment': 'SO2 divided by output'},
          {'Name': 'tso2_mandate_c', 'Type': 'float',
           'Comment': 'city reduction mandate in tonnes'},
          {'Name': 'target_reduction_so2_p', 'Type': 'float',
           'Comment': 'official province reduction mandate in percentage. From https://www.sciencedirect.com/science/article/pii/S0095069617303522#appsec1'},
          {'Name': 'above_threshold_mandate',
           'Type': 'map<double,boolean>',
           'Comment': 'Policy mandate above percentile .5, .75, .9, .95'},
          {'Name': 'avg_ij_o_city_mandate', 'Type': 'float', 'Comment': ''},
          {'Name': 'd_avg_ij_o_city_mandate', 'Type': 'string', 'Comment': ''},
          {'Name': 'above_average_mandate',
           'Type': 'varchar(5)', 'Comment': 'Policy mandate above national average'},
          {'Name': 'in_10_000_tonnes', 'Type': 'float',
           'Comment': 'city reduction mandate in 10k tonnes'},
          {'Name': 'tfp_cit', 'Type': 'double', 'Comment': 'TFP at the city industry level. From https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md#table-asif_tfp_firm_level'},
          {'Name': 'credit_constraint', 'Type': 'float',
           'Comment': 'Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311"'},
          {'Name': 'financial_dep_us', 'Type': 'float', 'Comment': 'Financial dependency metric based on US data'},
          {'Name': 'liquidity_need_us', 'Type': 'float', 'Comment': 'liquidity need metric based on US data'},
          {'Name': 'rd_intensity_us', 'Type': 'float', 'Comment': 'RD intensity metric based on US data'},
          {'Name': 'supply_all_credit', 'Type': 'double',
           'Comment': 'province external supply of credit'},
          {'Name': 'supply_long_term_credit', 'Type': 'float',
           'Comment': 'province external supply of long term credit'},
          {'Name': 'share_big_bank_loan',
           'Type': 'decimal(21,5)', 'Comment': 'share four State-owned commercial banks in total bank lending'},
          {'Name': 'lag_share_big_bank_loan',
           'Type': 'decimal(21,5)', 'Comment': 'lag share four State-owned commercial banks in total bank lending'},
          {'Name': 'share_big_loan',
           'Type': 'decimal(21,5)', 'Comment': 'share four State-owned commercial banks in total lending'},
          {'Name': 'lag_share_big_loan',
           'Type': 'decimal(21,5)', 'Comment': 'lag share four State-owned commercial banks in total lending'},
          {'Name': 'credit_supply',
           'Type': 'decimal(21,5)', 'Comment': 'total bank lending normalised by gdp'},
          {'Name': 'lag_credit_supply',
           'Type': 'decimal(21,5)', 'Comment': 'lag total bank lending normalised by gdp'},
          {'Name': 'credit_supply_long_term',
           'Type': 'decimal(21,5)', 'Comment': 'total long term bank lending normalised by gdp'},
          {'Name': 'lag_credit_supply_long_term',
           'Type': 'decimal(21,5)', 'Comment': 'lag total long term bank lending normalised by gdp'},
          {'Name': 'credit_supply_short_term',
           'Type': 'decimal(21,5)', 'Comment': 'total short term bank lending normalised by gdp'},
          {'Name': 'lag_credit_supply_short_term',
           'Type': 'decimal(21,5)', 'Comment': 'lag total short term bank lending normalised by gdp'},
          {'Name': 'output', 'Type': 'bigint', 'Comment': 'Output'},
          {'Name': 'sales', 'Type': 'bigint', 'Comment': 'Sales'},
          {'Name': 'employment', 'Type': 'bigint', 'Comment': 'Employemnt'},
          {'Name': 'capital', 'Type': 'bigint', 'Comment': 'Capital'},
          {'Name': 'current_asset', 'Type': 'bigint', 'Comment': 'current asset'},
          {'Name': 'tofixed', 'Type': 'bigint', 'Comment': 'total fixed asset'},
          {'Name': 'total_liabilities', 'Type': 'bigint',
              'Comment': 'total liabilities'},
          {'Name': 'total_asset', 'Type': 'bigint', 'Comment': 'total asset'},
          {'Name': 'tangible', 'Type': 'bigint', 'Comment': 'tangible asset'},
          {'Name': 'cashflow', 'Type': 'bigint', 'Comment': 'cash flow'},
          {'Name': 'current_ratio',
           'Type': 'decimal(21,5)', 'Comment': 'current ratio'},
          {'Name': 'lag_current_ratio',
           'Type': 'decimal(21,5)', 'Comment': 'lag value of current ratio'},
          {'Name': 'liabilities_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'liabilities to total asset'},
          {'Name': 'sales_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'sales to total asset'},
          {'Name': 'lag_sales_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'lag value of sales to asset'},
          {'Name': 'asset_tangibility_tot_asset',
           'Type': 'decimal(21,5)',
           'Comment': 'asset tangibility tot total asset'},
          {'Name': 'lag_liabilities_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'Lag liabiliteies to total asset'},
          {'Name': 'cashflow_to_tangible',
           'Type': 'decimal(21,5)', 'Comment': 'cash flow to tangible'},
          {'Name': 'lag_cashflow_to_tangible',
           'Type': 'decimal(21,5)', 'Comment': 'lag cash flow to tangible'},
          {'Name': 'cashflow_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'Cash flow to total asset'},
          {'Name': 'lag_cashflow_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'lag cash flow tot total asset'},
          {'Name': 'return_to_sale',
           'Type': 'decimal(21,5)', 'Comment': 'Return to sale'},
          {'Name': 'lag_return_to_sale',
           'Type': 'decimal(21,5)', 'Comment': 'Lag return tot sale'},
          {'Name': 'lower_location', 'Type': 'string', 'Comment': ''},
          {'Name': 'larger_location', 'Type': 'string', 'Comment': ''},
          {'Name': 'coastal', 'Type': 'string',
           'Comment': 'City is bordered by sea or not'},
          {'Name': 'dominated_output_soe_c', 'Type': 'boolean',
           'Comment': 'SOE dominated city of output. If true, then SOEs dominated city'},
          {'Name': 'dominated_employment_soe_c', 'Type': 'boolean',
           'Comment': 'SOE dominated city of employment. If true, then SOEs dominated city'},
          {'Name': 'dominated_sales_soe_c', 'Type': 'boolean',
           'Comment': 'SOE dominated city of sales. If true, then SOEs dominated city'},
          {'Name': 'dominated_capital_soe_c',
           'Type': 'boolean',
           'Comment': 'SOE dominated city of capital. If true, then SOEs dominated city'},
          {'Name': 'dominated_output_for_c',
           'Type': 'boolean',
           'Comment': 'foreign dominated city of output. If true, then foreign dominated city'},
          {'Name': 'dominated_employment_for_c',
           'Type': 'boolean',
           'Comment': 'foreign dominated city of employment. If true, then foreign dominated city'},
          {'Name': 'dominated_sales_for_c', 'Type': 'boolean',
           'Comment': 'foreign dominated cityof sales. If true, then foreign dominated city'},
          {'Name': 'dominated_capital_for_c',
           'Type': 'boolean',
           'Comment': 'foreign dominated city of capital. If true, then foreign dominated city'},
          {
    "Name": "dominated_output_i",
    "Type": "map<double,boolean>",
    "Comment": "map with information dominated industry knowing percentile .5, .75, .9, .95 of output",
},
    {"Name": "dominated_employment_i", "Type": "map<double,boolean>",
        "Comment": "map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment"},
    {"Name": "dominated_capital_i", "Type": "map<double,boolean>",
        "Comment": "map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital"},
    {"Name": "dominated_sales_i", "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales"},
    {
        "Name": "dominated_output_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output",
},
    {
        "Name": "dominated_employment_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment",
},
    {
        "Name": "dominated_sales_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales",
},
    {
        "Name": "dominated_capital_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital",
},
    {
        "Name": "dominated_output_for_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output",
},
    {
        "Name": "dominated_employment_for_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment",
},
    {
        "Name": "dominated_sales_for_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales",
},
    {
        "Name": "dominated_capital_for_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of capital",
},
    {"Name": "fe_c_i", "Type": "bigint", "Comment": "City industry fixed effect"},
    {"Name": "fe_t_i", "Type": "bigint", "Comment": "year industry fixed effect"},
    {"Name": "fe_c_t", "Type": "bigint", "Comment": "city industry fixed effect"}]
```

2. Provide a description

```python
description = """
Transform fin dep pollution baseline industry table by merging china_city_reduction_mandate and others
variables. Based on https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/04_fin_dep_pol_baseline_city.md
"""
```

3. provide metadata

- DatabaseName:
- TablePrefix:
- input: 
- filename: Name of the notebook or Python script: to indicate
- Task ID: from Coda
- index_final_table: a list to indicate if the current table is used to prepare the final table(s). If more than one, pass the index. Start at 0
- if_final: A boolean. Indicates if the current table is the final table -> the one the model will be used to be trained

```python
name_json = 'parameters_ETL_pollution_credit_constraint.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```python
with open(path_json) as json_file:
    parameters = json.load(json_file)
```

```python
partition_keys = ["province_en", "geocode4_corr","ind2", "year"]
notebookname =  "00_credit_constraint_industry.ipynb"
index_final_table = [0]
if_final = 'True'
```

```python
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    "blob/master",
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name'])
               , '', path))[1:],
    re.sub('.ipynb','.md',notebookname)
)
```

Grab the input name from query

```python
list_input = []
tables = glue.get_tables(full_output = False)
regex_matches = re.findall(r'(?=\.).*?(?=\s)|(?=\.\").*?(?=\")', query)
for i in regex_matches:
    cleaning = i.lstrip().rstrip().replace('.', '').replace('"', '')
    if cleaning in tables and cleaning != table_name:
        list_input.append(cleaning)
```

```python
json_etl = {
    'description': description,
    'query': query,
    'schema': schema,
    'partition_keys': partition_keys,
    'metadata': {
        'DatabaseName': DatabaseName,
        'TableName': table_name,
        'input': list_input,
        'target_S3URI': os.path.join('s3://', bucket, s3_output),
        'from_athena': 'True',
        'filename': notebookname,
        'index_final_table' : index_final_table,
        'if_final': if_final,
         'github_url':github_url
    }
}
json_etl['metadata']
```

**Chose carefully PREPARATION or TRANSFORMATION**

```python
index_to_remove = next(
                (
                    index
                    for (index, d) in enumerate(parameters['TABLES']['TRANSFORMATION']['STEPS'])
                    if d['metadata']['TableName'] == table_name
                ),
                None,
            )
if index_to_remove != None:
    parameters['TABLES']['TRANSFORMATION']['STEPS'].pop(index_to_remove)
parameters['TABLES']['TRANSFORMATION']['STEPS'].append(json_etl)
```

```python
print("Currently, the ETL has {} tables".format(len(parameters['TABLES']['TRANSFORMATION']['STEPS'])))
```

Save JSON

```python
with open(path_json, "w") as json_file:
    json.dump(parameters, json_file)
```

# Create or update the data catalog

The query is saved in the S3 (bucket `datalake-datascience`) but the table is not available yet in the Data Catalog. Use the function `create_table_glue` to generate the table and update the catalog.

Few parameters are required:

- name_crawler: Name of the crawler
- Role: Role to temporary provide an access tho the service
- DatabaseName: Name of the database to create the table
- TablePrefix: Prefix of the table. Full name of the table will be `TablePrefix` + folder name

To update the schema, please use the following structure

```
schema = [
    {
        "Name": "VAR1",
        "Type": "",
        "Comment": ""
    },
    {
        "Name": "VAR2",
        "Type": "",
        "Comment": ""
    }
]
```

```python
glue.update_schema_table(
    database = DatabaseName,
    table = table_name,
    schema= schema)
```

## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 

```python
#partition_keys = []

with open(path_json) as json_file:
    parameters = json.load(json_file)
```

```python
### COUNT DUPLICATES
if len(partition_keys) > 0:
    groups = ' , '.join(partition_keys)

    query_duplicates = parameters["ANALYSIS"]['COUNT_DUPLICATES']['query'].format(
                                DatabaseName,table_name,groups
                                )
    dup = s3.run_query(
                                query=query_duplicates,
                                database=DatabaseName,
                                s3_output="SQL_OUTPUT_ATHENA",
                                filename="duplicates_{}".format(table_name))
    display(dup)

```

## Count missing values

```python
#table = 'XX'
schema = glue.get_table_information(
    database = DatabaseName,
    table = table_name
)['Table']
```

```python
from datetime import date
today = date.today().strftime('%Y%M%d')
```

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    DatabaseName, table_name
)

for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

        table_middle += "{} ".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
    else:
        table_middle += "{} ,".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
query = table_top + table_middle + table_bottom
output = s3.run_query(
    query=query,
    database=DatabaseName,
    s3_output="SQL_OUTPUT_ATHENA",
    filename="count_missing",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
display(
    output.T.rename(columns={0: "total_missing"})
    .assign(total_missing_pct=lambda x: x["total_missing"] / x.iloc[0, 0])
    .sort_values(by=["total_missing"], ascending=False)
    .style.format("{0:,.2%}", subset=["total_missing_pct"])
    .bar(subset="total_missing_pct", color=["#d65f5f"])
)
```

# Update Github Data catalog

The data catalog is available in Glue. Although, we might want to get a quick access to the tables in Github. In this part, we are generating a `README.md` in the folder `00_data_catalogue`. All tables used in the project will be added to the catalog. We use the ETL parameter file and the schema in Glue to create the README. 

Bear in mind the code will erase the previous README. 

```python
README = """
# Data Catalogue

{}

    """

top_readme = """

## Table of Content

    """

template = """

## Table {0}

- Database: {1}
- S3uri: `{2}`
- Partitition: {3}
- Script: {5}

{4}

    """
github_link = os.path.join("https://github.com/", parameters['GLOBAL']['GITHUB']['owner'],
                           parameters['GLOBAL']['GITHUB']['repo_name'], "tree/master/00_data_catalog#table-")
for key, value in parameters['TABLES'].items():
    if key == 'CREATION':
        param = 'ALL_SCHEMA'
    else:
        param = 'STEPS'
        
    for schema in parameters['TABLES'][key][param]:
        description = schema['description']
        DatabaseName = schema['metadata']['DatabaseName']
        target_S3URI = schema['metadata']['target_S3URI']
        partition = schema['partition_keys']
        script = schema['metadata']['github_url']
        
        if param =='ALL_SCHEMA':
            table_name_git = '{}{}'.format(
                schema['metadata']['TablePrefix'],
                os.path.basename(schema['metadata']['target_S3URI']).lower()
            )
        else:
            try:
                table_name_git = schema['metadata']['TableName']
            except:
                table_name_git = '{}{}'.format(
                schema['metadata']['TablePrefix'],
                os.path.basename(schema['metadata']['target_S3URI']).lower()
            )
        
        tb = pd.json_normalize(schema['schema']).to_markdown()
        toc = "{}{}".format(github_link, table_name_git)
        top_readme += '\n- [{0}]({1})'.format(table_name_git, toc)

        README += template.format(table_name_git,
                                  DatabaseName,
                                  target_S3URI,
                                  partition,
                                  tb,
                                  script
                                  )
README = README.format(top_readme)
with open(os.path.join(str(Path(path).parent.parent), '00_data_catalog/README.md'), "w") as outfile:
    outfile.write(README)
```

# Analytics

In this part, we are providing basic summary statistic. Since we have created the tables, we can parse the schema in Glue and use our json file to automatically generates the analysis.

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 


For a full analysis of the table, please use the following Lambda function. Be patient, it can takes between 5 to 30 minutes. Times varies according to the number of columns in your dataset.

Use the function as follow:

- `output_prefix`:  s3://datalake-datascience/ANALYTICS/OUTPUT/TABLE_NAME/
- `region`: region where the table is stored
- `bucket`: Name of the bucket
- `DatabaseName`: Name of the database
- `table_name`: Name of the table
- `group`: variables name to group to count the duplicates
- `primary_key`: Variable name to perform the grouping -> Only one variable for now
- `secondary_key`: Variable name to perform the secondary grouping -> Only one variable for now
- `proba`: Chi-square analysis probabilitity
- `y_var`: Continuous target variables

Check the job processing in Sagemaker: https://eu-west-3.console.aws.amazon.com/sagemaker/home?region=eu-west-3#/processing-jobs

The notebook is available: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience?region=eu-west-3&prefix=ANALYTICS/OUTPUT/&showversions=false

Please, download the notebook on your local machine, and convert it to HTML:

```
cd "/Users/thomas/Downloads/Notebook"
aws s3 cp s3://datalake-datascience/ANALYTICS/OUTPUT/asif_unzip_data_csv/Template_analysis_from_lambda-2020-11-22-08-12-20.ipynb .

## convert HTML no code
jupyter nbconvert --no-input --to html Template_analysis_from_lambda-2020-11-21-14-30-45.ipynb
jupyter nbconvert --to html Template_analysis_from_lambda-2020-11-22-08-12-20.ipynb
```

Then upload the HTML to: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience?region=eu-west-3&prefix=ANALYTICS/HTML_OUTPUT/

Add a new folder with the table name in upper case

```python
import boto3

key, secret_ = con.load_credential()
client_lambda = boto3.client(
    'lambda',
    aws_access_key_id=key,
    aws_secret_access_key=secret_,
    region_name = region)
```

```python
primary_key = 'year'
secondary_key = 'period'
y_var = 'tso2'
```

```python
payload = {
    "input_path": "s3://datalake-datascience/ANALYTICS/TEMPLATE_NOTEBOOKS/template_analysis_from_lambda.ipynb",
    "output_prefix": "s3://datalake-datascience/ANALYTICS/OUTPUT/{}/".format(table_name.upper()),
    "parameters": {
        "region": "{}".format(region),
        "bucket": "{}".format(bucket),
        "DatabaseName": "{}".format(DatabaseName),
        "table_name": "{}".format(table_name),
        "group": "{}".format(','.join(partition_keys)),
        "keys": "{},{}".format(primary_key,secondary_key),
        "y_var": "{}".format(y_var),
        "threshold":0.5
    },
}
payload
```

```python
response = client_lambda.invoke(
    FunctionName='RunNotebook',
    InvocationType='RequestResponse',
    LogType='Tail',
    Payload=json.dumps(payload),
)
response
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
import sys
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
import create_schema
import create_report
```

```python
create_report.create_report(extension = "html", keep_code = True, notebookname =  notebookname)
```

```python
create_schema.create_schema(path_json, path_save_image = os.path.join(parent_path, 'utils'))
```

```python
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          os.path.join(str(Path(path).parent), "00_download_data"),
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
