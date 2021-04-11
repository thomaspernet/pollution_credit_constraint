
# Data Catalogue



## Table of Content

    
- [china_spatial_relocation](https://github.com/thomaspernet/pollution_credit_constraint/tree/master/00_data_catalog#table-china_spatial_relocation)
- [bank_socb_loan](https://github.com/thomaspernet/pollution_credit_constraint/tree/master/00_data_catalog#table-bank_socb_loan)
- [province_loan_and_credit](https://github.com/thomaspernet/pollution_credit_constraint/tree/master/00_data_catalog#table-province_loan_and_credit)
- [fin_dep_pollution_baseline_industry](https://github.com/thomaspernet/pollution_credit_constraint/tree/master/00_data_catalog#table-fin_dep_pollution_baseline_industry)

    

## Table china_spatial_relocation

- Database: policy
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/SPATIAL_RELOCATION`
- Partitition: []
- Script: https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/TARGET_SO2/china_cities_target_so2.py

|    | Name                    | Type   | Comment   |
|---:|:------------------------|:-------|:----------|
|  0 | citycn                  | float  |           |
|  1 | geocode4_corr           | float  |           |
|  2 | d_avg_ij_o_city_mandate | float  |           |
|  3 | avg_ij_o_city_mandate   | float  |           |

    

## Table bank_socb_loan

- Database: almanac_bank_china
- S3uri: `s3://datalake-datascience/DATA/ECON/ALMANAC_OF_CHINA_FINANCE_BANKING/PROVINCES/SOCB_LOAN`
- Partitition: []
- Script: https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/ALMANAC_BANK_LOAN/socb_loan.py

|    | Name                | Type   | Comment                                                              |
|---:|:--------------------|:-------|:---------------------------------------------------------------------|
|  0 | year                | string | year                                                                 |
|  1 | abc_doc             | string | google drive source file for abc                                     |
|  2 | icbc_doc            | string | google drive source file for icbc                                    |
|  3 | ccb_doc             | string | google drive source file for ccb                                     |
|  4 | boc_doc             | string | google drive source file for boc                                     |
|  5 | province_cn         | string | province chinese name                                                |
|  6 | province_en         | string | province english name                                                |
|  7 | abc_loan            | float  | loan by abc 中国农业发展银行各地区贷款余额 in 亿                     |
|  8 | icbc_loan           | float  | loan by icbc 中国工商银行各地区人民币存款贷款余额 各项贷款合计 in 亿 |
|  9 | ccb_loan            | float  | loan by ccb 中国建设银行各地区人民币存款 贷款余额 各项贷款合计 in 亿 |
| 10 | boc_loan            | float  | loan by boc 中国银行各地区人民币存款贷款余额 各项贷款合计 in 亿      |
| 11 | total_loan_big_four | float  | abc_loan + icbc_loan + ccb_loan + boc_loan                           |
| 12 | abc_metric          | string | metric display either 亿 or 万                                       |
| 13 | icbc_metric         | string | metric display either 亿 or 万                                       |
| 14 | ccb_metric          | string | metric display either 亿 or 万                                       |
| 15 | boc_metric          | string | metric display either 亿 or 万                                       |

    

## Table province_loan_and_credit

- Database: almanac_bank_china
- S3uri: `s3://datalake-datascience/DATA/ECON/ALMANAC_OF_CHINA_FINANCE_BANKING/PROVINCES/LOAN_AND_CREDIT`
- Partitition: []
- Script: https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/ALMANAC_BANK_LOAN/provinces.py

|    | Name                    | Type   | Comment                                                                     |
|---:|:------------------------|:-------|:----------------------------------------------------------------------------|
|  0 | province_cn             | string | province chinese name                                                       |
|  1 | province_en             | string | province english name                                                       |
|  2 | url                     | string | google drive source file                                                    |
|  3 | year                    | string | year                                                                        |
|  4 | total_deposit           | float  | All deposits of financial institutions (balance) 全部金融机构各项存款(余额) |
|  5 | total_loan              | float  | All financial institutions loans (balance) 全部金融机构各项贷款(余额)       |
|  6 | total_bank_loan         | float  | Bank loan 银行贷款                                                          |
|  7 | total_short_term        | float  | total Of which short-term loans 其中 短期贷款                               |
|  8 | total_long_term_loan    | float  | total Of Long-term loans 中长期贷款                                         |
|  9 | total_gdp               | float  | GDP province                                                                |
| 10 | metric                  | string | metric display either 亿 or 万                                              |
| 11 | cn_total_deposit        | string | Values in source spreadsheet                                                |
| 12 | cn_total_loan           | string | Values in source spreadsheet                                                |
| 13 | cn_total_bank_loan      | string | Values in source spreadsheet                                                |
| 14 | cn_total_short_term     | string | Values in source spreadsheet                                                |
| 15 | cn_total_long_term_loan | string | Values in source spreadsheet                                                |
| 16 | cn_total_gdp            | string | Values in source spreadsheet                                                |

    

## Table fin_dep_pollution_baseline_industry

- Database: environment
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/INDUSTRY`
- Partitition: ['province_en', 'geocode4_corr', 'ind2', 'year']
- Script: https://github.com/thomaspernet/pollution_credit_constraint/blob/master/01_data_preprocessing/02_transform_tables/00_credit_constraint_industry.md

|    | Name                         | Type                | Comment                                                                                                                                                                                                   |
|---:|:-----------------------------|:--------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | year                         | string              | year from 2001 to 2007                                                                                                                                                                                    |
|  1 | period                       | varchar(5)          | False if year before 2005 included, True if year 2006 and 2007                                                                                                                                            |
|  2 | province_en                  | string              |                                                                                                                                                                                                           |
|  3 | cityen                       | string              |                                                                                                                                                                                                           |
|  4 | geocode4_corr                | string              |                                                                                                                                                                                                           |
|  5 | tcz                          | string              | Two control zone policy city                                                                                                                                                                              |
|  6 | spz                          | string              | Special policy zone policy city                                                                                                                                                                           |
|  7 | ind2                         | string              | 2 digits industry                                                                                                                                                                                         |
|  8 | short                        | string              |                                                                                                                                                                                                           |
|  9 | polluted_d50i                | varchar(5)          | Sectors with values above Yearly 50th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 10 | polluted_d80i                | varchar(5)          | Sectors with values above Yearly 80th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 11 | polluted_d85i                | varchar(5)          | Sectors with values above Yearly 85th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 12 | polluted_d90i                | varchar(5)          | Sectors with values above Yearly 90th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 13 | polluted_d95i                | varchar(5)          | Sectors with values above Yearly 95th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 14 | polluted_mi                  | varchar(5)          | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW                                                                                                                                 |
| 15 | polluted_d50_cit             | varchar(5)          | Sectors with values above Yearly 50th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 16 | polluted_d80_cit             | varchar(5)          | Sectors with values above Yearly 80th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 17 | polluted_d85_cit             | varchar(5)          | Sectors with values above Yearly 85th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 18 | polluted_d90_cit             | varchar(5)          | Sectors with values above Yearly 90th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 19 | polluted_d95_cit             | varchar(5)          | Sectors with values above Yearly 95th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 20 | polluted_m_cit               | varchar(5)          | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW                                                                                                                                 |
| 21 | tso2                         | bigint              | Total so2 city sector. Filtered values above  4863 (5% of the distribution)                                                                                                                               |
| 22 | so2_intensity                | decimal(21,5)       | SO2 divided by output                                                                                                                                                                                     |
| 23 | tso2_mandate_c               | float               | city reduction mandate in tonnes                                                                                                                                                                          |
| 24 | above_threshold_mandate      | map<double,boolean> | Policy mandate above percentile .5, .75, .9, .95                                                                                                                                                          |
| 25 | avg_ij_o_city_mandate        | float               |                                                                                                                                                                                                           |
| 26 | d_avg_ij_o_city_mandate      | string              |                                                                                                                                                                                                           |
| 27 | above_average_mandate        | varchar(5)          | Policy mandate above national average                                                                                                                                                                     |
| 28 | in_10_000_tonnes             | float               | city reduction mandate in 10k tonnes                                                                                                                                                                      |
| 29 | tfp_cit                      | double              | TFP at the city industry level. From https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md#table-asif_tfp_firm_level |
| 30 | credit_constraint            | float               | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311"                                                                                                     |
| 31 | financial_dep_us             | float               | Financial dependency metric based on US data                                                                                                                                                              |
| 32 | liquidity_need_us            | float               | liquidity need metric based on US data                                                                                                                                                                    |
| 33 | rd_intensity_us              | float               | RD intensity metric based on US data                                                                                                                                                                      |
| 34 | supply_all_credit            | double              | province external supply of credit                                                                                                                                                                        |
| 35 | supply_long_term_credit      | float               | province external supply of long term credit                                                                                                                                                              |
| 36 | share_big_bank_loan          | decimal(21,5)       | share four State-owned commercial banks in total bank lending                                                                                                                                             |
| 37 | lag_share_big_bank_loan      | decimal(21,5)       | lag share four State-owned commercial banks in total bank lending                                                                                                                                         |
| 38 | share_big_loan               | decimal(21,5)       | share four State-owned commercial banks in total lending                                                                                                                                                  |
| 39 | lag_share_big_loan           | decimal(21,5)       | lag share four State-owned commercial banks in total lending                                                                                                                                              |
| 40 | credit_supply                | decimal(21,5)       | total bank lending normalised by gdp                                                                                                                                                                      |
| 41 | lag_credit_supply            | decimal(21,5)       | lag total bank lending normalised by gdp                                                                                                                                                                  |
| 42 | credit_supply_long_term      | decimal(21,5)       | total long term bank lending normalised by gdp                                                                                                                                                            |
| 43 | lag_credit_supply_long_term  | decimal(21,5)       | lag total long term bank lending normalised by gdp                                                                                                                                                        |
| 44 | credit_supply_short_term     | decimal(21,5)       | total short term bank lending normalised by gdp                                                                                                                                                           |
| 45 | lag_credit_supply_short_term | decimal(21,5)       | lag total short term bank lending normalised by gdp                                                                                                                                                       |
| 46 | output                       | bigint              | Output                                                                                                                                                                                                    |
| 47 | sales                        | bigint              | Sales                                                                                                                                                                                                     |
| 48 | employment                   | bigint              | Employemnt                                                                                                                                                                                                |
| 49 | capital                      | bigint              | Capital                                                                                                                                                                                                   |
| 50 | current_asset                | bigint              | current asset                                                                                                                                                                                             |
| 51 | tofixed                      | bigint              | total fixed asset                                                                                                                                                                                         |
| 52 | total_liabilities            | bigint              | total liabilities                                                                                                                                                                                         |
| 53 | total_asset                  | bigint              | total asset                                                                                                                                                                                               |
| 54 | tangible                     | bigint              | tangible asset                                                                                                                                                                                            |
| 55 | cashflow                     | bigint              | cash flow                                                                                                                                                                                                 |
| 56 | current_ratio                | decimal(21,5)       | current ratio                                                                                                                                                                                             |
| 57 | lag_current_ratio            | decimal(21,5)       | lag value of current ratio                                                                                                                                                                                |
| 58 | liabilities_tot_asset        | decimal(21,5)       | liabilities to total asset                                                                                                                                                                                |
| 59 | sales_tot_asset              | decimal(21,5)       | sales to total asset                                                                                                                                                                                      |
| 60 | lag_sales_tot_asset          | decimal(21,5)       | lag value of sales to asset                                                                                                                                                                               |
| 61 | asset_tangibility_tot_asset  | decimal(21,5)       | asset tangibility tot total asset                                                                                                                                                                         |
| 62 | lag_liabilities_tot_asset    | decimal(21,5)       | Lag liabiliteies to total asset                                                                                                                                                                           |
| 63 | cashflow_to_tangible         | decimal(21,5)       | cash flow to tangible                                                                                                                                                                                     |
| 64 | lag_cashflow_to_tangible     | decimal(21,5)       | lag cash flow to tangible                                                                                                                                                                                 |
| 65 | cashflow_tot_asset           | decimal(21,5)       | Cash flow to total asset                                                                                                                                                                                  |
| 66 | lag_cashflow_tot_asset       | decimal(21,5)       | lag cash flow tot total asset                                                                                                                                                                             |
| 67 | return_to_sale               | decimal(21,5)       | Return to sale                                                                                                                                                                                            |
| 68 | lag_return_to_sale           | decimal(21,5)       | Lag return tot sale                                                                                                                                                                                       |
| 69 | lower_location               | string              |                                                                                                                                                                                                           |
| 70 | larger_location              | string              |                                                                                                                                                                                                           |
| 71 | coastal                      | string              | City is bordered by sea or not                                                                                                                                                                            |
| 72 | dominated_output_soe_c       | boolean             | SOE dominated city of output. If true, then SOEs dominated city                                                                                                                                           |
| 73 | dominated_employment_soe_c   | boolean             | SOE dominated city of employment. If true, then SOEs dominated city                                                                                                                                       |
| 74 | dominated_sales_soe_c        | boolean             | SOE dominated city of sales. If true, then SOEs dominated city                                                                                                                                            |
| 75 | dominated_capital_soe_c      | boolean             | SOE dominated city of capital. If true, then SOEs dominated city                                                                                                                                          |
| 76 | dominated_output_for_c       | boolean             | foreign dominated city of output. If true, then foreign dominated city                                                                                                                                    |
| 77 | dominated_employment_for_c   | boolean             | foreign dominated city of employment. If true, then foreign dominated city                                                                                                                                |
| 78 | dominated_sales_for_c        | boolean             | foreign dominated cityof sales. If true, then foreign dominated city                                                                                                                                      |
| 79 | dominated_capital_for_c      | boolean             | foreign dominated city of capital. If true, then foreign dominated city                                                                                                                                   |
| 80 | dominated_output_i           | map<double,boolean> | map with information dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                                     |
| 81 | dominated_employment_i       | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                              |
| 82 | dominated_capital_i          | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                                 |
| 83 | dominated_sales_i            | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 84 | dominated_output_soe_i       | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                              |
| 85 | dominated_employment_soe_i   | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                          |
| 86 | dominated_sales_soe_i        | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 87 | dominated_capital_soe_i      | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                             |
| 88 | dominated_output_for_i       | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                          |
| 89 | dominated_employment_for_i   | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                      |
| 90 | dominated_sales_for_i        | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                           |
| 91 | dominated_capital_for_i      | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                         |
| 92 | fe_c_i                       | bigint              | City industry fixed effect                                                                                                                                                                                |
| 93 | fe_t_i                       | bigint              | year industry fixed effect                                                                                                                                                                                |
| 94 | fe_c_t                       | bigint              | city industry fixed effect                                                                                                                                                                                |

    