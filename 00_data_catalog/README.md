
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

|    | Name        | Type   | Comment                                             |
|---:|:------------|:-------|:----------------------------------------------------|
|  0 | bank        | string | Chinese bank 中国农业 中国工商 中国建设 or 中国银行 |
|  1 | province_cn | string | province chinese name                               |
|  2 | province_en | string | province english name                               |
|  3 | url         | string | google drive source file                            |
|  4 | year        | string | Year of the loan                                    |
|  5 | loan        | float  | loan in 亿                                          |
|  6 | metrics     | string | metric display either 亿 or 万                      |

    

## Table province_loan_and_credit

- Database: almanac_bank_china
- S3uri: `s3://datalake-datascience/DATA/ECON/ALMANAC_OF_CHINA_FINANCE_BANKING/PROVINCES/LOAN_AND_CREDIT`
- Partitition: []
- Script: https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/ALMANAC_BANK_LOAN/provinces.py

|    | Name                    | Type   | Comment                                          |
|---:|:------------------------|:-------|:-------------------------------------------------|
|  0 | province_cn             | string | province chinese name                            |
|  1 | province_en             | string | province english name                            |
|  2 | url                     | string | google drive source file                         |
|  3 | year                    | string | year                                             |
|  4 | cn_total_deposit        | string | Chinese character to match total deposit         |
|  5 | cn_total_loan           | string | Chinese character to match total loan            |
|  6 | cn_total_bank_loan      | string | Chinese character to match total bank loan       |
|  7 | cn_total_short_term     | string | Chinese character to match total short term loan |
|  8 | cn_total_long_term_loan | string | Chinese character to match total long term loan  |
|  9 | cn_total_gdp            | string | Chinese character to match total gdp             |
| 10 | total_deposit           | float  | total deposit                                    |
| 11 | total_loan              | float  | total loan                                       |
| 12 | total_bank_loan         | float  | total bank loan                                  |
| 13 | total_short_term        | float  | total short term loan                            |
| 14 | total_long_term_loan    | float  | total long term loan                             |
| 15 | total_gdp               | float  | total gdp                                        |
| 16 | metric                  | string | metric                                           |

    

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
| 21 | tso2                         | bigint              | Total so2 city sector.                                                                                                                                                                                    |
| 22 | tcod                         | bigint              | Total cod city sector                                                                                                                                                                                     |
| 23 | twaste_water                 | bigint              | Total waste water city sector.                                                                                                                                                                            |
| 24 | so2_intensity                | decimal(21,5)       | SO2 divided by output                                                                                                                                                                                     |
| 25 | tso2_mandate_c               | float               | city reduction mandate in tonnes                                                                                                                                                                          |
| 26 | target_reduction_so2_p       | float               | official province reduction mandate in percentage. From https://www.sciencedirect.com/science/article/pii/S0095069617303522#appsec1                                                                       |
| 27 | target_reduction_co2_p       | float               | official province reduction mandate in percentage. From https://www.sciencedirect.com/science/article/pii/S0095069617303522#appsec1                                                                       |
| 28 | above_threshold_mandate      | map<double,boolean> | Policy mandate above percentile .5, .75, .9, .95                                                                                                                                                          |
| 29 | avg_ij_o_city_mandate        | float               |                                                                                                                                                                                                           |
| 30 | d_avg_ij_o_city_mandate      | string              |                                                                                                                                                                                                           |
| 31 | above_average_mandate        | varchar(5)          | Policy mandate above national average                                                                                                                                                                     |
| 32 | in_10_000_tonnes             | float               | city reduction mandate in 10k tonnes                                                                                                                                                                      |
| 33 | tfp_cit                      | double              | TFP at the city industry level. From https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md#table-asif_tfp_firm_level |
| 34 | credit_constraint            | float               | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311"                                                                                                     |
| 35 | financial_dep_us             | float               | Financial dependency metric based on US data                                                                                                                                                              |
| 36 | liquidity_need_us            | float               | liquidity need metric based on US data                                                                                                                                                                    |
| 37 | rd_intensity_us              | float               | RD intensity metric based on US data                                                                                                                                                                      |
| 38 | supply_all_credit            | double              | province external supply of credit                                                                                                                                                                        |
| 39 | supply_long_term_credit      | float               | province external supply of long term credit                                                                                                                                                              |
| 40 | share_big_bank_loan          | decimal(21,5)       | share four State-owned commercial banks in total bank lending                                                                                                                                             |
| 41 | lag_share_big_bank_loan      | decimal(21,5)       | lag share four State-owned commercial banks in total bank lending                                                                                                                                         |
| 42 | share_big_loan               | decimal(21,5)       | share four State-owned commercial banks in total lending                                                                                                                                                  |
| 43 | lag_share_big_loan           | decimal(21,5)       | lag share four State-owned commercial banks in total lending                                                                                                                                              |
| 44 | credit_supply                | decimal(21,5)       | total bank lending normalised by gdp                                                                                                                                                                      |
| 45 | lag_credit_supply            | decimal(21,5)       | lag total bank lending normalised by gdp                                                                                                                                                                  |
| 46 | credit_supply_long_term      | decimal(21,5)       | total long term bank lending normalised by gdp                                                                                                                                                            |
| 47 | lag_credit_supply_long_term  | decimal(21,5)       | lag total long term bank lending normalised by gdp                                                                                                                                                        |
| 48 | credit_supply_short_term     | decimal(21,5)       | total short term bank lending normalised by gdp                                                                                                                                                           |
| 49 | lag_credit_supply_short_term | decimal(21,5)       | lag total short term bank lending normalised by gdp                                                                                                                                                       |
| 50 | output                       | bigint              | Output                                                                                                                                                                                                    |
| 51 | sales                        | bigint              | Sales                                                                                                                                                                                                     |
| 52 | employment                   | bigint              | Employemnt                                                                                                                                                                                                |
| 53 | capital                      | bigint              | Capital                                                                                                                                                                                                   |
| 54 | current_asset                | bigint              | current asset                                                                                                                                                                                             |
| 55 | tofixed                      | bigint              | total fixed asset                                                                                                                                                                                         |
| 56 | total_liabilities            | bigint              | total liabilities                                                                                                                                                                                         |
| 57 | total_asset                  | bigint              | total asset                                                                                                                                                                                               |
| 58 | tangible                     | bigint              | tangible asset                                                                                                                                                                                            |
| 59 | cashflow                     | bigint              | cash flow                                                                                                                                                                                                 |
| 60 | current_ratio                | decimal(21,5)       | current ratio                                                                                                                                                                                             |
| 61 | lag_current_ratio            | decimal(21,5)       | lag value of current ratio                                                                                                                                                                                |
| 62 | liabilities_tot_asset        | decimal(21,5)       | liabilities to total asset                                                                                                                                                                                |
| 63 | sales_tot_asset              | decimal(21,5)       | sales to total asset                                                                                                                                                                                      |
| 64 | lag_sales_tot_asset          | decimal(21,5)       | lag value of sales to asset                                                                                                                                                                               |
| 65 | asset_tangibility_tot_asset  | decimal(21,5)       | asset tangibility tot total asset                                                                                                                                                                         |
| 66 | lag_liabilities_tot_asset    | decimal(21,5)       | Lag liabiliteies to total asset                                                                                                                                                                           |
| 67 | cashflow_to_tangible         | decimal(21,5)       | cash flow to tangible                                                                                                                                                                                     |
| 68 | lag_cashflow_to_tangible     | decimal(21,5)       | lag cash flow to tangible                                                                                                                                                                                 |
| 69 | cashflow_tot_asset           | decimal(21,5)       | Cash flow to total asset                                                                                                                                                                                  |
| 70 | lag_cashflow_tot_asset       | decimal(21,5)       | lag cash flow tot total asset                                                                                                                                                                             |
| 71 | return_to_sale               | decimal(21,5)       | Return to sale                                                                                                                                                                                            |
| 72 | lag_return_to_sale           | decimal(21,5)       | Lag return tot sale                                                                                                                                                                                       |
| 73 | lower_location               | string              |                                                                                                                                                                                                           |
| 74 | larger_location              | string              |                                                                                                                                                                                                           |
| 75 | coastal                      | string              | City is bordered by sea or not                                                                                                                                                                            |
| 76 | dominated_output_soe_c       | boolean             | SOE dominated city of output. If true, then SOEs dominated city                                                                                                                                           |
| 77 | dominated_employment_soe_c   | boolean             | SOE dominated city of employment. If true, then SOEs dominated city                                                                                                                                       |
| 78 | dominated_sales_soe_c        | boolean             | SOE dominated city of sales. If true, then SOEs dominated city                                                                                                                                            |
| 79 | dominated_capital_soe_c      | boolean             | SOE dominated city of capital. If true, then SOEs dominated city                                                                                                                                          |
| 80 | dominated_output_for_c       | boolean             | foreign dominated city of output. If true, then foreign dominated city                                                                                                                                    |
| 81 | dominated_employment_for_c   | boolean             | foreign dominated city of employment. If true, then foreign dominated city                                                                                                                                |
| 82 | dominated_sales_for_c        | boolean             | foreign dominated cityof sales. If true, then foreign dominated city                                                                                                                                      |
| 83 | dominated_capital_for_c      | boolean             | foreign dominated city of capital. If true, then foreign dominated city                                                                                                                                   |
| 84 | dominated_output_i           | map<double,boolean> | map with information dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                                     |
| 85 | dominated_employment_i       | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                              |
| 86 | dominated_capital_i          | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                                 |
| 87 | dominated_sales_i            | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 88 | dominated_output_soe_i       | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                              |
| 89 | dominated_employment_soe_i   | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                          |
| 90 | dominated_sales_soe_i        | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 91 | dominated_capital_soe_i      | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                             |
| 92 | dominated_output_for_i       | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                          |
| 93 | dominated_employment_for_i   | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                      |
| 94 | dominated_sales_for_i        | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                           |
| 95 | dominated_capital_for_i      | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                         |
| 96 | fe_c_i                       | bigint              | City industry fixed effect                                                                                                                                                                                |
| 97 | fe_t_i                       | bigint              | year industry fixed effect                                                                                                                                                                                |
| 98 | fe_c_t                       | bigint              | city industry fixed effect                                                                                                                                                                                |

    