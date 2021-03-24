
# Data Catalogue



## Table of Content

    
- [china_spatial_relocation](https://github.com/thomaspernet/pollution_credit_constraint/tree/master/00_data_catalog#table-china_spatial_relocation)
- [fin_dep_pollution_baseline_industry](https://github.com/thomaspernet/pollution_credit_constraint/tree/master/00_data_catalog#table-fin_dep_pollution_baseline_industry)

    

## Table china_spatial_relocation

- Database: policy
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/SPATIAL_RELOCATION`
- Partitition: []
- Script: https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/TARGET_SO2/china_cities_target_so2.py

|    | Name                             | Type   | Comment   |
|---:|:---------------------------------|:-------|:----------|
|  0 | citycn                           | string |           |
|  1 | distance                         | float  |           |
|  2 | ttoutput_i                       | float  |           |
|  3 | ttoutput_j                       | float  |           |
|  4 | tso2_mandate_c_j                 | float  |           |
|  5 | SO2_perc_reduction_c_j           | float  |           |
|  6 | avg_ij_o_city_mandate            | float  |           |
|  7 | w_avg_ij_o_city_mandate          | float  |           |
|  8 | avg_ij_city_mandate              | float  |           |
|  9 | w_avg_ij_city_mandate            | float  |           |
| 10 | avg_ij_o_perc_city_mandate       | float  |           |
| 11 | w_avg_ij_o_perc_city_mandate     | float  |           |
| 12 | avg_ij_perc_city_mandate         | float  |           |
| 13 | w_avg_ij_perc_city_mandate       | float  |           |
| 14 | d_avg_ij_o_city_mandate_previous | float  |           |
| 15 | d_w_avg_ij_o_city_mandate        | float  |           |
| 16 | tso2_mandate_c                   | float  |           |
| 17 | SO2_perc_reduction_c             | float  |           |
| 18 | output_diff                      | float  |           |
| 19 | target_diff                      | string |           |
| 20 | d_avg_ij_o_city_mandate          | string |           |

    

## Table fin_dep_pollution_baseline_industry

- Database: environment
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/INDUSTRY`
- Partitition: ['province_en', 'geocode4_corr', 'ind2', 'year']
- Script: https://github.com/thomaspernet/pollution_credit_constraint/blob/master/01_data_preprocessing/02_transform_tables/00_credit_constraint_industry.md

|    | Name                        | Type                | Comment                                                                                                                                                                                                   |
|---:|:----------------------------|:--------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | year                        | string              | year from 2001 to 2007                                                                                                                                                                                    |
|  1 | period                      | varchar(5)          | False if year before 2005 included, True if year 2006 and 2007                                                                                                                                            |
|  2 | province_en                 | string              |                                                                                                                                                                                                           |
|  3 | cityen                      | string              |                                                                                                                                                                                                           |
|  4 | geocode4_corr               | string              |                                                                                                                                                                                                           |
|  5 | tcz                         | string              | Two control zone policy city                                                                                                                                                                              |
|  6 | spz                         | string              | Special policy zone policy city                                                                                                                                                                           |
|  7 | ind2                        | string              | 2 digits industry                                                                                                                                                                                         |
|  8 | short                       | string              |                                                                                                                                                                                                           |
|  9 | polluted_d50i               | varchar(5)          | Sectors with values above Yearly 50th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 10 | polluted_d80i               | varchar(5)          | Sectors with values above Yearly 80th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 11 | polluted_d85i               | varchar(5)          | Sectors with values above Yearly 85th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 12 | polluted_d90i               | varchar(5)          | Sectors with values above Yearly 90th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 13 | polluted_d95i               | varchar(5)          | Sectors with values above Yearly 95th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 14 | polluted_mi                 | varchar(5)          | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW                                                                                                                                 |
| 15 | polluted_d50_cit            | varchar(5)          | Sectors with values above Yearly 50th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 16 | polluted_d80_cit            | varchar(5)          | Sectors with values above Yearly 80th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 17 | polluted_d85_cit            | varchar(5)          | Sectors with values above Yearly 85th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 18 | polluted_d90_cit            | varchar(5)          | Sectors with values above Yearly 90th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 19 | polluted_d95_cit            | varchar(5)          | Sectors with values above Yearly 95th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 20 | polluted_m_cit              | varchar(5)          | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW                                                                                                                                 |
| 21 | tso2                        | bigint              | Total so2 city sector. Filtered values above  4863 (5% of the distribution)                                                                                                                               |
| 22 | so2_intensity               | decimal(21,5)       | SO2 divided by output                                                                                                                                                                                     |
| 23 | tso2_mandate_c              | float               | city reduction mandate in tonnes                                                                                                                                                                          |
| 24 | above_threshold_mandate     | map<double,boolean> | Policy mandate above percentile .5, .75, .9, .95                                                                                                                                                          |
| 25 | avg_ij_o_city_mandate       | float               |                                                                                                                                                                                                           |
| 26 | d_avg_ij_o_city_mandate     | string              |                                                                                                                                                                                                           |
| 27 | above_average_mandate       | varchar(5)          | Policy mandate above national average                                                                                                                                                                     |
| 28 | in_10_000_tonnes            | float               | city reduction mandate in 10k tonnes                                                                                                                                                                      |
| 29 | tfp_cit                     | double              | TFP at the city industry level. From https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md#table-asif_tfp_firm_level |
| 30 | credit_constraint           | float               | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311"                                                                                                     |
| 31 | supply_all_credit           | double              | province external supply of credit                                                                                                                                                                        |
| 32 | supply_long_term_credit     | float               | province external supply of long term credit                                                                                                                                                              |
| 33 | output                      | bigint              | Output                                                                                                                                                                                                    |
| 34 | sales                       | bigint              | Sales                                                                                                                                                                                                     |
| 35 | employment                  | bigint              | Employemnt                                                                                                                                                                                                |
| 36 | capital                     | bigint              | Capital                                                                                                                                                                                                   |
| 37 | current_asset               | bigint              | current asset                                                                                                                                                                                             |
| 38 | tofixed                     | bigint              | total fixed asset                                                                                                                                                                                         |
| 39 | total_liabilities           | bigint              | total liabilities                                                                                                                                                                                         |
| 40 | total_asset                 | bigint              | total asset                                                                                                                                                                                               |
| 41 | tangible                    | bigint              | tangible asset                                                                                                                                                                                            |
| 42 | cashflow                    | bigint              | cash flow                                                                                                                                                                                                 |
| 43 | current_ratio               | decimal(21,5)       | current ratio                                                                                                                                                                                             |
| 44 | lag_current_ratio           | decimal(21,5)       | lag value of current ratio                                                                                                                                                                                |
| 45 | liabilities_tot_asset       | decimal(21,5)       | liabilities to total asset                                                                                                                                                                                |
| 46 | sales_tot_asset             | decimal(21,5)       | sales to total asset                                                                                                                                                                                      |
| 47 | lag_sales_tot_asset         | decimal(21,5)       | lag value of sales to asset                                                                                                                                                                               |
| 48 | asset_tangibility_tot_asset | decimal(21,5)       | asset tangibility tot total asset                                                                                                                                                                         |
| 49 | lag_liabilities_tot_asset   | decimal(21,5)       | Lag liabiliteies to total asset                                                                                                                                                                           |
| 50 | cashflow_to_tangible        | decimal(21,5)       | cash flow to tangible                                                                                                                                                                                     |
| 51 | lag_cashflow_to_tangible    | decimal(21,5)       | lag cash flow to tangible                                                                                                                                                                                 |
| 52 | cashflow_tot_asset          | decimal(21,5)       | Cash flow to total asset                                                                                                                                                                                  |
| 53 | lag_cashflow_tot_asset      | decimal(21,5)       | lag cash flow tot total asset                                                                                                                                                                             |
| 54 | return_to_sale              | decimal(21,5)       | Return to sale                                                                                                                                                                                            |
| 55 | lag_return_to_sale          | decimal(21,5)       | Lag return tot sale                                                                                                                                                                                       |
| 56 | lower_location              | string              |                                                                                                                                                                                                           |
| 57 | larger_location             | string              |                                                                                                                                                                                                           |
| 58 | coastal                     | string              | City is bordered by sea or not                                                                                                                                                                            |
| 59 | dominated_output_soe_c      | boolean             | SOE dominated city of output. If true, then SOEs dominated city                                                                                                                                           |
| 60 | dominated_employment_soe_c  | boolean             | SOE dominated city of employment. If true, then SOEs dominated city                                                                                                                                       |
| 61 | dominated_sales_soe_c       | boolean             | SOE dominated city of sales. If true, then SOEs dominated city                                                                                                                                            |
| 62 | dominated_capital_soe_c     | boolean             | SOE dominated city of capital. If true, then SOEs dominated city                                                                                                                                          |
| 63 | dominated_output_for_c      | boolean             | foreign dominated city of output. If true, then foreign dominated city                                                                                                                                    |
| 64 | dominated_employment_for_c  | boolean             | foreign dominated city of employment. If true, then foreign dominated city                                                                                                                                |
| 65 | dominated_sales_for_c       | boolean             | foreign dominated cityof sales. If true, then foreign dominated city                                                                                                                                      |
| 66 | dominated_capital_for_c     | boolean             | foreign dominated city of capital. If true, then foreign dominated city                                                                                                                                   |
| 67 | dominated_output_i          | map<double,boolean> | map with information dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                                     |
| 68 | dominated_employment_i      | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                              |
| 69 | dominated_capital_i         | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                                 |
| 70 | dominated_sales_i           | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 71 | dominated_output_soe_i      | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                              |
| 72 | dominated_employment_soe_i  | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                          |
| 73 | dominated_sales_soe_i       | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 74 | dominated_capital_soe_i     | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                             |
| 75 | dominated_output_for_i      | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                          |
| 76 | dominated_employment_for_i  | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                      |
| 77 | dominated_sales_for_i       | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                           |
| 78 | dominated_capital_for_i     | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                         |
| 79 | fe_c_i                      | bigint              | City industry fixed effect                                                                                                                                                                                |
| 80 | fe_t_i                      | bigint              | year industry fixed effect                                                                                                                                                                                |
| 81 | fe_c_t                      | bigint              | city industry fixed effect                                                                                                                                                                                |

    