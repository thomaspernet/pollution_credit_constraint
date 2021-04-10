# FIN DEP POLLUTION BASELINE INDUSTRY

Transform fin dep pollution baseline industry table by merging china_city_reduction_mandate and others
variables. Based on https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/04_fin_dep_pol_baseline_city.md

* **[fin_dep_pollution_baseline_industry](https://github.com/thomaspernet/pollution_credit_constraint/blob/master/01_data_preprocessing/02_transform_tables/00_credit_constraint_industry.md)**: 
Transform fin dep pollution baseline industry table by merging china_city_reduction_mandate and others
variables. Based on https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/04_fin_dep_pol_baseline_city.md

    * CREATION
        * [china_city_sector_pollution](None): None
        * [china_city_code_normalised](None): None
        * [asif_industry_financial_ratio_industry](None): None
        * [china_city_reduction_mandate](None): None
        * [china_city_tcz_spz](None): None
        * [ind_cic_2_name](None): None
        * [china_sector_pollution_threshold](None): None
        * [china_city_sector_year_pollution_threshold](None): None
        * [china_credit_constraint](None): None
        * [asif_tfp_firm_level](None): None
        * [asif_firms_prepared](None): None
        * [asif_industry_characteristics_ownership](None): None
        * [asif_city_characteristics_ownership](None): None
        * [province_credit_constraint](None): None
        * [china_spatial_relocation](https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/TARGET_SO2/china_cities_target_so2.py): Use the spreadsheet to download spatia relocation data. In the spreadsheet, construct the dummy as in the notebook to avoid computing it with Athena
        * [bank_socb_loan](https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/ALMANAC_BANK_LOAN/socb_loan.py): Download socb loan
        * [province_loan_and_credit](https://github.com/thomaspernet/pollution_credit_constraint/01_data_preprocessing/00_download_data/ALMANAC_BANK_LOAN/provinces.py): Download province loan and deposit data

### ETL diagrams



![](https://raw.githubusercontent.com/thomaspernet/pollution_credit_constraint/master/utils/IMAGES/fin_dep_pollution_baseline_industry.jpg)

