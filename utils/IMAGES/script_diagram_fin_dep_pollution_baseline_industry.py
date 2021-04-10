
from diagrams import Cluster, Diagram
from diagrams.aws.compute import ECS
from diagrams.aws.database import Redshift, RDS
from diagrams.aws.integration import SQS
from diagrams.aws.storage import S3

with Diagram("FIN DEP POLLUTION BASELINE INDUSTRY", show=False, filename="/home/ec2-user/pollution_credit_constraint/utils/IMAGES/fin_dep_pollution_baseline_industry", outformat="jpg"):

     temp_1 = S3('china_city_sector_pollution')
     temp_2 = S3('china_city_code_normalised')
     temp_3 = S3('asif_industry_financial_ratio_industry')
     temp_4 = S3('china_city_reduction_mandate')
     temp_5 = S3('china_city_tcz_spz')
     temp_6 = S3('ind_cic_2_name')
     temp_7 = S3('china_sector_pollution_threshold')
     temp_8 = S3('china_city_sector_year_pollution_threshold')
     temp_9 = S3('china_credit_constraint')
     temp_10 = S3('asif_tfp_firm_level')
     temp_11 = S3('asif_firms_prepared')
     temp_12 = S3('asif_industry_characteristics_ownership')
     temp_13 = S3('asif_city_characteristics_ownership')
     temp_14 = S3('province_credit_constraint')
     temp_15 = S3('china_spatial_relocation')
     temp_16 = S3('bank_socb_loan')
     temp_17 = S3('province_loan_and_credit')

     with Cluster("FINAL"):

         temp_final_0 = Redshift('fin_dep_pollution_baseline_industry')


     temp_final_0 << temp_1
     temp_final_0 << temp_2
     temp_final_0 << temp_3
     temp_final_0 << temp_4
     temp_final_0 << temp_5
     temp_final_0 << temp_6
     temp_final_0 << temp_7
     temp_final_0 << temp_8
     temp_final_0 << temp_9
     temp_final_0 << temp_10
     temp_final_0 << temp_11
     temp_final_0 << temp_12
     temp_final_0 << temp_13
     temp_final_0 << temp_14
     temp_final_0 << temp_15
     temp_final_0 << temp_16
     temp_final_0 << temp_17
