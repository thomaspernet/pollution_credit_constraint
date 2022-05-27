import multiprocess
import fuzzywuzzy
from fuzzywuzzy import process
import warnings
import pandas as pd
import numpy as np
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from pathlib import Path
import os
import re
import json
import time
import glob
import shutil
import tempfile
from tqdm import tqdm
import pickle
import logging
import io
import boto3
import requests
from bs4 import BeautifulSoup
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
# Copy destination in S3 without bucket and "/" at the end
PATH_S3 = "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES"

# CREATE LIST CALL
nb_calls = [int(i) for i in list(np.arange(0, 31721, 10))] # 31721 for exit and 227270 for all
# SEND TO S3 -> to trigger Lambda
nb_calls[:3]

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


list_calls = list(chunks(nb_calls, 100))
len(list_calls)
PATH_S3_TRIGGER = "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES/TRIGGER_LIST"
j = 0
for i in tqdm(list_calls[1:]):
    json_data = {
        'range': i
    }
    FILENAME = "data_{}.json".format(j)
    PATH_LOCAL_RAW = os.path.join(tempfile.gettempdir(), FILENAME)
    with open(PATH_LOCAL_RAW, 'w') as outfile:
        json.dump(json_data, outfile)
    j += 1
    s3.upload_file(PATH_LOCAL_RAW, PATH_S3_TRIGGER)
    os.remove(PATH_LOCAL_RAW)
    time.sleep(15)
# RETRIEVE THE DATA

#j = 228
start_time = time.time()
list_s3 = s3.list_all_files_with_prefix(
    prefix=os.path.join(PATH_S3, 'RAW_JSON_EXIT'))
differences = list(set(nb_calls) -
                   set([int(re.findall(r'\d+', i.split("/")[-1])[0])
                        for i in list_s3])
                   )
while len(differences) > 0:
    list_calls = list(chunks(differences, 100))
    for i in tqdm(list_calls):
        json_data = {
            'range': i
        }
        FILENAME = "data_{}.json".format(j)
        PATH_LOCAL_RAW = os.path.join(tempfile.gettempdir(), FILENAME)
        with open(PATH_LOCAL_RAW, 'w') as outfile:
            json.dump(json_data, outfile)
        s3.upload_file(PATH_LOCAL_RAW, PATH_S3_TRIGGER)
        os.remove(PATH_LOCAL_RAW)
        time.sleep(15)
        j += 1
    # check missing
    list_s3 = s3.list_all_files_with_prefix(
        prefix=os.path.join(PATH_S3, 'RAW_JSON_EXIT'))
    differences = list(set(nb_calls) -
                       set([int(re.findall(r'\d+', i.split("/")[-1])[0])
                            for i in list_s3])
                       )

end_time = time.time() - start_time
print(end_time / 3600)

# read files
#### Choose  RAW_JSON for whole data and RAW_JSON_EXIT for exit
warnings.filterwarnings("ignore")
client_ = boto3.resource('s3')
list_s3 = s3.list_all_files_with_prefix(
    prefix=os.path.join(PATH_S3, 'RAW_JSON_EXIT'))


def convert_to_df(key):
    obj = client_.Object(bucket, key)
    body = obj.get()['Body'].read()
    fix_bytes_value = body.replace(b"'", b'"')
    my_json = json.load(io.BytesIO(fix_bytes_value))
    return pd.DataFrame(my_json)


# append to df -> bit slow: about 200k files -> we could have done it with Athena

df = pd.concat(map(convert_to_df, list_s3))
df.shape

df.to_csv('list_firms.csv', index=False)
################################################################################
################################################################################
################################################################################
# Download extra information
warnings.filterwarnings('ignore')
df = pd.read_csv('list_firms.csv', dtype={'id': 'str'})
df.shape

# DOWNLOAD CITIES
query = """
SELECT * FROM "chinese_lookup"."china_city_code_normalised"
"""
df_cities = s3.run_query(
    query=query,
    database="chinese_lookup",
    s3_output='SQL_OUTPUT_ATHENA',
    filename="chinese_cities",  # Add filename to print dataframe
    destination_key='SQL_OUTPUT_ATHENA/CSV',  # Use it temporarily
)

regex = "北京|天津|石家庄|唐山|秦皇岛|邯郸|邢台|保定|张家口|承德|沧州|廊坊|衡水|太原|大同|阳泉|长治|晋城|朔州|晋中|运城|忻州|临汾|吕梁|呼和浩特|包头|乌海|赤峰|通辽|鄂尔多斯|呼伦贝尔|巴彦淖尔|乌兰察布|沈阳|大连|鞍山|抚顺|本溪|丹东|锦州|营口|阜新|辽阳|盘锦|铁岭|朝阳|葫芦岛|长春|吉林|四平|辽源|通化|白山|松原|白城|哈尔滨|齐齐哈尔|鸡西|鹤岗|双鸭山|大庆|伊春|佳木斯|七台河|牡丹江|黑河|绥化|上海|南京|无锡|徐州|常州|苏州|南通|连云港|淮安|盐城|扬州|镇江|泰州|宿迁|杭州|宁波|温州|嘉兴|湖州|绍兴|金华|衢州|舟山|台州|丽水|合肥|芜湖|蚌埠|淮南|马鞍山|淮北|铜陵|安庆|黄山|滁州|阜阳|宿州|巢湖市|六安|亳州|池州|宣城|福州|厦门|莆田|三明|泉州|漳州|南平|龙岩|宁德|南昌|景德镇|萍乡|九江|新余|鹰潭|赣州|吉安|宜春|抚州|上饶|济南|青岛|淄博|枣庄|东营|烟台|潍坊|济宁|泰安|威海|日照|莱芜|临沂|德州|聊城|滨州|菏泽|郑州|开封|洛阳|平顶山|安阳|鹤壁|新乡|焦作|濮阳|许昌|漯河|三门峡|南阳|商丘|信阳|周口|驻马店|武汉|黄石|十堰|宜昌|襄樊|鄂州|荆门|孝感|荆州|黄冈|咸宁|随州|长沙|株洲|湘潭|衡阳|邵阳|岳阳|常德|张家界|益阳|郴州|永州|怀化|娄底|广州|韶关|深圳|珠海|汕头|佛山|江门|湛江|茂名|肇庆|惠州|梅州|汕尾|河源|阳江|清远|东莞|中山|潮州|揭阳|云浮|南宁|柳州|桂林|梧州|北海|防城港|钦州|贵港|玉林|百色|贺州|河池|来宾|崇左|海口|三亚|重庆|成都|自贡|攀枝花|泸州|德阳|绵阳|广元|遂宁|内江|乐山|南充|眉山|宜宾|广安|达州|雅安|巴中|资阳|贵阳|六盘水|遵义|安顺|昆明|曲靖|玉溪|保山|昭通|丽江|思茅|临沧|西安|铜川|宝鸡|咸阳|渭南|延安|汉中|榆林|安康|商洛|兰州|嘉峪关|金昌|白银|天水|武威|张掖|平凉|酒泉|庆阳|定西|陇南|西宁|银川|石嘴山|吴忠|固原|中卫|乌鲁木齐|克拉玛依|beijing$|tianjin$|shijiazhuang$|tangshan$|qinhuangdao$|handan$|xingtai$|baoding$|zhangjiakou$|chengde$|cangzhou$|langfang$|hengshui$|taiyuan$|datong$|yangquan$|changzhi$|jincheng$|shuozhou$|jinzhong$|yuncheng$|xinzhou$|linfen$|luliang$|hohhot$|baotou$|wuhai$|chifeng$|tongliao$|erdos$|hulunbeier$|bayannaoer$|wulanchabu$|shenyang$|dalian$|anshan$|fushun$|benxi$|dandong$|jinzhou$|yingkou$|fuxin$|liaoyang$|panjin$|tieling$|chaoyang$|huludao$|changchun$|jilin$|siping$|liaoyuan$|tonghua$|hakusan$|matsubara$|baicheng$|harbin$|qiqihar$|jixi$|hegang$|shuangyashan$|daqing$|yī chūn$|jiamusi$|qitaihe$|mudanjiang$|heihe$|suihua$|shanghai$|nanjing$|wuxi$|xuzhou$|changzhou$|sūzhōu$|nantong$|lianyungang$|huaian$|yancheng$|yangzhou$|zhenjiang$|tàizhōu$|suqian$|hangzhou$|ningbo$|wenzhou$|jiaxing$|huzhou$|shaoxing$|jinhua$|quzhou$|zhoushan$|táizhōu$|lishui$|hefei$|wuhu$|bengbu$|huainan$|ma'anshan$|huaibei$|tongling$|anqing$|huangshan$|chuzhou$|fuyang$|sùzhōu$|chaohu$|lu'an$|bozhou$|chizhou$|xuancheng$|fúzhōu$|xiamen$|putian$|sanming$|quanzhou$|zhangzhou$|nanping$|longyan$|ningde$|nanchang$|jingdezhen$|pingxiang$|jiujiang$|xinyu$|yingtan$|ganzhou$|ji'an$|yíchūn$|fǔzhōu$|shangrao$|jinan$|qingdao$|zibo$|zaozhuang$|dongying$|yantai$|weifang$|jining$|tai'an$|weihai$|rizhao$|laiwu$|linyi$|dezhou$|liaocheng$|binzhou$|heze$|zhengzhou$|kaifeng$|luoyang$|pingdingshan$|anyang$|hebi$|xinxiang$|jiaozuo$|puyang$|xuchang$|luohe$|sanmenxia$|nanyang$|shangqiu$|xinyang$|zhoukou$|zhumadian$|wuhan$|huangyou$|shiyan$|yichang$|xiangfan$|ezhou$|jingmen$|xiaogan$|jingzhou$|huanggang$|xianning$|suizhou$|changsha$|zhuzhou$|xiangtan$|hengyang$|shaoyang$|yueyang$|changde$|zhangjiajie$|yiyang$|chenzhou$|yongzhou$|huaihua$|loudi$|canton$|shaoguan$|shenzhen$|zhuhai$|shantou$|foshan$|jiangmen$|zhanjiang$|maoming$|zhaoqing$|huizhou$|meizhou$|shanwei$|heyuan$|yangjiang$|qingyuan$|dongguan$|zhongshan$|chaozhou$|jieyang$|yunfu$|nanning$|liuzhou$|guilin$|wuzhou$|beihai$|fangchenggang$|qinzhou$|guigang$|yùlín$|baise$|hezhou$|hechi$|laibin$|chongzuo$|haikou$|sanya$|chongqing$|chengdu$|zigong$|panzhihua$|luzhou$|deyang$|mianyang$|guangyuan$|suining$|neijiang$|leshan$|nanchong$|meishan$|yibin$|guangan$|dazhou$|ya'an$|bazhong$|ziyang$|guiyang$|liupanshui$|zunyi$|anshun$|kunming$|qujing$|yuxi$|baoshan$|zhaotong$|lijiang$|simao$|lincang$|xi'an$|tongchuan$|baoji$|xianyang$|weinan$|yan'an$|hanzhong$|yúlín$|ankang$|shangluo$|lanzhou$|jiayuguan$|jinchang$|baiyin$|tianshui$|wuwei$|zhangye$|pingliang$|jiuquan$|qingyang$|dingxi$|longnan$|xining$|yinchuan$|shizuishan$|wuzhong$|guyuan$|zhongwei$|urumqi$|karamay$"
df_cities.head()

def find_city(x):
    match = " ".join(re.findall(regex, str(x)))
    if len(match) > 0:
        return match
    else:
        return None

# Test NLP
#from polyfuzz.models import TFIDF
#from polyfuzz import PolyFuzz


# list_candidates = list(df_cities['citycn_correct'].drop_duplicates()) + \
# list(df_cities['cityen_correct'].drop_duplicates().str.lower())
####
test = (
    df
    .assign(
        city=lambda x: x.apply(
            lambda x: find_city(x['fullName']), axis=1
        )
    )
)

empty_city = pd.concat(
    [test.loc[lambda x: ~x['city'].isin([None])], (
        test
        .loc[lambda x:  ~x['city'].isin([None])]
        .assign(count=lambda x:x.apply(lambda x: len(x['city'].split(" ")), axis=1))
        .loc[lambda x: x['count'] > 1]
        .drop(columns=['count'])
    )])

empty_city.shape
list_calls = list(chunks(list(empty_city['id']), 100))
len(list_calls)
j = 0
for i in tqdm(list_calls[29:]):
    df_temp = empty_city.loc[lambda x: x['id'].isin(i)]
    # save it to S3
    FILENAME = "data_id_{}.csv".format(j)
    PATH_LOCAL_RAW = os.path.join(tempfile.gettempdir(), FILENAME)
    df_temp.to_csv(PATH_LOCAL_RAW, index=False)
    time.sleep(10)  # play nice with the server
    s3.upload_file(
        PATH_LOCAL_RAW,
        "DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES/TRIGGER_LIST_BANK_BRANCHES_CITY"
    )
    os.remove(PATH_LOCAL_RAW)
    j += 1

# ADD GEOCODE AND ADD foudn cities
list_s3 = s3.list_all_files_with_prefix(
    prefix="DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES_CITY_REVERSE_MAP"
)
len(list_s3)

df_cities_reverse = pd.concat(map(s3.read_df_from_s3, list_s3))
df_city_branches_final = (
    pd.concat(
        [
            (
                test
                .loc[lambda x: ~x['id'].isin([str(i) for i in df_cities_reverse['id'].to_list()])]
                .reindex(columns=df_cities_reverse.columns)
            ),
            df_cities_reverse
        ]
    )
)

# retrieve missing
temp = (
    df_city_branches_final
    .loc[lambda x: x['city'].isin([np.nan])]
    .assign(
        clean_text=lambda x: x['location'].str.strip(
        ).str.lower().str.split(","),
        query=lambda x: x.apply(lambda
                                x: ' '.join(x['clean_text']) if isinstance(x['clean_text'], list) else np.nan, axis=1),
        clean_query=lambda x: x.apply(lambda x: re.sub(
            r'[0-9]+', '', str(x['query'])), axis=1)
        # fuzzywuzzy = lambda x: x.apply(lambda x:get_fuzzywuzzy(x['query']) if isinstance(x['clean_text'], list) else np.nan, axis =1
        #    )
    )
)

list_candidates = list(dict.fromkeys(temp['clean_query'].to_list()))
len(list_candidates)
list_candidates[:3]

def get_fuzzywuzzy(x):

    return {'query': x, 'results': process.extract(x, regex.replace("$", '').split('|'))}

# Faster the process


list_fuzzy = []
t1 = time.time()
pool = multiprocess.Pool(multiprocess.cpu_count())
list_fuzzy = pool.map(get_fuzzywuzzy, list_candidates)
pool.close()
pool.join()
t = time.time() - t1
print("SELFTIMED:", t)


def get_highest(x):
    if len(x['results']) > 0:
        maximum = max(x['results'], key=lambda item: item[1])
        if maximum[1] > 80:
            return {'query': x['query'], 'results': maximum[0]}


# Remove found from the list
df_city_branches_final.shape

df_city_branches_final_geo = (
    (
    pd.concat(
        [
            # remove newly found ID
            (
                df_city_branches_final
                .loc[lambda x: ~x['id'].isin(temp.merge(pd.DataFrame([i for i in [get_highest(x=i) for i in list_fuzzy] if i]))['id'])]
            ),
            # add newly found ID
            (
                temp.merge(pd.DataFrame(
                    [i for i in [get_highest(x=i) for i in list_fuzzy] if i]))
                .assign(city=lambda x: x['results'])
                .drop(columns=['results'])
            )
        ]
    )
     .assign(city_match = lambda x: x['city'].str.lower())
    )
    .merge(
        (df_cities
         .assign(geocode4_corr=lambda x: x['geocode4_corr'].astype(int).astype(str))
         .reindex(columns=['geocode4_corr', 'citycn_correct', 'cityen_correct'])
         .set_index(['geocode4_corr'])
         .stack()
         .reset_index()
         .drop(columns=['level_1'])
         .rename(columns={0: 'city'})
         .drop_duplicates()
         .assign(city = lambda x: x['city'].str.lower())
         .rename(columns = {'city':'city_match'})
         ), on=['city_match'], how='left'
    )
    .drop(columns = ['clean_text', 'query', 'clean_query'])
    .assign(
    city_temp = lambda x:x.apply(lambda x: '|'.join(
    ast.literal_eval(x['city_temp'])) if isinstance(x['city_temp'], str) else np.nan, axis =1),
    points = lambda x:x.apply(lambda x:
    '|'.join([str(i) for i in ast.literal_eval(x['points'])])
     if isinstance(x['points'], str) else np.nan, axis =1)
    )
    .reindex(columns =['id', 'flowNo', 'certCode', 'fullName', 'address', 'postCode',
       'setDate', 'printDate', 'useState', 'organNo', 'lastFlowNo', 'shutOut',
       'exitDate', 'lostReason', 'simpleName', 'deadLine', 'makeReason',
       'longitude', 'latitude', 'englishName', 'jrOrganPreproty',
       'jrOrganTypeNo', 'jrOrgBranchTypeNo', 'uscc', 'scope', 'lastScope',
       'fatherOrganNo', 'fatherOrganName', 'organName', 'provinceName',
       'lastId', 'lastCertCode', 'lastfullname', 'lastaddress', 'colIndex',
       'date', 'city', 'location', 'city_temp', 'points',  'lat',
       'lon', 'altitude', 'city_match', 'geocode4_corr'])
)

###  clean up dataframe to avoid issue in Athena
df_city_branches_final_geo = (
df_city_branches_final_geo
.assign(
location = lambda x: x.apply(lambda x: np.nan if pd.isna(x['location']) else
x['location'].replace(',', "|"), axis =1))
.assign(
**{
    "{}".format(col): df_city_branches_final_geo[col]
    .astype(str)
    .str.replace('\,', '', regex=True)
    .str.replace(r'\/|\(|\)|\?|\:|\-', '', regex=True)
    .str.replace('__', '_')
    .str.replace('\\n', '', regex=True)
    .str.replace('\"','', regex=True)
    for col in df_city_branches_final_geo.select_dtypes(include="object").columns
}
)
)
####
df_city_branches_final_geo = (pd.read_csv("df_city_branches_final_geo.csv",
dtype = {'id':'str','geocode4_corr':'str', 'lostReason':'str',
'location':'str', 'city_temp':'str', 'points':'str'}), parse_dates = [''])

df_city_branches_final_geo.head()

df_city_branches_final_geo.to_csv("df_city_branches_final_geo.csv", index=False)
# SAVE S3
s3.upload_file("df_city_branches_final_geo.csv",
"DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES/RAW_CSV")
os.remove("df_city_branches_final_geo.csv")

# ADD SHCEMA
for i in pd.io.json.build_table_schema(df_city_branches_final_geo)['fields']:
    if i['type'] in ['number', 'integer']:
        i['type'] = 'int'
    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':i['name']}))

schema =[{'Name': 'index', 'Type': 'int', 'Comment': 'index'},
{'Name': 'id', 'Type': 'string', 'Comment': 'id'},
{'Name': 'flowNo', 'Type': 'int', 'Comment': 'flowNo'},
{'Name': 'certCode', 'Type': 'string', 'Comment': 'certCode'},
{'Name': 'fullName', 'Type': 'string', 'Comment': 'fullName'},
{'Name': 'address', 'Type': 'int', 'Comment': 'address'},
{'Name': 'postCode', 'Type': 'int', 'Comment': 'postCode'},
{'Name': 'setDate', 'Type': 'string', 'Comment': 'setDate'},
{'Name': 'printDate', 'Type': 'string', 'Comment': 'printDate'},
{'Name': 'useState', 'Type': 'int', 'Comment': 'useState'},
{'Name': 'organNo', 'Type': 'int', 'Comment': 'organNo'},
{'Name': 'lastFlowNo', 'Type': 'int', 'Comment': 'lastFlowNo'},
{'Name': 'shutOut', 'Type': 'int', 'Comment': 'shutOut'},
{'Name': 'exitDate', 'Type': 'int', 'Comment': 'exitDate'},
{'Name': 'lostReason', 'Type': 'string', 'Comment': 'lostReason'},
{'Name': 'simpleName', 'Type': 'int', 'Comment': 'simpleName'},
{'Name': 'deadLine', 'Type': 'int', 'Comment': 'deadLine'},
{'Name': 'makeReason', 'Type': 'int', 'Comment': 'makeReason'},
{'Name': 'longitude', 'Type': 'int', 'Comment': 'longitude'},
{'Name': 'latitude', 'Type': 'int', 'Comment': 'latitude'},
{'Name': 'englishName', 'Type': 'int', 'Comment': 'englishName'},
{'Name': 'jrOrganPreproty', 'Type': 'int', 'Comment': 'jrOrganPreproty'},
{'Name': 'jrOrganTypeNo', 'Type': 'int', 'Comment': 'jrOrganTypeNo'},
{'Name': 'jrOrgBranchTypeNo', 'Type': 'int', 'Comment': 'jrOrgBranchTypeNo'},
{'Name': 'uscc', 'Type': 'int', 'Comment': 'uscc'},
{'Name': 'scope', 'Type': 'int', 'Comment': 'scope'},
{'Name': 'lastScope', 'Type': 'int', 'Comment': 'lastScope'},
{'Name': 'fatherOrganNo', 'Type': 'int', 'Comment': 'fatherOrganNo'},
{'Name': 'fatherOrganName', 'Type': 'int', 'Comment': 'fatherOrganName'},
{'Name': 'organName', 'Type': 'int', 'Comment': 'organName'},
{'Name': 'provinceName', 'Type': 'int', 'Comment': 'provinceName'},
{'Name': 'lastId', 'Type': 'int', 'Comment': 'lastId'},
{'Name': 'lastCertCode', 'Type': 'int', 'Comment': 'lastCertCode'},
{'Name': 'lastfullname', 'Type': 'int', 'Comment': 'lastfullname'},
{'Name': 'lastaddress', 'Type': 'int', 'Comment': 'lastaddress'},
{'Name': 'colIndex', 'Type': 'int', 'Comment': 'colIndex'},
{'Name': 'date', 'Type': 'string', 'Comment': 'date'},
{'Name': 'city', 'Type': 'string', 'Comment': 'city'},
{'Name': 'location', 'Type': 'string', 'Comment': 'location from Google mal'},
{'Name': 'city_temp', 'Type': 'string', 'Comment': 'city_temp'},
{'Name': 'points', 'Type': 'string', 'Comment': 'points'},
{'Name': 'lat', 'Type': 'int', 'Comment': 'lat'},
{'Name': 'lon', 'Type': 'int', 'Comment': 'lon'},
{'Name': 'altitude', 'Type': 'int', 'Comment': 'altitude'},
{'Name': 'city_match', 'Type': 'string', 'Comment': 'city_match'},
{'Name': 'geocode4_corr', 'Type': 'string', 'Comment': 'geocode4_corr'}]

# ADD DESCRIPTION
description = 'Chinese banking branches ajusted with city name'

glue = service_glue.connect_glue(client=client)

target_S3URI = os.path.join("s3://", bucket,
"DATA/ECON/CHINA/CHINA_BANKING_AND_INSURANCE_REGULATORY_COMMISSION/BANK_BRANCHES/RAW_CSV")
name_crawler = "crawl-industry-name"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "china"
TablePrefix = 'branches_'  # add "_" after prefix, ex: hello_


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
filename = 'bank_branches.py'
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
