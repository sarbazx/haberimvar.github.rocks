import os
import json
import mysql.connector
import time
from urllib.request import urlopen
import pandas as pd
import numpy as np

insert_new_message = "INSERT INTO Haber (TITLE, CONTENT,NEWS_URL) VALUES (%s, %s, %s)"

def data_extractor_from_api(url):
    response = urlopen(url)
    data_json = json.loads(response.read())
    data = data_json["articles"]
    df = pd.json_normalize(data)

    return df 
    
def getting_data_pipeline():
    url = "https://newsapi.org/v2/everything?q=apple&from=2022-09-06&to=2022-09-06&sortBy=popularity&apiKey=a298633332b04ca49fbe8450dd6fe353"
    apple_df = data_extractor_from_api(url)

    url = "https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey=a298633332b04ca49fbe8450dd6fe353"
    buss_df = data_extractor_from_api(url)

    url = "https://newsapi.org/v2/top-headlines?sources=techcrunch&apiKey=a298633332b04ca49fbe8450dd6fe353"
    tech_df = data_extractor_from_api(url)

    url = "https://newsapi.org/v2/everything?domains=wsj.com&apiKey=a298633332b04ca49fbe8450dd6fe353"
    wall_df = data_extractor_from_api(url)

    df_list = [apple_df, buss_df, tech_df, wall_df]
    df = pd.concat(df_list, axis=0)
    df = df.reset_index()
    df.drop("index", axis=1, inplace=True)

    return df
    
df = getting_data_pipeline()

def lambda_handler(event, context):
    title = df["title"]
    content = df["content"]
    news_url = df["url"]
    
    cnx = mysql.connector.connect(host=os.environ['RDS_HOSTNAME'], user=os.environ['RDS_USERNAME'],
                                      passwd=os.environ['RDS_PASSWORD'], database=os.environ['RDS_DB_NAME'],
                                      port=os.environ['RDS_PORT'])
    cur = cnx.cursor()
    
    for i,y,z in zip(title, content, news_url):
        cur.execute(insert_new_message, (i, y, z))
    
            
    cnx.commit()
    cnx.close()

    return {'statusCode': 200, 'body': json.dumps(cur.lastrowid, indent=0, sort_keys=True, default=str)}
    