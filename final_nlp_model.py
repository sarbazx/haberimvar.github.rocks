###############################################################################################
# Libraries
###############################################################################################

from warnings import filterwarnings
import warnings

warnings.simplefilter(action='ignore', category=Warning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

import pandas as pd
import numpy as np
from urllib.request import urlopen
import json
import spacy
import mysql.connector
import os
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')

pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 300)

###############################################################################################
# Model Pipeline
###############################################################################################
get_messages_sql = """
SELECT * FROM haberimvar.Haber 
WHERE 
YEAR(DATE_) + MONTH(DATE_) + DAY(DATE_) 
=
YEAR(CURDATE()) +
MONTH(CURDATE()) +
DAY(CURDATE())
ORDER BY ID ASC
"""

get_initial_sql = "SELECT * FROM Haber ORDER BY RAND() LIMIT 10"
get_news_sql = "SELECT * FROM Haber WHERE ID=%s"

def connect_rds():
    cnx = mysql.connector.connect(host='your_db_host',
                                  user='your_db_user',
                                  passwd='your_db_passwd',
                                  database='haberimvar',
                                  port='your_db_port')
    cur = cnx.cursor()
    cur.execute(get_messages_sql)
    res = cur.fetchall()
    df = pd.DataFrame(res)

    cnx.close()

    return df

def get_news_by_id(id):
    cnx = mysql.connector.connect(host='your_db_host',
                                  user='your_db_user',
                                  passwd='your_db_passwd',
                                  database='haberimvar',
                                  port='your_db_port')
    cur = cnx.cursor()
    cur.execute(get_news_sql, (id,))
    res = cur.fetchall()

    cnx.close()
    ret = []
    for row in res:
        ret.append({"id": row[0], "title": row[1], "content": row[2], "date": row[3], "url": row[4]})

    return json.dumps(ret, indent=0, sort_keys=True, default=str)


def get_initial_data():
    cnx = mysql.connector.connect(host='your_db_host',
                                  user='your_db_user',
                                  passwd='your_db_passwd',
                                  database='haberimvar',
                                  port='your_db_port')
    cur = cnx.cursor()
    cur.execute(get_initial_sql)
    res = cur.fetchall()

    cnx.close()
    ret = []
    for row in res:
        ret.append({"id": row[0], "title": row[1], "content": row[2], "date": row[3]})

    return json.dumps(ret, indent=0, sort_keys=True, default=str)


main_df = connect_rds().drop_duplicates(subset=2)
main_df = main_df.rename(columns={0: "id",
                       1: "title",
                       2: "content",
                       3: "date",
                       4: "url"})
df = main_df
sw = stopwords.words('english')

def text_preprocessing(df, sw):
    df['content'] = df['content'].str.lower()
    df['content'] = df['content'].str.replace('[^\w\s]', '')
    df['content'] = df['content'].str.replace('\d', '')
    df['content'] = df['content'].apply(lambda x: " ".join(x for x in str(x).split() if x not in sw))
    temp_df = pd.Series(' '.join(df['content']).split()).value_counts()
    drops = temp_df[temp_df <= 1]
    df['content'] = df['content'].apply(lambda x: " ".join(x for x in x.split() if x not in drops))
    return df

df = text_preprocessing(df, sw)
old_df = main_df
nlp = spacy.load('en_core_web_sm')

def model_pipeline(df, old_df, nlp):

    nlp_docs = []

    for nlp_doc in df["content"]:
        nlp_docs.append(nlp(nlp_doc))

    col1 = []
    col2 = []
    col3 = []
    col4 = []
    col3_title = []
    col4_title = []
    col3_id = []
    col4_id = []
    col3_content = []
    col4_content = []

    for i in nlp_docs:
        for y in nlp_docs:
            col1.append(i)
            col2.append(y)

    for i in df["content"]:
        for y in df["content"]:
            col3.append(i)
            col4.append(y)

    for i in df["title"]:
        for y in df["title"]:
            col3_title.append(i)
            col4_title.append(y)

    for i in df["id"]:
        for y in df["id"]:
            col3_id.append(i)
            col4_id.append(y)

    for i in old_df["content"]:
        for y in old_df["content"]:
            col3_content.append(i)
            col4_content.append(y)

    nlp_df = pd.DataFrame({"col1": col1, "col2": col2, "col3": col3,
                           "col4": col4, "col3_title": col3_title, "col4_title": col4_title,
                           "col3_id": col3_id, "col4_id": col4_id, "col3_content": col3_content,
                           "col4_content": col4_content})

    drop_rows = nlp_df[nlp_df["col3_title"] == nlp_df["col4_title"]]
    nlp_df.drop(drop_rows.index, axis=0, inplace=True)
    nlp_df = nlp_df.reset_index()
    nlp_df.drop("index", axis=1, inplace=True)

    scores = []
    for i in range(0, len(nlp_df)):
        scores.append(nlp_df["col1"][i].similarity(nlp_df["col2"][i]))

    scores = pd.DataFrame(scores, columns=["Similarity_Scores"])
    nlp_df = pd.concat([nlp_df, scores], axis=1)

    nlp_df.drop_duplicates(inplace=True)

    return nlp_df


def news_recommender(new_title, rec_count=10):
    nlp_df = model_pipeline(df, old_df, nlp)
    rec_df = nlp_df[nlp_df['col3_title'].str.contains(new_title) == True].sort_values(by="Similarity_Scores", ascending=False)[
             0:rec_count]
    split_content = [i.split("[") for i in rec_df["col4_content"]]
    rec_df["col4_content"] = [i[0] for i in split_content]

    data = pd.DataFrame(rec_df[["col4_id", "col4_title", "col4_content"]])

    return data.to_json(orient="table")

