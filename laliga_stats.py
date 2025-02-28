import requests
from datetime import datetime
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def log_progress(message):
    timestamp_format = "%Y-%h-%D-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("log_progress","a") as f:
        f.write(f"{timestamp}:{message}\n ")

def extract(url, table_attr):
    df = pd.DataFrame(columns=table_attr)
    page = requests.get(url).text
    data = BeautifulSoup(page,"html.parser")
    table = data.find_all("tbody")
    rows = table[0].find_all("tr")
    for row in rows:
        col = row.find_all("td")
        hd = row.find_all("th")
        if len(col) != 0:
            if col[5].text.strip():
                home_goal,away_goal = (col[5].find('a', recursive=False).contents[0].split("–"))
                data_dect = {"week":hd[0].text.strip(), 
                             "day":col[0].text.strip(), 
                             "time":col[2].text.strip(), 
                             "home":col[3].text.strip(), 
                             "xGh":col[4].text.strip(),
                             "hGoal":int(home_goal),
                             "aGoal":int(away_goal),
                             "xGa":col[6].text.strip(),
                             "away":col[7].text.strip()}
                df1 = pd.DataFrame(data_dect,index=[0])
                if df1 is not None and not df1.empty:
                    df= pd.concat([df,df1], ignore_index=True)
    return df

def load_to_db(df,DB_NAME,DB_PASSWORD,DB_USER,DB_HOST,DB_PORT):
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df.to_sql("games",engine,if_exists="replace",index=False)

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

url = "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures"
table_attr = ["week","day","time","home","xGh","hGoal","aGoal","xGa","away"]

log_progress("Preliminaries complete. Initiating ETL process")

df = extract(url,table_attr)
log_progress("Data extraction complete. Initiating Loading process")
DB_NAME = "laliga_games"
DB_USER = "postgres"
DB_PASSWORD = "0409"
DB_HOST = "localhost"
DB_PORT = "5432"



log_progress("SQL Connection initiated")
load_to_db(df,DB_NAME,DB_PASSWORD,DB_USER,DB_HOST,DB_PORT)
log_progress("Data loaded to Database as a table,")

csv_path = "laliga_games"
log_progress("Data saved to CSV file")

load_to_csv(df, csv_path)