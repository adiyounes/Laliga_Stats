import requests
from datetime import datetime
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


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
                home_goal,away_goal = col[5].find('a', recursive=False).contents[0].split("–")
                data_dect = {"week":hd[0].text.strip(), 
                             "day":col[0].text.strip(), 
                             "time":col[2].text.strip(), 
                             "home":col[3].text.strip(), 
                             "xGh":col[4].text.strip(),
                             "hGoal":home_goal,
                             "aGoal":away_goal,
                             "xGa":col[6].text.strip(),
                             "away":col[7].text.strip()}
                df1 = pd.DataFrame(data_dect,index=[0])
                if df1 is not None and not df1.empty:
                    df= pd.concat([df,df1], ignore_index=True)
    return df



url = "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures"
table_attr = ["week","day","time","home","xGh","hGoal","aGoal","xGa","away"]


log_progress("hi there")

df = extract(url,table_attr)
print(df.head())