import pandas as pd
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3

def extract(url, attributes):

    html = requests.get(url).text
    data = BeautifulSoup(html,'html.parser')
    table = data.find_all('tbody')
    rows = table[0].find_all('tr')
    df = pd.DataFrame(columns=attributes)

    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {attributes[0] : col[1].text.strip(),
                         attributes[1] : col[2].contents[0].strip()}
            data_dict = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,data_dict], ignore_index=True)
    return df

def transform(df):
    df['MC_GBP_Billion'] = round(pd.to_numeric(df['MC_USD_Billion'])*0.8 ,2)
    df['MC_EUR_Billion'] = round(pd.to_numeric(df['MC_USD_Billion'])*0.93 ,2)
    df['MC_INR_Billion'] = round(pd.to_numeric(df['MC_USD_Billion'])*82.95 ,2)
    df['MC_USD_Billion'] = round(pd.to_numeric(df['MC_USD_Billion']) ,2)
    return df

def load_csv(data, path):
    data.to_csv(path)

def load_to_db(df, sql_connection ,table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query, sql_connection):
    output = pd.read_sql(query,sql_connection)
    print(output)

def log_progress(message):
    now = datetime.now()
    time_format = "%Y-%H-%D-%H:%M:%S"
    time_stamp = now.strftime(time_format)
    with open('code_log.txt','a') as f:
        f.write(time_stamp + ' : ' + message +'\n')

cols = ['Name','MC_USD_Billion']
url ='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
path = 'Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, cols)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating Loading process')

load_csv(df,path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated')

load_to_db(df,sql_connection,table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query = f'SELECT * FROM {table_name}'
print("1st query: ")
print(run_query(query,sql_connection))

query = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
print("2nd : ")
print(run_query(query,sql_connection))

query = f'SELECT Name from {table_name} LIMIT 5'
print("3rd : ")
print(run_query(query,sql_connection))

log_progress('Process Complete')

sql_connection.close()

log_progress('Server Connection closed')
