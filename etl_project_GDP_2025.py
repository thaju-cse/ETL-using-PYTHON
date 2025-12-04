import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime


def extract(url):
    print("Extracting process initiating..")
    
    raw_request = requests.get(url)
    print("Data pulled successfully..")
    
    raw_text = raw_request.text
    soup_raw = BeautifulSoup(raw_text, 'html.parser')
    tables = soup_raw.find_all('table')
    table = tables[2].find_all('tr')
    print("Almost there..")
    
    gdp_data = []
    r = []
    for element in table[0].find_all('th'):
        r.append(element.text)
    gdp_data.append(r)
    for row in table[1:]:
        r = []
        for element in row.find_all('td'):
            r.append(element.text)
        gdp_data.append(r)

    print("Task Completed.")
    
    return gdp_data
        
def transform(gdp_data_list):
    print("Started Transforming data..")

    transformed_gdp_data = [['Country/Territory','IMF_2025','World_Bank_22_24','United_Nations_23']]
    for row in gdp_data_list[1:]:
        r = []
        try:
            one = row[0].replace('\xa0','')
        except:
            one = ''
        try:
            two = int(row[1].replace(',','')) / 1000
        except:
            two = 0
        try:
            three = int(row[2].replace(',','')) / 1000
        except:
            three = 0
        try:
            four = int(row[3].replace(',','')) / 1000
        except:
            four = 0
        transformed_gdp_data.append([one,two,three,four])

    print("Transforming Task Completed.")

    return transformed_gdp_data
        
def loading(transformed_gdp_data):
    print("Initiating Loading process..")

    csv_path = 'gdp_data_2025_csv.csv'
    db_path = 'gdp_data_2025_db.db'
    table_path = 'gdp_data_2025_table'
    data_frame = pd.DataFrame(columns = ['Country/Territory', 'IMF_2025', 'World_Bank_22_24', 'United_Nations_23'])

    for row in transformed_gdp_data[1:]:
        row_dict = {'Country/Territory': row[0],
                    'IMF_2025': row[1],
                    'World_Bank_22_24': row[2],
                    'United_Nations_23': row[3]}
        temp_data_frame = pd.DataFrame(row_dict, index = [0])
        data_frame = pd.concat([data_frame, temp_data_frame], ignore_index = True)

    print("Inserting data into csv file..")
    with open(csv_path, 'w') as file:
        data_frame.to_csv(file, sep = ',')

    print("Inserting data into database table..")
    conn = sqlite3.connect(db_path)
    data_frame.to_sql(table_path, conn, if_exists = 'replace', index = False)
    conn.close

    print("Loading successfully Completed.")

    return { 'csv_file_name': csv_path, 'db_name': db_path, 'table_name': table_path}

def run_query(query, db_name, table_name):
    print('Query execution started..')
    try:
        sql_connection = sqlite3.connect(db_name)
        data_frame = pd.read_sql(query,sql_connection)

        print("Execution of the query completed, printing and returning Data Frame..")
        print(data_frame)
    
        log_process(query ,True)
        return True
    
    except:
        print("***Querying process FAILED***")
        log_process(query, False)
        return False

def log_process(query, value):
    print("Started Logging the process..")
    
    log_file = 'log_process.txt'
    if value:

        with open(log_file, 'a') as file:
            file.write('DATE and TIME:' +str(datetime.now())+'Query:\t'+ query +'Executed Successfully.')
    
    else:
        with open(log_file, 'a') as file:
            file.write('***DATE and TIME:' +str(datetime.now())+'Query:\t'+ query +'Execution Failed.')
    
        
    print("Logging process Completed.")
    return True


def main():
    url = 'https://web.archive.org/web/20250913062823/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
    gdp_data_list = extract(url)
        
    transformed_gdp_data = transform(gdp_data_list)  
   
    files_dict = loading(transformed_gdp_data)

    table_name = files_dict['table_name']
    db_name = files_dict['db_name']
    query = 'select * from ' + table_name
    
    querying = run_query(query, db_name, table_name)

    print("*****************************")
    

    
if __name__ == '__main__':
    main()
