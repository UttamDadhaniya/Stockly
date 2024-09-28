import mysql.connector as mysql
import csv
import pandas as pd
from sqlalchemy import create_engine
import os
import configparser

COL = []

#--------------------------------------------------------------
#configuration information


config = configparser.ConfigParser()
config.read('config.ini')

db_host = config['database_1']['host']
db_user = config['database_1']['user']
db_password = config['database_1']['password']
db_port = config['database_1']['port']
db_name_1 = config['database_1']['database']    #datewisedb
db_name_2 = config['database_2']['database']    #stockwisedb

#---------------------------------------------------------------


conn = mysql.connect(user=db_user, password=db_password, host=db_host, database=db_name_2) #global connection to stock database close after calling the function
cursor = conn.cursor() 

list_of_files = os.listdir(os.path.join(os.getcwd(), 'Bhavcopy'))        # Files is a array containing name of all the bhavcopy

def table_exists(tablename):
    cursor.execute(f"SHOW TABLES LIKE '{tablename}'")
    result = cursor.fetchone()

    if result:
        return True
    else:
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {tablename}(
            TradeDt DATE,
            Sgmt VARCHAR(10) NOT NULL,
            ISIN VARCHAR(12) NOT NULL,
            TckrSymb VARCHAR(10) NOT NULL,
            FinInstrmNm VARCHAR(255) NOT NULL,
            OpnPric DOUBLE,
            HghPric DOUBLE NOT NULL,
            LwPric DOUBLE NOT NULL,
            ClsPric DOUBLE NOT NULL,
            LastPric DOUBLE NOT NULL,
            PrvsClsgPric DOUBLE NOT NULL,
            TtlTradgVol BIGINT NOT NULL,
            TtlTrfVal DOUBLE NOT NULL,
            TtlNbOfTxsExctd BIGINT NOT NULL
            
        );
        """
        cursor.execute(create_table_query)
        print(f"Table '{tablename}' created successfully.")
        return True

def get_column_indices(filename):

    global COL
    COL.clear()
    csv_file = os.path.join(os.getcwd(), 'Bhavcopy', filename)
    # Define the target column names
    target_columns = [
        'TradDt', 'Sgmt', 'ISIN', 'TckrSymb', 'FinInstrmNm', 'OpnPric', 
        'HghPric', 'LwPric', 'ClsPric', 'LastPric', 
        'PrvsClsgPric', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd'
    ]
        
    # Open the CSV file
    with open(csv_file, mode='r', newline='') as file:
        reader = csv.reader(file)
        header = next(reader)
        for column in target_columns:
            if column in header:
                COL.append(header.index(column))
            else:
                print("column not found")  # Column not found
 

def file_to_stock():                #uploades stock wise data to database 2 - 'stockwisedb' where data of perticular stock is uploded
    
    global conn, cursor
    
    filename = list_of_files
    batch_size = 1000
    
    for file in filename:
    
        get_column_indices(file)

        file = os.path.join(os.getcwd(), 'Bhavcopy', file)
        
        with open(file, mode='r') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)

            data_batch = []

            for row in csv_reader:

                t_name = row[COL[2]]

                #function to create table and check if table exits or not
                if(table_exists(t_name)):
                    
                    row_data = [row[col] for col in COL]

                    if len(row_data) != 14:  # Ensure correct number of columns for insert
                        print(f"Warning: Expected 14 values, but got {len(row_data)}. Row: {row}")
                        continue
                    
                    data_batch.append(tuple(row_data))

                    if len(data_batch) >= batch_size:

                        sql = f"""INSERT INTO {t_name} 
                                    (TradeDt, Sgmt, ISIN, TckrSymb, FinInstrmNm, 
                                    OpnPric, HghPric, LwPric, ClsPric, LastPric, 
                                    PrvsClsgPric, TtlTradgVol, TtlTrfVal, TtlNbOfTxsExctd) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s)"""
            
                        try:
                            cursor.executemany(sql, data_batch)
                            print(f"Inserted {len(data_batch)} records into {t_name}.")
                            conn.commit()
                            data_batch = []
                        except Exception as e:
                            print(f"An error occurred during batch insert: {e}")
                
            if data_batch:
                try:
                    cursor.executemany(sql, data_batch)
                    print(f"Inserted remaining {len(data_batch)} records into {t_name}.")
                except Exception as e:
                    print(f"An error occurred during final batch insert: {e}")
    conn.commit()
                       
    

def update_stocks(name):            #updates list of stock when new bhavcopy is there

    name_of_file = os.getcwd() + '\\Bhavcopy\\' + name

    columns = ['Sgmt', 'ISIN', 'TckrSymb', 'FinInstrmNm']
    
    stocks = pd.read_csv(name_of_file, usecols=columns)
    
    database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_1}'
            
    engine = create_engine(database_url)
    
    database_url_2 = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_2}'
            
    engine_2 = create_engine(database_url_2)
    
    try:
        stocks.to_sql('stocklist', con=engine, if_exists='replace', index=False)
        stocks.to_sql('stocklist', con=engine_2, if_exists='replace', index=False)
        
    except ValueError as e:
        print ("Error occured while updating stock list")
 
def file_to_table():                #uplodes daily bhavcopy to database1 - 'datewisedb'

    for filename in list_of_files:

        if (filename == "upload.py"):
            print("Successfully done")
        else:
            csv_file_path = os.path.join(os.getcwd(), 'Bhavcopy', filename)

            columns_to_read = ['Sgmt', 'ISIN', 'TckrSymb', 'SctySrs', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 'PrvsClsgPric', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd']
            
            data = pd.read_csv(csv_file_path, usecols=columns_to_read)

            data['PerChange'] = (data['ClsPric'] - data['PrvsClsgPric'])/data['PrvsClsgPric']

            database_url = f'mysql+mysqldb://{db_user}:{db_password}@{db_host}:{db_port}/{db_name_1}'
            
            engine = create_engine(database_url)
            
            try:
                data.to_sql(filename.replace(".csv",""), con=engine, if_exists='fail', index=False)
                update_stocks(filename)
                 
            except ValueError as e:
                print (filename + " file is already uploaded") 

 

#file_to_table()                 #uplodes daily bhavcopy to database 1 - 'datewisedb'

file_to_stock()                 #uploades stock wise data to database 2 - 'stockwisedb' where data of perticular stock is uploded

#conn.commit()
cursor.close()
conn.close()

#use 7 zip on all files and extract the files
#use *.* in search bar cut all csv file and paste it in new folder
#rename all files using cmd command - rename "BhavCopy_NSE_CM_0_0_0_*.csv" "//////////////////////*.csv"
#rename all files using cmd command - rename *.* ????????.*

#3rd database - stocknames is used for getting names using ISIN