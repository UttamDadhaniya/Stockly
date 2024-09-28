import mysql.connector as mysql
import csv
import pandas as pd
from sqlalchemy import create_engine
import os

COL = []

#conn = mysql.connect(user='root', password='', host='localhost', database='stockwisedb') #global connection to stock database close after calling the function
#cursor = conn.cursor() 


directory_path = os.getcwd() + '\\Bhavcopy'  # directory where all the Bhavcopy is stored and needs to be uploaded

with os.scandir(directory_path) as entries:
    list_of_files = [entry.name for entry in entries if entry.is_file()]        # Files is a array containing name of all the bhavcopy

def table_exists(tablename):
    cursor.execute(f"SHOW TABLES LIKE '{tablename}'")
    result = cursor.fetchone()

    if result:
        return True
    else:
        create_table_query = f"""
        CREATE TABLE {tablename}(
            TradeDt DATE,
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
    csv_file = filename
    # Define the target column names
    target_columns = [
        'TradDt', 'ISIN', 'TckrSymb', 'FinInstrmNm', 'OpnPric', 
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
    
    for file in filename:
    
        if(filename == "upload.py"):
            print("Not a valid file")
        else:
            get_column_indices(file)
            
            with open(file, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file)
                header = next(csv_reader)
                for row in csv_reader:
                
                #function to create table and check if table exits or not
                    if(table_exists(row[COL[1]])):
                    
                        t_name = row[COL[1]]
                        
                        sql = f"""INSERT INTO {t_name} (TradeDt, ISIN, TckrSymb, FinInstrmNm, OpnPric,HghPric, LwPric, ClsPric, LastPric, PrvsClsgPric,TtlTradgVol, TtlTrfVal, TtlNbOfTxsExctd) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE TradeDt = TradeDt"""
                        
                        row_data = [row[col] for col in COL[:13]] #array to list
                        try:
                            cursor.execute(sql, tuple(row_data))
                        except Exception as e:
                            print(f"An error occurred: {e}")
                        
                        print("details updated for "+row[COL[0]]+" and "+row[COL[3]])
                        #Commit the changes and close the connection
                    else:
                        print("Error occured in function file_to_stock")
    conn.commit()
    

def update_stocks(name):            #updates list of stock when new bhavcopy is there

    name_of_file = name

    columns = ['ISIN', 'TckrSymb', 'FinInstrmNm']
    
    stocks = pd.read_csv(name_of_file, usecols=columns)
    
    database_url = 'mysql+mysqldb://root:@localhost:3306/datewisedb'
            
    engine = create_engine(database_url)
    
    database_url_2 = 'mysql+mysqldb://root:@localhost:3306/stockwisedb'
            
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
            csv_file_path = filename

            columns_to_read = ['ISIN', 'TckrSymb', 'SctySrs', 'FinInstrmNm', 'OpnPric', 'HghPric', 'LwPric', 'ClsPric', 'LastPric', 'PrvsClsgPric', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd']
            
            data = pd.read_csv(csv_file_path, usecols=columns_to_read)

            database_url = 'mysql+mysqldb://root:@localhost:3306/datewisedb'
            
            engine = create_engine(database_url)
            
            try:
                data.to_sql(filename.replace(".csv",""), con=engine, if_exists='fail', index=False)
                update_stocks(filename)
                
                
            except ValueError as e:
                print (filename + " file is already uploaded") 

 

#file_to_table()                 #uplodes daily bhavcopy to database 1 - 'datewisedb'

#file_to_stock()                 #uploades stock wise data to database 2 - 'stockwisedb' where data of perticular stock is uploded

#cursor.close()
#conn.close()

#use 7 zip on all files and extract the files
#use *.* in search bar cut all csv file and paste it in new folder
#rename all files using cmd command - rename "BhavCopy_NSE_CM_0_0_0_*.csv" "//////////////////////*.csv"
#rename all files using cmd command - rename *.* ????????.*

#3rd database - stocknames is used for getting names using ISIN