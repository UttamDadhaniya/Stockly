import pandas as pd
import csv
import os

# Define the directory where your CSV files are located
bhavcopy_directory = os.path.join(os.getcwd(), 'Bhavcopy')
list_of_files = os.listdir(bhavcopy_directory)

COL = [
    'TradDt', 'Sgmt', 'ISIN', 'TckrSymb', 'FinInstrmNm', 'OpnPric', 
    'HghPric', 'LwPric', 'ClsPric', 'LastPric', 
    'PrvsClsgPric', 'TtlTradgVol', 'TtlTrfVal', 'TtlNbOfTxsExctd'
]

def find_row(file, ISIN):
    pass



# Initialize a list to store the specific rows
specific_rows = []

# Loop through each file
for file in list_of_files:
    file_path = os.path.join(bhavcopy_directory, file)
    
    # Read the CSV file
    with open(file_path, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = next(csv_reader)  # Get the header row

        print (header[len(header)-2])
        
        # Specify the condition for the row you want (e.g., first row, or based on a condition)
        # For this example, let's collect the first data row
        first_row = next(csv_reader)
        
        # Append the first row (or any specific row you want) to the specific_rows list
        specific_rows.append(first_row)
    break

# Create a DataFrame from the collected rows
#df = pd.DataFrame(specific_rows, columns=header)

# Display the DataFrame
#print(df)

# Optionally, you can save this DataFrame to a CSV file or a database
# df.to_csv('collected_rows.csv', index=False)