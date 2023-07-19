import csv
import requests
import sqlite3

# Web address of the CSV file
csv_url = 'https://data.cityofchicago.org/resource/jcxq-k9xf.csv'

# Make a GET request to fetch the CSV data
response = requests.get(csv_url)

# Check if the request was successful
if response.status_code == 200:
    # Create a reader object from the CSV data
    csv_data = response.text
    reader = csv.reader(csv_data.splitlines())

    # Extract the column names from the first row
    column_names = next(reader)

    # Connect to the SQLite database
    conn = sqlite3.connect('your_database_name.db')
    cursor = conn.cursor()

    # Create the table with the extracted column names
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS data_table (
        {", ".join(column_names)} 
    )
    '''

    # Execute the SQL statement to create the table
    cursor.execute(create_table_sql)
    
     # Insert rows into the table
    insert_row_sql = f'''
    INSERT INTO data_table ({", ".join(column_names)}) 
    VALUES ({", ".join(["?"] * len(column_names))})
    '''

    for row in reader:
        cursor.execute(insert_row_sql, row)


    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    print("Table created successfully.")
else:
    print("Failed to fetch CSV data from the web address.")
    
 ### Remove Duplicate Rows and null rows
 


 # Connect to the SQLite database
conn = sqlite3.connect('your_database_name.db')
cursor = conn.cursor()

 # Execute the SQL update query to change null or None values to 0
update_query = "UPDATE data_table SET hardship_index = 0 WHERE hardship_index IS NULL OR hardship_index = ''"
cursor.execute(update_query)

# Create a temporary table with distinct rows
create_temp_table_query = "CREATE TABLE temp_table AS SELECT DISTINCT * FROM data_table"
cursor.execute(create_temp_table_query)

# Delete the original table
delete_original_query = "DROP TABLE data_table"
cursor.execute(delete_original_query)

# Rename the temporary table to the original table name
rename_table_query = "ALTER TABLE temp_table RENAME TO data_table"
cursor.execute(rename_table_query)


# delete null rows
delete_empty_query = "DELETE FROM data_table WHERE hardship_index IS NULL OR hardship_index = ''"
cursor.execute(delete_empty_query)

conn.commit()
conn.close()

    
# 1 - How many rows are in the dataset?

# Connect to the SQLite database
conn = sqlite3.connect('your_database_name.db')
cursor = conn.cursor()

# Execute the SQL query to count the rows
count_query = 'SELECT COUNT(*) FROM data_table'
cursor.execute(count_query)

# Fetch the result of the query
row_count = cursor.fetchone()[0]

# Print the number of rows
print("Number of rows in the dataset:", row_count)



# 2 - How many community areas in Chicago have a hardship index greater than 50.0?


community_query = 'SELECT COUNT(*) FROM data_table WHERE hardship_index > 50.0'
cursor.execute(community_query)

# Fetch the result of the query
greater_count = cursor.fetchone()[0]

# Print the greater
print("Number of greater community in the dataset:", greater_count)


# 3 - What is the maximum value of hardship index in this dataset?

max_query = 'SELECT MAX(hardship_index) FROM data_table'
cursor.execute(max_query)

# Fetch the result of the query
max_index = cursor.fetchone()[0]

# Print the maximum of rows
print("Maximum value of hardship index:", max_index)

# 4 - Which community area which has the highest hardship index?

community_area_query = 'SELECT community_area_name FROM data_table WHERE hardship_index = (SELECT MAX(hardship_index) FROM data_table)'
cursor.execute(community_area_query)

# Fetch the result of the query
community_area_result = cursor.fetchone()[0]

# Print maximum
print("Maximum value of hardship index:", community_area_result)

# 5 - Which Chicago community areas have per-capita incomes greater than $60,000?

incomes_query = 'SELECT community_area_name FROM data_table WHERE per_capita_income_ > 60000'
cursor.execute(incomes_query)

# Fetch the result of the query
incomes_result = cursor.fetchall()

# Print the incomes greater of rows
print("Community areas have per-capita incomes greater than $60,000:", incomes_result)


# Create a scatter plot using the variables `per_capita_income_` and `hardship_index`. Explain the correlation between the two variables.
import matplotlib.pyplot as plt

# Execute the SQL query to retrieve per_capita_income_ and hardship_index
query = 'SELECT per_capita_income_, hardship_index FROM data_table'
cursor.execute(query)

# Fetch all the results of the query
results = cursor.fetchall()

# Extract the per_capita_income_ and hardship_index values from the results
per_capita_income = [result[0] for result in results]
hardship_index = [result[1] for result in results]

# Create a scatter plot
plt.scatter(per_capita_income, hardship_index)
plt.xlabel('Per Capita Income')
plt.ylabel('Hardship Index')
plt.title('Scatter Plot: Per Capita Income vs. Hardship Index')

# Show the plot
plt.show()




conn.commit()
conn.close()