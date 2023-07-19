import pandas as pd
import sqlite3
import ssl

# Set the certificate verification paths
ssl._create_default_https_context = ssl._create_unverified_context

# Load CSV files into data frames
df1 = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoCensusData.csv')
df2 = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoPublicSchools.csv')
df3 = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DB0201EN-SkillsNetwork/labs/FinalModule_Coursera_V5/data/ChicagoCrimeData.csv')

# Create a connection to the SQLite database
conn = sqlite3.connect('Hands_On_Lab.db')

# Write the data frames to the database
df1.to_sql('CENSUS_DATA', conn, if_exists='replace', index=False)
df2.to_sql('CHICAGO_PUBLIC_SCHOOLS', conn, if_exists='replace', index=False)
df3.to_sql('CHICAGO_CRIME_DATA', conn, if_exists='replace', index=False)

# Close the database connection
conn.close()

print ("-----------------------------------------------")

# Create a connection to the SQLite database
conn = sqlite3.connect('Hands_On_Lab.db')
# Create a cursor object to execute SQL queries
cursor = conn.cursor()



### Problem 1

### Find the total number of crimes recorded in the CRIME table.
### -----------------------------------------------------------

# Execute the query
query1 = "SELECT COUNT(*) FROM CHICAGO_CRIME_DATA;"
cursor.execute(query1)

# Fetch the result
total_crimes = cursor.fetchone()[0]

# Print the result
print("Total number of crimes recorded:", total_crimes)

print ("-----------------------------------------------")

#----------------------------------------------------------------

### Problem 2

##### List community areas with per capita income less than 11000.
#----------------------------------------------------------------

# Execute the query
query2 = "SELECT community_area_name FROM CENSUS_DATA WHERE per_capita_income < 11000;"
cursor.execute(query2)

# Fetch all the results
results2 = cursor.fetchall()

# Print the results
print("Community areas with per capita income less than 11000:")
for row in results2:
    print (row[0])
print ("-----------------------------------------------")

#----------------------------------------------------------------

### Problem 3

##### List all case numbers for crimes  involving minors?(children are not considered minors for the purposes of crime analysis)
#----------------------------------------------------------------
# Execute the query
query3 = "SELECT case_number FROM CHICAGO_CRIME_DATA WHERE description LIKE '%MINOR%';"
cursor.execute(query3)

# Fetch all the results
results3 = cursor.fetchall()

# Print the results
print("Case numbers for crimes involving minors:")
for row in results3:
    print(row[0])
print ("-----------------------------------------------")


#----------------------------------------------------------------

### Problem 4

##### List all kidnapping crimes involving a child?
#----------------------------------------------------------------
# Execute the query
query4 = "SELECT case_number FROM CHICAGO_CRIME_DATA WHERE primary_type = 'KIDNAPPING' AND description LIKE '%CHILD%';"
cursor.execute(query4)

# Fetch all the results
results4 = cursor.fetchall()

# Print the results
print("Case numbers for kidnapping crimes involving a child:")
for row in results4:
    print(row[0])
print ("-----------------------------------------------")


#----------------------------------------------------------------

### Problem 5

##### What kinds of crimes were recorded at schools?
#----------------------------------------------------------------
# Execute the query
query5 = "SELECT DISTINCT primary_type FROM CHICAGO_CRIME_DATA WHERE location_description LIKE '%SCHOOL%';"
cursor.execute(query5)

# Fetch all the results
results5 = cursor.fetchall()

# Print the results
print("Types of crimes recorded at schools:")
for row in results5:
    print(row[0])
print ("-----------------------------------------------")


#----------------------------------------------------------------

### Problem 6

##### List the average safety score for each type of school.
#----------------------------------------------------------------
# Execute the query
query6 = "SELECT `Elementary, Middle, or High School`, AVG(safety_score) FROM CHICAGO_PUBLIC_SCHOOLS GROUP BY `Elementary, Middle, or High School`;"
cursor.execute(query6)

# Fetch all the results
results6 = cursor.fetchall()

# Print the results
print("Average safety score for each type of school:")
for row in results6:
    print(f"School Type: {row[0]}, Average Safety Score: {row[1]}")

print ("-----------------------------------------------")
#----------------------------------------------------------------


### Problem 7

##### List 5 community areas with highest % of households below poverty line
#----------------------------------------------------------------
# Execute the query
query7 = "SELECT community_area_name, percent_households_below_poverty FROM CENSUS_DATA ORDER BY percent_households_below_poverty DESC LIMIT 5;"
cursor.execute(query7)

# Fetch all the results
results7 = cursor.fetchall()

# Print the results
print("Community areas with the highest percentage of households below the poverty line:")
for row in results7:
    print(f"Community Area: {row[0]}, Percentage of Households Below Poverty Line: {row[1]}")

print ("-----------------------------------------------")
#----------------------------------------------------------------



### Problem 8

##### Which community area is most crime prone?
#----------------------------------------------------------------
# Execute the query
query8 = "SELECT community_area_number, COUNT(*) as crime_count FROM CHICAGO_CRIME_DATA GROUP BY community_area_number ORDER BY crime_count DESC LIMIT 1;"
cursor.execute(query8)

# Fetch the result
result8 = cursor.fetchone()

# Get the community area with the highest crime count
most_crime_prone_area = result8[0]
crime_count = result8[1]

# Print the result
print("The most crime-prone community area:")
print(f"Community Area: {most_crime_prone_area}, Crime Count: {crime_count}")

print ("-----------------------------------------------")
#----------------------------------------------------------------

### Problem 9

##### Use a sub-query to find the name of the community area with highest hardship index
#----------------------------------------------------------------
# Execute the query using a subquery
query9 = "SELECT community_area_name FROM CENSUS_DATA WHERE hardship_index = (SELECT MAX(hardship_index) FROM CENSUS_DATA);"
cursor.execute(query9)

# Fetch the result
result9 = cursor.fetchone()

# Get the community area with the highest hardship index
highest_hardship_area = result9[0]

# Print the result
print("Community area with the highest hardship index:")
print(f"Community Area: {highest_hardship_area}")
print ("-----------------------------------------------")
#----------------------------------------------------------------


### Problem 10

##### Use a sub-query to determine the Community Area Name with most number of crimes?
#----------------------------------------------------------------
# Execute the query using a subquery and matching community area numbers
query10 = """
SELECT cd.community_area_name
FROM CENSUS_DATA AS cd
JOIN (
    SELECT community_area_number, COUNT(*) AS count
    FROM CHICAGO_CRIME_DATA
    GROUP BY community_area_number
    ORDER BY count DESC
    LIMIT 1
) AS cc ON cd.community_area_number = cc.community_area_number;
"""
cursor.execute(query10)

# Fetch the result
result10 = cursor.fetchone()

# Get the community area name with the most number of crimes
most_crime_area = result10[0]

# Print the result
print("Community area with the most number of crimes:")
print(f"Community Area: {most_crime_area}")

print ("-----------------------------------------------")
#----------------------------------------------------------------

# Close the database connection
conn.close()
