import psycopg2

conn = psycopg2.connect("host='localhost' port='5432' dbname='mydb' user='myuser' password='mysecretpassword'")
cur = conn.cursor()

#open the file
file = open(r'./customer_transactions.csv', 'r')
tablename = "customer_transactions"

firstLine = file.readline().strip()

columns = firstLine.split(",")

# Build SQL code to drop table if exists and create table
sqlQueryCreate = """ DROP TABLE IF EXISTS """+ tablename + """;\n"""
sqlQueryCreate += """ CREATE TABLE """+ tablename + " ( \""""

# Define columns for table
for column in columns:
    sqlQueryCreate += column + "\" VARCHAR(64),\n \""

sqlQueryCreate = sqlQueryCreate[:-4]
sqlQueryCreate += ");"
print(sqlQueryCreate)
cur.execute(sqlQueryCreate)

#copy from file
cur.copy_from(file,"customer_transactions", sep=",")

file.close()
conn.commit()
conn.close()
