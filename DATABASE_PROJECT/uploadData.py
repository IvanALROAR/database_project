import mysql.connector
import csv

cnx = mysql.connector.connect(user="root", password="", database="mercatoanalisis")

cursor = cnx.cursor()

csv_data = csv.reader(open("./preus.csv"))

next(csv_data)

for row in csv_data:
    cursor.execute('INSERT INTO preus VALUES(%s,%s,%s, %s, %s, %s, %s, %s)', (row[2], row[0], row[1], row[3], row[4], row[5], row[6], row[7]))

cnx.commit()
cursor.close()

print("Done")