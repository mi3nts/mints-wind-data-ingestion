import psycopg2
import pypsqlcon
import sys

print('Deleting one week old or older data')

try:
    connection, cursor = pypsqlcon.createConnection()
    cursor.execute("DELETE from wind_data WHERE recorded_time < now() - interval '7 days';")
    connection.commit()
except (Exception, psycopg2.Error) as error:
    print("Error while performing query", error)
    sys.exit(1)
finally:
    pypsqlcon.closeConnection()
