"""
    Connection file for the postgreSQL database to write wind data to.
"""
import psycopg2

connection = None
cursor = None

def createConnection():
    ### --- Edit the connection info here ---
    connection = psycopg2.connect(user = "<username>",
                                password = "<password>",
                                host = "127.0.0.1",
                                port = "5432",
                                database = "noaa", 
                                async=False)
    ### --------------------------------------
    cursor = connection.cursor()
    return connection, cursor

def closeConnection():
    if(connection):
        cursor.close()
        connection.close()
