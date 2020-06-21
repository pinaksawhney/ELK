import psycopg2
import datetime
from time import sleep
import logging
import logstash


""" 
Class to perform postgresql operations including initializing the connection, 
creating the table and writing to the table
"""


class PostGresGenerator:
    connection = None
    cursor = None
    tablename = ""
    col1 = ""
    col2 = ""
    Id = 0
    host = 'localhost'

    """
    Initialize the table name, table columns and connection
    """

    def __init__(self):
        self.connection, self.cursor = self.connect()
        self.tablename = "events"
        self.col1 = "event_id"
        self.col2 = "event_timestamp"
        self.logger = logging.getLogger('python-logstash-logger')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logstash.LogstashHandler(self.host, 5959, version=1))


    """
    Connects to the the Table and returns an Array of connection properties
    :return [connection, cursor]
    """

    def connect(self):
        try:
            connection = psycopg2.connect(user="postgres",
                                          password="postgres",
                                          host="127.0.0.1",
                                          port="5432",
                                          database="postgres_db")

            cursor = connection.cursor()
            return [connection, cursor]

        except (Exception, psycopg2.Error) as error:
            self.logger.error('Error while connecting to PostgreSQL {}.'.format(error))

    """
    Creates the table in table postgres database
    """

    def create_table(self):
        try:
            create_table_query = '''CREATE TABLE ''' + self.tablename + ''' (event_id INT PRIMARY KEY NOT NULL, event_timestamp  TEXT   NOT NULL);'''
            self.cursor.execute(create_table_query)
            self.connection.commit()
            self.logger.info("Table created successfully in PostgreSQL")

        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Error while creating PostgreSQL table {}".format(error))

    """
    Writes a record into the table
    :arg curr_ID: current value of the event_id
    :arg curr_timestamp: current timestamp
    """

    def write_to_table(self, curr_ID, curr_timestamp):
        try:
            postgres_insert_query = """ INSERT INTO """ + self.tablename + """ (""" + self.col1 + """,""" + self.col2 + """) VALUES (%s,%s)"""
            record_to_insert = (curr_ID, curr_timestamp)
            self.cursor.execute(postgres_insert_query, record_to_insert)

            self.connection.commit()
            count = self.cursor.rowcount
            self.logger.info("{0} rows Insertrd".format(count))
            self.logger.info("{0}, {1} :record inserted successfully into events table".format(curr_ID, curr_timestamp))

        except (Exception, psycopg2.Error) as error:
            if self.connection:
                self.logger.error("Failed to insert record into events table {}".format(error))


"""
Main driver program
"""
if __name__ == "__main__":
    pgresg = PostGresGenerator()
    pgresg.connect()
    pgresg.create_table()
    # loop every 1 second
    while 1:
        pgresg.write_to_table(pgresg.Id, datetime.datetime.utcnow())
        pgresg.Id += 1
        sleep(1)
