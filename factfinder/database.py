import sqlite3
from pathlib import Path


class Database:
    """sqlite3 database class that holds testers jobs"""

    def __init__(self, database):
        """Initialize db class variables"""
        self.conn = sqlite3.connect(database)
        self.c = self.conn.cursor()

    def close(self):
        """close sqlite3 connection"""
        self.conn.close()

    def execute(self, query):
        """execute a row of data to current cursor"""
        self.c.execute(query)

    def commit(self):
        """commit changes to database"""
        self.conn.commit()

    def init_tables(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS pff_{0} (
            geotype text,
            geogname text,
            geoid text,
            dataset text,
            variable text,
            c double precision,
            e double precision,
            m double precision,
            p double precision,
            z double precision
        );
        """
        self.execute(query.format("social"))
        self.execute(query.format("demographic"))
        self.execute(query.format("economic"))
        self.execute(query.format("housing"))
        self.commit()
