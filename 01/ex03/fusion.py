import os
import sys
import csv
import psycopg2
from psycopg2 import sql


class database:
    def __init__(self):
        self.db_name = "piscineds"
        self.db_user = "minh-ngu"
        self.db_password = "mysecretpassword"
        self.db_host = "localhost"
        self.schema = "minh_ngu_schema"
        self.conn = None
        self.cursor = None
        self.joined_table = "customers"

    def connect(self) -> None:
        """ Connect to the PostgreSQL database server """
        self.conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host
        )
        self.cursor = self.conn.cursor()

    def table_exists(self, table_name: str) -> bool:
        """ Check if a table exists in the database """
        query = sql.SQL(
            "SELECT EXISTS (SELECT FROM information_schema.tables \
WHERE table_schema = %s AND table_name = %s);"
        )
        self.cursor.execute(query, (self.schema, table_name))
        if self.cursor.fetchone()[0]:
            print(f"Table {self.schema}.{table_name} already exists.")
            return True
        return False

    def close(self):
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 1:
        print(f"Usage: {sys.argv[0]}")
        sys.exit(1)
    d = database()
    try:
        d.connect()
        if d.table_exists("customers") is False:
            print("The customers table does not exist.")
            sys.exit(1)

        d.cursor.execute(sql.SQL("""
            ALTER TABLE {schema}.{table}
            ADD COLUMN IF NOT EXISTS category_id BIGINT,
            ADD COLUMN IF NOT EXISTS category_code TEXT,
            ADD COLUMN IF NOT EXISTS brand TEXT;
        """).format(
            schema=sql.Identifier(d.schema),
            table=sql.Identifier(d.joined_table)
        ))

        d.cursor.execute(sql.SQL("""
            UPDATE {schema}.{table}
            SET 
                category_id = {schema}.item.category_id,
                category_code = {schema}.item.category_code,
                brand = {schema}.item.brand
            FROM {schema}.item
            WHERE {schema}.{table}.product_id = {schema}.item.product_id;
        """).format(
            schema=sql.Identifier(d.schema),
            table=sql.Identifier(d.joined_table)
        ))
        d.conn.commit()
        print("Tables joined successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
        d.conn.rollback()
        sys.exit(1)
    finally:
        d.close()
