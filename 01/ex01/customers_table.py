import os
import sys
import csv
import psycopg2
from psycopg2 import sql


def get_csv_files(csv_dir: str) -> list[str]:
    """ Get a list of CSV files in a directory """
    try:
        files = os.listdir(csv_dir)
        return [os.path.splitext(f)[0] for f in files if f.endswith(".csv")]
    except FileNotFoundError:
        print(f"Error: The directory '{csv_dir}' does not exist.")
        return None
    except PermissionError:
        print(f"Error: Permission denied to access the directory '{csv_dir}'.")
        return None


class database:
    def __init__(self, csv_dir: str):
        self.db_name = "piscineds"
        self.db_user = "minh-ngu"
        self.db_password = "mysecretpassword"
        self.db_host = "localhost"
        self.schema = "minh_ngu_schema"
        self.csv_dir = csv_dir
        self.conn = None
        self.cursor = None
        self.joined_table = "customers"
        self.files = get_csv_files(self.csv_dir)

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

    def drop_table(self, table_name: str) -> None:
        """ Drop a table from the database """
        try:
            print(f"Dropping table {self.schema}.{table_name} ...")
            query = sql.SQL(
                "DROP TABLE IF EXISTS {schema}.{table} CASCADE;"
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier(table_name)
            )
            self.cursor.execute(query)
            self.conn.commit()
            print(f"Table {self.schema}.{table_name} dropped.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error dropping table {self.schema}.{table_name}: {e}")

    def join_tables(self) -> None:
        """ Join tables in the database """
        try:
            if self.table_exists(self.joined_table):
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) FROM {schema}.{table}")
                        .format(schema=sql.Identifier(self.schema), table=sql.Identifier(self.joined_table))
                    )
                    initial_customers_count = cursor.fetchone()[0]
                    print(f"Initial rows count of 'customers' table: {initial_customers_count}")

            table_row_counts = {}
            base_query = []

            for f in self.files:
                if not self.table_exists(f):
                    print(f"Error: Table {self.schema}.{f} does not exist.")
                    return
                
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL("SELECT COUNT(*) FROM {schema}.{table}")
                        .format(schema=sql.Identifier(self.schema), table=sql.Identifier(f))
                    )
                    table_row_counts[f] = cursor.fetchone()[0]

                print(f"Rows count of '{self.schema}.{f}': {table_row_counts[f]}")

                base_query.append(
                    sql.SQL("SELECT * FROM {schema}.{table}").format(
                        schema=sql.Identifier(self.schema),
                        table=sql.Identifier(f),
                    )
                )
            combined_query = sql.SQL(" UNION ALL ").join(base_query)
            final_query = sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {schema}.{joined_table} AS {union_query}
                """
            ).format(
                schema=sql.Identifier(self.schema),
                joined_table=sql.Identifier(self.joined_table),
                union_query=combined_query,
            )
            with self.conn.cursor() as cursor:
                cursor.execute(final_query)
                self.conn.commit()
            print(f"Successfully joined tables into {self.joined_table}'.")
            with self.conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL("SELECT COUNT(*) FROM {schema}.{joined_table}")
                    .format(schema=sql.Identifier(self.schema), joined_table=sql.Identifier(self.joined_table))
                )
                joined_table_count = cursor.fetchone()[0]

            print(f"Total rows in joined table '{self.joined_table}': {joined_table_count}")
            print(f"Sum of individual table row counts:{sum(table_row_counts.values())}")

            if joined_table_count == sum(table_row_counts.values()):
                print("Row count verification passed!")
            else:
                print("Row count verification failed!")
        except Exception as e:
            print(f"Error joining tables: {e}")
            self.drop_table(self.joined_table)

    def close(self):
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <csv_dir>")
        sys.exit(1)
    if not os.path.isdir(sys.argv[1]):
        print(f"Error: '{sys.argv[1]}' is not a directory.")
        sys.exit(1)
    d = database(sys.argv[1])
    if d.files is None:
        sys.exit(1)
    if len(d.files) == 0:
        print(f"No csv files found in '{sys.argv[1]}'.")
        sys.exit(1)
    try:
        d.connect()
        d.join_tables()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        d.close()
