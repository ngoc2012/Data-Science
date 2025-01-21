import os
import sys
import csv
import psycopg2
from psycopg2 import sql


def validate_csv(csv_file: str) -> bool:
    """ Validate the integrity of a CSV file. """
    cols = [
        "product_id",
        "category_id",
        "category_code",
        "brand"
    ]
    try:
        if not os.path.isfile(csv_file):
            print(f"Error: '{csv_file}' is not a file.")
            return False
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                print(f"Error: '{csv_file}' is missing a header.")
                return False
            if len(header) != len(cols):
                print(f"Error: '{csv_file}' must have {len(cols)} columns.")
                return False
            if header != cols:
                print(f"Error: '{csv_file}' has incorrect column.")
                return False
        return True
    except Exception as e:
        print(f"Error :'{csv_file}' {e}")
        return False


class database:
    def __init__(self, csv_file: str):
        self.db_name = "piscineds"
        self.db_user = "minh-ngu"
        self.db_password = "mysecretpassword"
        self.db_host = "localhost"
        self.schema = "minh_ngu_schema"
        self.csv_file = csv_file
        self.table_name = os.path.splitext(os.path.basename(csv_file))[0]
        self.conn = None
        self.cursor = None

    def connect(self) -> None:
        """ Connect to the PostgreSQL database server """
        self.conn = psycopg2.connect(
            dbname=self.db_name,
            user=self.db_user,
            password=self.db_password,
            host=self.db_host
        )
        self.cursor = self.conn.cursor()

    def table_exists(self) -> bool:
        """ Check if a table exists in the database """
        query = sql.SQL(
            "SELECT EXISTS (SELECT FROM information_schema.tables \
WHERE table_schema = %s AND table_name = %s);"
        )
        self.cursor.execute(query, (self.schema, self.table_name))
        if self.cursor.fetchone()[0]:
            print(f"Table {self.schema}.{self.table_name} already exists.")
            return True
        return False

    def create_table(self) -> None:
        """ Create a new table in the database """
        print(f"Creating table {self.schema}.{self.table_name} ...")
        query = sql.SQL(
            """
            CREATE TABLE {schema}.{table} (
                product_id INT,
                category_id BIGINT,
                category_code TEXT,
                brand TEXT
            );
            """
        ).format(
            schema=sql.Identifier(self.schema),
            table=sql.Identifier(self.table_name))
        self.cursor.execute(query)
        self.conn.commit()
        print(f"Table {self.schema}.{self.table_name} created.")

    def import_csv(self) -> None:
        """ Import data from a CSV file into a table """
        table = self.schema + '.' + self.table_name
        try:
            with open(self.csv_file, 'r') as f:
                copy_query = sql.SQL(
                    """
                    COPY {schema}.{table} \
(product_id,category_id,category_code,brand)
                    FROM STDIN
                    DELIMITER ','
                    CSV HEADER NULL AS '';
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(self.table_name),
                )
                self.cursor.copy_expert(copy_query, f)
                self.conn.commit()
                print(f"{self.csv_file} imported into {table}.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error importing {self.csv_file} into table {table}: {e}")
            self.drop_table()

    def drop_table(self) -> None:
        """ Drop a table from the database """
        try:
            print(f"Dropping table {self.schema}.{self.table_name} ...")
            query = sql.SQL(
                "DROP TABLE IF EXISTS {schema}.{table} CASCADE;"
            ).format(
                schema=sql.Identifier(self.schema),
                table=sql.Identifier(self.table_name)
            )
            self.cursor.execute(query)
            self.conn.commit()
            print(f"Table {self.schema}.{self.table_name} dropped.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error dropping table {self.schema}.{self.table_name}: {e}")

    def close(self):
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <file_csv>")
        sys.exit(1)
    d = database(sys.argv[1])
    try:
        d.connect()
        if not d.table_exists() and validate_csv(d.csv_file):
            d.create_table()
            d.import_csv()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        d.close()
        sys.exit(0)
