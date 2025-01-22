import os
import sys
import csv
import psycopg2
from psycopg2 import sql


def validate_csv(csv_file: str) -> bool:
    """ Validate the integrity of a CSV file. """
    expected_columns = [
        "event_time",
        "event_type",
        "product_id",
        "price",
        "user_id",
        "user_session",
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

    def create_table(self, table_name: str) -> None:
        """ Create a new table in the database """
        print(f"Creating table {self.schema}.{table_name} ...")
        query = sql.SQL(
            """
            CREATE TABLE {schema}.{table} (
                event_time TIMESTAMP,
                event_type TEXT,
                product_id INT,
                price NUMERIC(10, 2),
                user_id INT,
                user_session UUID NULL
            );
            """
        ).format(
            schema=sql.Identifier(self.schema),
            table=sql.Identifier(table_name))
        self.cursor.execute(query)
        self.conn.commit()
        print(f"Table {self.schema}.{table_name} created.")

    def import_csv(self, table_name: str, csv_file: str) -> None:
        """ Import data from a CSV file into a table """
        try:
            with open(csv_file, 'r') as f:
                copy_query = sql.SQL(
                    """
                    COPY {schema}.{table} (event_time, event_type, product_id, price, user_id, user_session)
                    FROM STDIN
                    DELIMITER ','
                    CSV HEADER NULL AS '';
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier(table_name),
                )
                self.cursor.copy_expert(copy_query, f)
                self.conn.commit()
                print(f"Data imported from {csv_file} into {self.schema}.{table_name}.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error importing CSV file {csv_file} into table {self.schema}.{table_name}: {e}")
            self.drop_table(table_name)

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

    def process_csv_files(self) -> None:
        """ Process CSV files in a directory """
        n = 0
        for table_name in self.files:
            csv_file = os.path.join(self.csv_dir, table_name + ".csv")
            if not self.table_exists(table_name) and validate_csv(csv_file):
                n += 1
                self.create_table(table_name)
                self.import_csv(table_name, csv_file)
        if n == 0:
            print("No new valid csv file found.")
            return
        print("All CSV files processed.")

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
        d.process_csv_files()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
    finally:
        d.close()
        sys.exit(0)
