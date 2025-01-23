import sys
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

        d.cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {schema}.{table}")
            .format(schema=sql.Identifier(d.schema), table=sql.Identifier(d.joined_table))
        )
        initial_customers_count = cursor.fetchone()[0]
        print(f"Initial rows count of 'customers' table{initial_customers_count}")
        # ROW_NUMBER() to assign a unique row number to each duplicate group
        """
        WITH cte AS (
            SELECT 
                order_id,
                ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS row_num
            FROM orders
        )
        order_id customer_id   order_date    order_amount row_num
            1        101  2025-01-01 10:00:00   100.00       1
            3        101  2025-01-01 12:00:00   150.00       2
            5        101  2025-01-02 10:00:00   120.00       3
            2        102  2025-01-01 11:00:00   200.00       1
            6        102  2025-01-02 11:00:00   220.00       2
            4        103  2025-01-01 13:00:00   250.00       1
        """
        d.cursor.execute(sql.SQL("""
            WITH cte AS (
                SELECT 
                    event_time,
                    event_type,
                    product_id,
                    price,
                    user_id,
                    user_session,
                    ROW_NUMBER() OVER (
                        PARTITION BY event_time, event_type, product_id, price, user_id, user_session
                        ORDER BY event_time
                    ) AS row_num
                FROM {schema}.{table}
            )
            DELETE FROM {schema}.{table}
            WHERE (event_time, event_type, product_id, price, user_id, user_session) IN (
                SELECT event_time, event_type, product_id, price, user_id, user_session
                FROM cte
                WHERE row_num > 1
            );
        """).format(
            schema=sql.Identifier(d.schema),
            table=sql.Identifier(d.joined_table)
        ))
        d.conn.commit()
        print("Duplicates successfully removed from the customers table.")

        d.cursor.execute(
            sql.SQL("SELECT COUNT(*) FROM {schema}.{table}")
            .format(schema=sql.Identifier(d.schema), table=sql.Identifier(d.joined_table))
        )
        customers_count = cursor.fetchone()[0]
        print(f"Rows count of 'customers' table after purge{customers_count}")

        d.cursor.execute(sql.SQL("""
        SELECT event_time, event_type, product_id, price, user_id, user_session, COUNT(*) 
        FROM {schema}.{table}
        GROUP BY event_time, event_type, product_id, price, user_id, user_session
        HAVING COUNT(*) > 1;
        """).format(
            schema=sql.Identifier(d.schema),
            table=sql.Identifier(d.joined_table)
        ))
        duplicate_rows = d.cursor.fetchall()

        if duplicate_rows:
            print("Duplicates still exist after the operation. Details:")
            for row in duplicate_rows:
                print(row)
        else:
            print("No duplicates found. Verification passed!")

    except Exception as e:
        print(f"An error occurred: {e}")
        d.conn.rollback()
        sys.exit(1)
    finally:
        d.close()
