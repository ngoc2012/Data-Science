
CREATE DATABASE piscineds;
CREATE ROLE "minh-ngu" WITH LOGIN PASSWORD 'mysecretpassword';
GRANT ALL PRIVILEGES ON DATABASE piscineds TO "minh-ngu";


-- Configure PostgreSQL for Remote Access (Optional)
-- If you need to connect to PostgreSQL remotely:
-- 1. Edit the `postgresql.conf` file:
--    ```bash
--    sudo nano /etc/postgresql/<version>/main/postgresql.conf
--    ```
--    Look for the line `listen_addresses` and change it to:
--    ```plaintext
--    listen_addresses = '*'
--    ```

-- 2. Edit the `pg_hba.conf` file:
--    ```bash
--    sudo nano /etc/postgresql/<version>/main/pg_hba.conf
--    ```
--    Add a line to allow connections (e.g., for all IPv4 addresses):
--    ```plaintext
--    host    all             all             0.0.0.0/0            md5
--    ```

-- 3. Restart PostgreSQL:
--    sudo systemctl restart postgresql
