To connect pgAdmin to a PostgreSQL database installed on your computer, follow these steps:

---

### 1. **Ensure PostgreSQL is Running**
Ensure that your PostgreSQL server is up and running on your computer.

1. Check the PostgreSQL service status:
   ```bash
   sudo systemctl status postgresql
   ```
2. Start it if it's not running:
   ```bash
   sudo systemctl start postgresql
   ```

---

### 2. **Open pgAdmin**
Launch pgAdmin using one of the following methods:
- **Web Mode**: Open a browser and go to `http://localhost/pgadmin4`.
- **Desktop Mode**: Launch pgAdmin from your applications menu or by typing `pgadmin4` in the terminal.

---

### 3. **Add a New Server Connection**
1. Open the pgAdmin dashboard.
2. Right-click on "Servers" in the left-hand navigation pane and select **Create > Server**.

---

### 4. **Configure the Server Connection**
In the **Create - Server** dialog, configure the connection:

#### General Tab:
- **Name**: Enter a name for your connection (e.g., `Local PostgreSQL`).

#### Connection Tab:
- **Host name/address**: Enter `localhost` (or `127.0.0.1`).
- **Port**: Enter `5432` (default PostgreSQL port).
- **Maintenance database**: Enter `postgres` (default maintenance database).
- **Username**: Enter your PostgreSQL username (default is `postgres`).
- **Password**: Enter the password for the specified username.

Click **Save**.

---

### 5. **Verify the Connection**
After saving, pgAdmin will attempt to connect to the PostgreSQL server. If successful, the new server will appear in the left-hand navigation pane under "Servers."

---

### 6. **Troubleshooting Connection Issues**
If the connection fails, check the following:

#### a. **PostgreSQL Configuration**
- Ensure the `postgresql.conf` file allows connections. Open it using:
  ```bash
  sudo nano /etc/postgresql/<version>/main/postgresql.conf
  ```
  Locate and set:
  ```conf
  listen_addresses = 'localhost'
  ```
  Replace `<version>` with your PostgreSQL version number.

#### b. **pg_hba.conf File**
- Ensure the `pg_hba.conf` file permits local connections. Open it using:
  ```bash
  sudo nano /etc/postgresql/<version>/main/pg_hba.conf
  ```
  Ensure it contains:
  ```conf
  # IPv4 local connections:
  host    all             all             127.0.0.1/32            md5
  # IPv6 local connections:
  host    all             all             ::1/128                 md5
  ```

#### c. **Restart PostgreSQL**
After making changes to the configuration files, restart PostgreSQL:
```bash
sudo systemctl restart postgresql
```

---

### 7. **Test the Connection Again**
Retry the connection in pgAdmin. If issues persist, let me know the error message for further assistance!