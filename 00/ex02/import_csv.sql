-- The copy command is restricted to superusers or users with the pg_read_server_files role when used to read files from the server. 
-- The \copy command, however, works for all users since it runs the operation through the client.

-- Load data into `data_2022_dec`
\copy minh_ngu_schema.data_2022_dec (event_time, event_type, product_id, price, user_id, user_session) FROM '/tmp/subject/customer/data_2022_dec.csv' DELIMITER ',' CSV HEADER NULL AS '';

-- Load data into `data_2022_nov`
\copy minh_ngu_schema.data_2022_nov (event_time, event_type, product_id, price, user_id, user_session) FROM '/tmp/subject/customer/data_2022_nov.csv' DELIMITER ',' CSV HEADER NULL AS '';

-- Load data into `data_2022_oct`
\copy minh_ngu_schema.data_2022_oct (event_time, event_type, product_id, price, user_id, user_session) FROM '/tmp/subject/customer/data_2022_oct.csv' DELIMITER ',' CSV HEADER NULL AS '';

-- Load data into `data_2023_jan`
\copy minh_ngu_schema.data_2023_jan (event_time, event_type, product_id, price, user_id, user_session) FROM '/tmp/subject/customer/data_2023_jan.csv' DELIMITER ',' CSV HEADER NULL AS '';
