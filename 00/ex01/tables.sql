CREATE SCHEMA minh_ngu_schema AUTHORIZATION "minh-ngu";

-- Connect to your database
\c piscineds;

-- Table for `data_2022_dec.csv`
CREATE TABLE minh_ngu_schema.data_2022_dec (
    event_time TIMESTAMP,
    event_type TEXT,
    product_id INT,
    price NUMERIC(10, 2),
    user_id INT,
    user_session UUID
);

-- The TEXT type can store strings of any length, constrained only by the database's maximum size for a row, which is 1 GB.

-- INT is an 8-byte signed integer.
-- Minimum Value: -9,223,372,036,854,775,808 (-2^63)
-- Maximum Value: 9,223,372,036,854,775,807 (2^63 - 1)

-- NUMERIC(10, 2) is a numeric data type that can store numbers with up to 10 digits, 2 of which can be after the decimal point.

-- With NUMERIC(10, 2), the largest value you can store is 99,999,999.99.
-- The smallest positive value is 0.01.
-- Negative values are allowed, so the range is -99,999,999.99 to 99,999,999.99.


-- Table for `data_2022_nov.csv`
CREATE TABLE minh_ngu_schema.data_2022_nov (
    event_time TIMESTAMP,
    event_type TEXT,
    product_id INT,
    price NUMERIC(10, 2),
    user_id INT,
    user_session UUID
);

-- Table for `data_2022_oct.csv`
CREATE TABLE minh_ngu_schema.data_2022_oct (
    event_time TIMESTAMP,
    event_type TEXT,
    product_id INT,
    price NUMERIC(10, 2),
    user_id INT,
    user_session UUID
);

-- Table for `data_2023_jan.csv`
CREATE TABLE minh_ngu_schema.data_2023_jan (
    event_time TIMESTAMP,
    event_type TEXT,
    product_id INT,
    price NUMERIC(10, 2),
    user_id INT,
    user_session UUID
);
