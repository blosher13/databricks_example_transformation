-- Databricks notebook source
-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/databricks_exercise-2.csv"
-- MAGIC file_type = "csv"
-- MAGIC
-- MAGIC # CSV options
-- MAGIC infer_schema = "true"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC
-- MAGIC # create dataframe
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC
-- MAGIC # create table from dataframe
-- MAGIC # permanent_table_name = "users_raw"
-- MAGIC
-- MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)

-- COMMAND ----------

-- CREATE DATABASE test;

CREATE OR REPLACE TABLE test.users_raw (
  user_id STRING,
  user_first_touch_timestamp LONG,
  email STRING,
  updated TIMESTAMP
);

COPY INTO test.users_raw
FROM 'dbfs:/user/hive/warehouse/users_raw'
FILEFORMAT = PARQUET;

-- COMMAND ----------

USE SCHEMA test;
SELECT * FROM users_raw;

-- COMMAND ----------

SELECT
  count(*) as total_rows,
  count(user_id),
  count(user_first_touch_timestamp),
  count(email),
  count(updated)
FROM users_raw

-- COMMAND ----------

select count(*) from users_raw where email is null

-- COMMAND ----------

select count(*), * from users_raw
group by user_id, user_first_touch_timestamp, email, updated
having count(*) > 1;

-- COMMAND ----------

select distinct(*) from users_raw

-- COMMAND ----------

select * from users_raw where user_id = 'UA000000107383284';

-- COMMAND ----------

select
  count(distinct(user_id)) as distinct_user_ids,
  count(user_id) as non_null_user_ids,
  count(user_id) - count(distinct(user_id)) as difference
from users_raw;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_clean AS 
SELECT
  user_id,
  user_first_touch_timestamp,
  max(email) AS email,
  max(updated) AS updated
FROM users_raw
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT * FROM users_clean;

-- COMMAND ----------

select count(*), * from users_raw
group by user_id, user_first_touch_timestamp, email, updated
having count(*) > 1;

-- COMMAND ----------

select * from users_clean where user_id = 'UA000000107383284';

-- COMMAND ----------

select
  count(distinct(user_id)) as distinct_user_ids,
  count(user_id) as non_null_user_ids,
  count(user_id) - count(distinct(user_id)) as difference
from users_clean;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_clean2 AS (
with new_date_format as (
  SELECT
    *,
    user_first_touch_timestamp / 1e6 AS first_touch1,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch_timestamp
  FROM users_clean
)
select
  user_id,
  email,
  updated,
  first_touch_timestamp,
  date_format(first_touch_timestamp, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch_timestamp, "HH:mm:ss") AS first_touch_time
from new_date_format
);
select * from users_clean2

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_clean3 AS
select
  *,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
from users_clean2

-- COMMAND ----------

select * from users_clean3
