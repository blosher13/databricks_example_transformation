# How to clean & transform data with Databricks using SQL

### Link to medium article/blog here: 

### Cleaning and transforming data is a highly important skill for data engineers and is vital for both Business Intelligence (BI) and Artificial Intelligence (AI). Although there are many tools that are used to clean and transformed data, it can get complicated very easily and so GUI (Graphical User Interface) based transformation tools are usually not the best idea. Databricks provides the use of "notebooks" to grammatically transform data in a logical top-to-bottom approach using either SQL, Python, or Scala. 

### In this project I will illustrate how one can use databricks to clean and transform data using Spark SQL using a practical example dataset with the following schema:

| field | type | description |
|---|---|---|
| user_id | string | unique identifier |
| user_first_touch_timestamp | long | time at which the user record was created in microseconds since epoch |
| email | string | most recent email address provided by the user to complete an action |
| updated | timestamp | time at which this record was last updated |

### This dataset contains 990 rows and 4 columns
