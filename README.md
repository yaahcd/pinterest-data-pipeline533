# Pinterest Data Pipeline

### Table of Contents
---
<ul>
<li>Description</li>
<li>Installation</li>
<li>Milestones & Tasks</li>
<li>File structure</li>
</ul>

### Description
---
This project simulates Pinterest's data pipeline. It utilizes AWS technologies such as EC2, S3, MSK, API Gateway, MWAA, and Kinesis, as well as Spark, Apache Kafka, and Databricks, for data cleaning and processing within the pipeline.

### Installation
---
To run this project:
<ul>
<li>Clone this repository:</li>
</ul>

```
https://github.com/yaahcd/multinational-retail-data-centralisation463.git
```


### Milestones & Tasks
---
1. Prepare  GitHub and AWS account to start the project.
2. Using the `user_posting_emulation` script, read data from the AWS RDS database using the provided credentials safely stored in a YAML file.
   
3. Configure and connect to the `AWS EC2` instance. Make the necessary configurations to connect to the `AWS MSK Cluster`, and get started with creating `Kafka` topics.
4. Configure and connect `AWS S3 Bucket` to `AWS MSK Cluster`.
5. Using `AWS API Gateway`, create a Kafka REST proxy integration method and deploy API. Configure the aforementioned API on the `AWS EC2` instance using the Confluent package. Then, use the `user_posting_emulation` script to send the data from Milestone 02 to the `AWS MSK Cluster` through `AWS API Gateway` requests. Each data record should be stored under the correct, previously created `Kafka` topic.
6. Mount the `AWS S3 bucket` from Milestone 4 to `Databricks` to read and prepare the data for cleaning.
7. Using `Spark`, clean the data and perform queries. Both the code for cleaning and querying the data can be found in the 'databricks' folder.\
&emsp;The queries performed were:\
&emsp; *- Find the most popular Pinterest category people post to based on their country.*\
&emsp; *- Find how many posts each category had between 2018 and 2022.*\
&emsp; *- Find the user with most followers in each country.*\
&emsp; *- Find the most popular category for different age groups.*\
&emsp; *- Find the median follower count for different age groups.*\
&emsp; *- Find how many users have joined between 2015 and 2020.*\
&emsp; *- Find the median follower count of users have joined between 2015 and 2020.*\
&emsp; *- Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.*
1. Create an `Airflow` DAG in the `AWS MWAA` environment to trigger a `Databricks` Notebook to run daily. The Python script for this DAG can be found in `0e03d1c30c91_dag`.
2. Create data streams using `AWS Kinesis Data Streams` and configure the previously created API to allow it to invoke the following Kinesis actions:    
&emsp; *- List streams*\
&emsp; *- Create, describe and delete streams*\
&emsp; *- Add records to streams*\
Then, using the `user_posting_emulation_streaming` script, send data to the corresponding `Kinesis Stream` through API requests. Once the data is sent, read it in a `Databricks` Notebook and perform the necessary data cleaning using `Spark`. Finally, write the cleaned data to `Delta Tables`.

### File structure
---

├── README.md\
├── 0e03d1c30c91_dag.py\
├── user_posting_emulation.py\
├── user_posting_emulation_streaming.py\
└── databricks\
&emsp;&emsp; ├── Milestone 6, task 2.py\
&emsp;&emsp; ├── Milestone 7, query tasks.py\
&emsp;&emsp; ├── Milestone 7, task 1.py\
&emsp;&emsp; ├── Milestone 7, task 2.py\
&emsp;&emsp; ├── Milestone 7, task 3.py\
&emsp;&emsp; └── Milestone 9, Databricks and AWS Kinesis.py
