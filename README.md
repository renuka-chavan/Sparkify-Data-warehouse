
# **DATA WAREHOUSE PROJECT**

In this project, you'll build an ETL pipeline that extracts data from S3, stages data in Redshift, and transforms data into a set of dimensional tables for an analytics team.

## **Sparkify Data Warehouse on AWS Redshift**

In this project, you will build a cloud-based data warehouse for a music streaming startup called Sparkify. The data warehouse is designed to help the analytics team to understand user behavior and song play activity.

We filter event logs using page = 'NextSong' to ensure the fact table contains only actual song play events, excluding non-music interactions such as logins or settings changes.

### **The files included are:**

- `/sql_queries.py` - Defining SQL statements.

- `/create_tables.py` - Drops and creates staging and analytics tables.

- `/etl.py` - Load data from S3 into staging tables and transforms it into analytics tables.

- `/displayresult.py` - Runs analytical queries and displays results.

- `/dwh.cfg` - Configuration file (Redshift, IAM, S3 paths)

- `/README.md` - Project documentation.

- `/Output Screenshots` - Screenshots of the steps followed to complete the Project.

### **Steps to be followed:**

- Write SQL queries in sql_queries.py
- Drop and create staging and analytics tables by running create_tables.py
- Create an IAM Role
- Create Security Group
- Create an IAM User
- Create Access Key
- Create Cluster Subnet Group
- Launch Redshift Cluster
- Configure Host, DB_Name, DB_User, DB_Password, DB_Port From Cluster
- Configure ARN from IAM Role
- Configure Log_Data, Log_JSONPATH, Song_Data from S3(provided)
- Load Data from S3 into staging tables and transforms into analytics tables on Redshift Cluster by running etl.py
- Display analytic queries by running displayresult.py

### **Related informations:**
- Cluster Host endpoint 

```bash
  redshift-cluster-1.c0noncmht1ap.us-east-1.redshift.amazonaws.com
```
- IAM Role

```bash
  arn:aws:iam::425174427514:role/myRedshiftRole
```

- S3 LOG_DATA
```bash
  s3://udacity-dend/log-data
```

- S3 LOG_JSONPATH
```bash
  s3://udacity-dend/log_json_path.json
```

- S3 SONG_DATA
```bash
  s3://udacity-dend/song_data
```


