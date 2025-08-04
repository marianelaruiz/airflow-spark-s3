# Apache Airflow & Spark & Amazon S3(Simple Storage Service)

This repository is intended for orchestrating Spark applications using the Apache Airflow tool with AWS S3 storage.

## Table of Contents

1. [Technologies](#technologies)
2. [Install and Run](#install-and-run)
3. [Amazon S3 (Simple Storage Service) Configuration](#s3-simple-storage-service-configuration)
4. [Apache Airflow Webserver](#apache-airflow-webserver)
5. [Databricks Lakehouse Architecture](#databricks-lakehouse-architecture)

## 1. Technologies

A list of technologies used within the project:

* [Python](https://www.python.org): Version 3.12
* [Pyspark](https://spark.apache.org/docs/latest/api/python/index.html): Version 3.5.3
* [Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html): Version 2.10.2
* [Amazon S3 (Simple Storage Service)](https://aws.amazon.com/s3/): Cloud storage for data lake.

## 2. Install and Run

```bash
# Clone this repo
$ git clone https://github.com/marianelaruiz/airflow-spark-s3
# Enter the project folder
$ cd integracao-airflow-spark-s3
```

### Windows

```bash
# Create a virtual environment
$ python -m venv venv 

# Activate your virtual environment
$ venv\Scripts\activate


```

### MacOS & Linux

```bash
# Create a virtual environment
python3 -m venv venv # or virtualenv venv

# Activate your virtual environment
source venv/bin/activate

```
### Initial Setup and Initialize Airflow
   ```bash
  bash setup_airflow.sh
   ```
### 3. Amazon S3 Configuration
You need to create an S3 bucket for this project. Save the bucket name for the following steps.

Upload the JSON files provided in the `lakehouse/landing` folder of this repository to `lakehouse/landing` in your S3 bucket.


### Config and Run Apache Airflow
*Ensure that Apache Airflow is installed on your machine.*
Yo have two options:

1. **Create an Airflow User** *(optional)*:
   If you prefer to create a custom user:
   ```bash
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com
   ```
   NOTE: Update `S3_PATH` to use your bucket name

   ``` 
   cd airflow
   export S3_PATH="s3a://my-bucket-name/"
   export AIRFLOW_HOME=$(pwd)
   ```
   
   In a terminal:
   ```    
   airflow webserver --port 8080    
   ```
   In another terminal:
   ```    
   airflow scheduler
   ```

2. **Start Airflow** :
   For a quicker setup without creating a user, you can use the standalone mode:
   NOTE: Update `S3_PATH` to use your bucket name
   ```bash
   cd airflow
   export S3_PATH="s3a://my-bucket-name/"
   export AIRFLOW_HOME=$(pwd)
   airflow standalone
   ```
   > This command starts both the Airflow webserver and scheduler, and automatically creates an admin user. The username and password will be displayed in the terminal.

**Access the Airflow UI**:
   Open the browser and navigate to `http://localhost:8080` to access the Airflow UI.

### 3. Apache Airflow Webserver

Webserver will start at: `http://127.0.0.1:8080`

From here, you can access the address above in your browser and log in. 

The first configuration of the web server should be to change the host in Airflow. To do this, go to Admin > Connections > Search for "spark_default" > Change the "Host" field from "yarn" to "local" and save.

Then, you can go to the "Search Dags" field and search for `dag_lakehouse`. Click on the search result, and you will have access to the interface related to the created Airflow instance. You can execute it by clicking the "Trigger DAG" button in the upper right corner of the screen, where you can observe the execution order and whether the Spark applications were successful or not. 

You can check that everything ran succesfully by looking in your s3 bucket for the bronze, silver, and gold folders, where a subfolder called `parquet` will be created.

## 4. Databricks Lakehouse Architecture

The Medallion Architecture is a data design pattern used to logically organize data in a lakehouse, aimed at progressively enhancing the structure and quality of data as it flows through each layer of the architecture. The layers include Bronze (raw data), Silver (cleaned and transformed data), and Gold (data ready for analysis).
