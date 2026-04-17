## Table of contents
- [Overview](#overview)
- [The dataset](#the-dataset)
- [Data modeling](#data-modeling)
- [Technologies](#technologies)
- [Reproducibility](#reproducibility)
    - [1. Prerequisites](#1-prerequisites)
    - [2. Setting up cloud environment](#2-setting-up-cloud-environment)
    - [3. Setting up and run Airflow](#3-setting-up-and-run-airflow)
    - [4. Run the DAG](#4-run-the-dag)
    - [5. Visualization](#5-visualization)


## Overview
This project demonstrates how to perform an end-to-end data pipeline on the GCP platform setup with Pulumi as IaC tool, orchestration by Airflow, processing with PySpark, and showcase in Looker.

The goal is to visualize the distribution of stock prices in the S&P500 from the public dataset. We can learn more from the data by responding to some of the following business questions on the final dashboard like:

- What is the value of the S&P500 index at the last day of each month?

- The distribution of companies across different sectors.

- What is the average adjusted closing price over a specified period of time?

The following diagram illustrates a high-level structure of the pipeline where data flows from the data sources to the visualization tool.

![The ELT](/images/design.png "Made with draw.io")

## The dataset
Source: [S&P 500 Stocks from Kaggle](https://www.kaggle.com/datasets/andrewmvd/sp-500-stocks/versions/1020)<br>
We are going to process 3 CSV files along this project.

1. __sp500_index.csv__ contains the index scores from 2014 to 2024.

2. __sp500_companies.csv__ contains all the companies's information (might be >500 because the index includes multiple classes of stock of some constituent companies).

3. __sp500_stocks.csv__ represents collected daily stock prices for each row including:
   - Date: The trading date.
   - Symbol: The stock ticker symbol.
   - Open, High, Low, Close: The stock’s opening, highest, lowest, and closing prices for the day.
   - Adj Close: The adjusted closing price, which accounts for stock splits and dividends.
   - Volume: The number of shares traded that day.

In total, the S&P500 stock data contains 1,891,034 entries, with 502 companies and 2,516 index scores.

## Data modeling
This project is just a simple demonstration of a batch data pipeline, so the modeling is straightforward. We can directly process the data from the source tables to the correspondent fact tables in BigQuery to be used by Looker later.

About optimization in the Data Warehouse, considering there are 2 data sources with time series structure (index and stocks), the source table for these 2 tables will be partitioned for faster queries.

| Source | sp500_index | sp500_companies | sp500_stocks |
| ------- | --- | --- | --- |
| Fact table name| fact_monthly_idx_score | fact_sector_distribution | fact_daily_avg_adj_close |
| Partition field| Date| N/A | Date |

## Technologies
1. **[Pulumi](https://www.pulumi.com)**: an open-source tool that provides `Infrastructure as Code (IaC)`. This project uses pulumi-gcp, a Pulumi Google Cloud Platform (GCP) provider package, providing access to GCP and managing cloud resources via Python code.

2. **[Docker](https://www.docker.com)**: a set of platform as a service that containerizes software, allowing them to act the same way across multiple platforms. In this project, we will run Airflow on Docker.

3. **[Apache Airflow](https://airflow.apache.org/)**: an open-source tool to programmatically author, schedule and monitor workflows. The majority of data tasks in the project will be executed and monitored on Airflow.

4. **[Google Cloud Storage](https://cloud.google.com/storage)** (GCS): a global, secure, and scalable object or blob store for mutable, unstructured data. We will store all the raw data in this location. Also, the preprocessed data will be stored in S3 before being loaded to BigQuery.

5. **[BigQuery](https://cloud.google.com/bigquery)**: a fully managed and highly scalable data warehouse solution offered by Google Cloud. This will be our Data Warehouse and make the data available for the visualization tool from it.

6. **[Apache Spark](https://spark.apache.org/)**: an open-source software that can efficiently process Big Data in a distributed or parallel system. PySpark (Spark with Python) will be used to transform the raw data and then persist new data in the Data Warehouse.

7. **[Looker Studio](https://lookerstudio.google.com/)**: a business intelligence and data visualization platform that enables users to create interactive dashboards, reports, and data explorations by connecting to various data sources. We will build a dashboard with Looker to better visualize the processed data in BigQuery.


## Reproducibility
### 1. Prerequisites
In order to run the project smoothly locally (or in a VM), prepare these necessary dependencies:
- GCP account with sufficient permissions to access and work on IAM, GCS, BigQuery, Cloud Composer (optional) and Looker Studio.
- Install [Google Cloud CLI](https://cloud.google.com/sdk/docs/install-sdk) and log in with an authorized credential (mainly for development purposes, not needed otherwise).
- Linux-based OS with Python and Jupiter Notebook installed (Windows WSL with VSCode used).
- A virtual python isolated environment ([venv](https://docs.python.org/3/library/venv.html) with python 3.12.3 used).
- Install Python packages for IDE suggestion use.
    ```bash 
    pip install -r requirements.txt
    ```
- Install [Pulumi](https://www.pulumi.com/docs/iac/download-install/#download-install-pulumi).
- Install [Docker](https://www.docker.com/products/docker-desktop/)<br>
*(To run docker with sudo privilege in Jupyter Notebook, restart and add --allow-root argument first*.
*If you use VSCode, check this [post](https://www.reddit.com/r/vscode/comments/ywcbeo/how_to_run_jupyter_notebooks_as_root))*.


### 2. Setting up cloud environment
We are going to use Pulumi to create the necessary infrastructure in Google Cloud.
Open the pulumi-iac folder as its own separate project (to bypass conflict between pip dependencies in the main project) and follow the instructions in [this notebook](/pulumi-iac/setup.ipynb).

After finished, we expected to have the following GCP resources:
- 1 service account with all the required roles to operate in GCP
- 1 GCS bucket as the data lake
- 1 BigQuery dataset as the data warehouse
- 1 Composer environment using Airflow for workflow orchestration __(optional)__

*Note: To run Spark jobs in Composer, it's recommended to integrate with [DataProc](https://cloud.google.com/dataproc) instance (which this project has not implemented yet)*.


### 3. Setting up and run Airflow
For demo purposes, we will not use Airflow with GCP Composer, unless we have an actual production environment to fully take advantage of the cloud computing paradigm, then we could sync the dags code like [this](./airflow/composer_sync.ipynb) (cause Composer do not support PySpark currently). So for this project, let's use Airflow in the local environment (or any VM) with Docker installed.
- Move to the `./airflow` directory
    ```bash
     cd airflow
    ```
- Follow the instructions in [this notebook](./airflow/init_setup.ipynb) to reinitialize a clean Airflow folder if needed or skip to the next steps.
- Prepare Airflow environment settings in the [config](./airflow/config/) directory:
    - Download and save the service-account.json from GCP for the service account created by Pulumi
    - Update __[conn.yml](./airflow/config/conn.yml)__ to add connections (not needed if run spark locally)
    - Update __[example.var.yml](./airflow/config/example.var.yml)__ with your own GCP resources created by Pulumi then rename it to __var.yml__
- Build custom Airflow [Docker](/airflow/Dockerfile) image
    ```bash
     make build
    ```
    This process might take up to 10 minutes or more depending on the internet speed. At this stage, Docker also installs Java (to run Spark) and several Python packages defined in the [requirements.txt](/airflow/requirements.txt).

- Use docker-compose to launch Airflow

    - Initialize Airflow (1st time only)
        ```bash
        docker-compose up airflow-init 
        ```
    - Launch Airflow
        ```bash
        make up
        ```
    This would launch `Postgres`, `Redis` internal database with `Airflow Scheduler`, `Airflow Worker`, `Airflow Triggerer` and `Airflow Webserver` as a fully functional Airflow deployment.


### 4. Run the DAG
Once Airflow is up and running, we can now proceed the DAG execution, the following screenshot shows a successful run of the sp500_dag:
![DAG](/images/success_dag.jpg "Graph view for the DAG run")

This DAG is implemented on the assumption that the data source gets updated every day so that the ingestion task (download then load on BigQuery) can only be executed 1 time per day, but the transformation task (use PySpark to read then write data onto BigQuery) still can be trigger manually for versatility.

More details of the tasks can be found in the comment from the [DAG code](/airflow/dags/sp500_dag.py).


### 5. Visualization
We can now connect to the BigQuery database from Looker Studio to visualize and interact with the data in multiple charts.
<!-- <iframe width="600" height="450" src="https://lookerstudio.google.com/embed/reporting/825104ec-a247-40a8-9497-c39707855405/page/JIIFF" frameborder="0" style="border:0" allowfullscreen sandbox="allow-storage-access-by-user-activation allow-scripts allow-same-origin allow-popups allow-popups-to-escape-sandbox"></iframe> -->

The final dashboard can be found [here](https://lookerstudio.google.com/embed/reporting/825104ec-a247-40a8-9497-c39707855405).

For personal reasons, the BigQuery dataset will expire the data after Apr 5, 2025, so the [PDF version](/visualization/S&P500_Demo.pdf) got exported for future preview if needed.
