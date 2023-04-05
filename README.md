# Credit Card Fraud Pipeline Subscription End-to-End Data Pipeline

## Bussiness Understanding

Credit score is an important metric for banks to rate the credit performance of their applicants. 
They use personal information and financial records of credit card applicants to predict whether these applicants will default in the future or not. 
From these predictions, the banks will then decide if they want to issue credit cards to these applicants or not. 
The banks are asking us to create an end-to-end pipeline to help them handle this problem. 
The original datasets and data dictionary can be found in [here](https://www.kaggle.com/datasets/rikdifos/credit-card-approval-prediction).

## Problem Statements

From said back story, we can conclude that the bank want to increase the efficiency of their campaign by targeting client with higher chance of success based on the feature from the data.

## Project Objectives
The objectives of this projects are described below:
- Create an automated pipeline to flow both batch and stream data from data sources to data warehouses and data marts.
- Create a visualization dashboard to get insights from the data, which can be used for business decisions.
- Create an infrastructure as code which makes the codes reusable and scalable for another projects.

## Data Pipeline
![image](https://user-images.githubusercontent.com/108534539/230115233-4fb03230-53f4-4e25-a70d-11cbd7beb4c8.png)

## Tools

- Orchestration: Airflow
- Compute : Virtual Machine (VM) instance
- Container : Docker
- Storage: Google Cloud Storage
- Warehouse: BigQuery
- Data Visualization: Looker

## Reproducibility
![Screenshot (189)](https://user-images.githubusercontent.com/108534539/230118957-612b63c8-4edd-4aaa-9700-92b439ff870a.png)

## Data Visualization Dashboard
![Screenshot (188)](https://user-images.githubusercontent.com/108534539/230117610-c579e654-8bf5-487b-be4f-f0354212f220.png)

![Screenshot (187)](https://user-images.githubusercontent.com/108534539/230117643-9577559c-ac6d-4e47-8dcf-4af817646479.png)


## Google Cloud Usage Billing Report
Data infrastructure we used in this project are entirely built on Google Cloud Platform with more or less 3 weeks of project duration, 
using this following services:
- Google Cloud Storage (pay for what you use)
- Virtual Machine (VM) instance (cost are based Vcpu & memory and storage disk)
- Google BigQuery (first terrabyte processed are free of charge)
- Google Looker Studio (cost is based from number of Looker Blocks (data models and visualizations), users, and the number of queries processed per month)
> Total cost around 6$ out of 300$ free credits that GCP provided

## Project Instruction
##### Clone this repository and enter the directory
```bash
git clone https://github.com/yevadrian/big-data-lambda-architecture && cd big-data-lambda-architecture
```


### Create a file named "service-account.json" containing your Google service account credentials
```json
{
  "type": "service_account",
  "project_id": "[PROJECT_ID]",
  "private_key_id": "[KEY_ID]",
  "private_key": "-----BEGIN PRIVATE KEY-----\n[PRIVATE_KEY]\n-----END PRIVATE KEY-----\n",
  "client_email": "[SERVICE_ACCOUNT_EMAIL]",
  "client_id": "[CLIENT_ID]",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/[SERVICE_ACCOUNT_EMAIL]"
}
```

##### Create batch pipeline with Docker Compose
```bash
sudo docker-compose up
```

##### Open Airflow with username and password "airflow" to run the DAG
```
localhost:8090
```

![image](https://user-images.githubusercontent.com/108534539/230137434-ca2e097f-2003-4cf1-8578-a1bc69c0f73d.png)

##### Open Spark to monitor Spark master and Spark workers
```
localhost:8080
```
![image](https://user-images.githubusercontent.com/108534539/230136347-1fe5de5e-3585-4b04-8665-a14512f0efe3.png)


##### Enter the directory Streaming pipeline
```bash
sudo docker-compose up
```

##### Create batch pipeline with Docker Compose
```bash
sudo docker-compose up
```

##### Install required Python packages
```bash
pip install -r requirements.txt
```

##### Run the producer to stream the data into the Kafka topic
```bash
python3 producer.py
```

##### Run the consumer to consume the data from Kafka topic and load them into BigQuery
```bash
python3 consumer.py
```

![Screenshot (190)](https://user-images.githubusercontent.com/108534539/230141794-eb04880c-bf5e-4566-aa94-cbe8501e6e3f.png)

##### Open Confluent to view the topic
```
localhost:9021
```
![image](https://user-images.githubusercontent.com/108534539/230141014-bb9ef28b-af25-4fa8-b49a-ce5ef8f69aa2.png)

##### Open Schema Registry to view the active schemas
```
localhost:8081/schemas
```
![image](https://user-images.githubusercontent.com/108534539/230141266-c959f01b-b51e-4dc4-8adf-39cd820f466a.png)

