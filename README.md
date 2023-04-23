# Credit Card Fraud Pipeline Subscription End-to-End Data Pipeline

## Bussiness Understanding

Credit score is an important metric for banks to rate the credit performance of their applicants. 
They use personal information and financial records of credit card applicants to predict whether these applicants will default in the future or not. 
From these predictions, the banks will then decide if they want to issue credit cards to these applicants or not. 
The banks are asking us to create an end-to-end pipeline to help them handle this problem. 
The original datasets and data dictionary can be found in [here](https://www.kaggle.com/datasets/rikdifos/credit-card-approval-prediction).

## Problem Statements

Financial institution is experiencing challenges in managing and analyzing its large volume of credit card applicant data. This makes it difficult to mitigate fraud from credit card applicant data.

## Goal
To mitigate the possibility of fraud on credit card applicant, a data pipeline is created to facilitate data analysis and reporting application record.

## Obejective
The objectives of this projects are described below:
- Design and create end-to-end data pipeline with lambda architecture 
- Create a data warehouse that can integrate all the credit card applicant data from different sources and provide a single source of truth for the institution's analytics needs
- Create a visualization dashboard to get insights from the data, which can be used for business decisions and reach goal from this project.

## Data Pipeline
![image](https://user-images.githubusercontent.com/108534539/230115233-4fb03230-53f4-4e25-a70d-11cbd7beb4c8.png)

## Tools

- Cloud : Google Cloud Platform
- Infrastructure as Code : Terraform
- Containerization : Docker, Docker Compose
- Compute : Virtual Machine (VM) instance
- Stream Processing : Kafka
- Orchestration: Airflow
- Transformation : Spark, dbt
- Data Lake: Google Cloud Storage
- Data Warehouse: BigQuery
- Data Visualization: Looker
- Language : Python

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
### Clone this repository and enter the directory
```bash
git clone https://github.com/archie-cm/final-project-credit-card-fraud-pipeline.git && cd final-project-credit-card-fraud-pipeline
```


### Create a file named "service-account.json" containing your Google service account credentials and copy file to dbt folder
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
### Cloud Resource Provisioning with Terraform

1. Install `gcloud` SDK, `terraform` CLI, and create a GCP project. Then, create a service account with **Storage Admin**, **Storage Pbject Admin**, and **BigQuery Admin** role. Download the JSON credential and store it on `service-account.json`. Open `terraform/main.tf` in a text editor, and fill your GCP's project id.

2. Enable IAM API and IAM Credential API in GCP.

3. Change directory to `terraform` by executing
```
cd terraform
```

4. Initialize Terraform (set up environment and install Google provider)
```
terraform init
```
5. Plan Terraform infrastructure creation
```
terraform plan
```
6. Create new infrastructure by applying Terraform plan
```
terraform apply
```
7. Check GCP console to see newly-created resources.

### Batch Pipeline

1. Setting dbt in profiles.yml

2. Create batch pipeline with Docker Compose
```bash
sudo docker-compose up
```
3. Open Airflow with username and password "airflow" to run the DAG
```
localhost:8090
```

![image](https://user-images.githubusercontent.com/108534539/230137434-ca2e097f-2003-4cf1-8578-a1bc69c0f73d.png)

![image](https://user-images.githubusercontent.com/108534539/231919353-46fb7526-6c9e-4bce-a2f1-752ef3c02012.png)


4. Open Spark to monitor Spark master and Spark workers
```
localhost:8080
```
![image](https://user-images.githubusercontent.com/108534539/230136347-1fe5de5e-3585-4b04-8665-a14512f0efe3.png)


### Streaming Pipeline

1. Enter directory kafka
```bash
cd kafka
```

2. Create streaming pipeline with Docker Compose
```bash
sudo docker-compose up
```

3. Install required Python packages
```bash
pip install -r requirements.txt
```

4. Run the producer to stream the data into the Kafka topic
```bash
python3 producer.py
```

5. Run the consumer to consume the data from Kafka topic and load them into BigQuery
```bash
python3 consumer.py
```

![Screenshot (190)](https://user-images.githubusercontent.com/108534539/230141794-eb04880c-bf5e-4566-aa94-cbe8501e6e3f.png)

6. Open Confluent to view the topic
```
localhost:9021
```
![image](https://user-images.githubusercontent.com/108534539/230141014-bb9ef28b-af25-4fa8-b49a-ce5ef8f69aa2.png)

7. Open Schema Registry to view the active schemas
```
localhost:8081/schemas
```
![image](https://user-images.githubusercontent.com/108534539/230141266-c959f01b-b51e-4dc4-8adf-39cd820f466a.png)

