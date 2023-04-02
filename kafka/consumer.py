from confluent_kafka.avro import AvroConsumer
from google.cloud import bigquery
import os 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/usr/.google/credentials/google_credentials.json"

dataset_name = 'application_record'
table_name = 'application_record_training'

client = bigquery.Client()
client.create_dataset(dataset_name, exists_ok=True)
dataset = client.dataset(dataset_name)

schema = [
    bigquery.SchemaField('ID', 'INT64'),
    bigquery.SchemaField('CODE_GENDER', 'STRING'),
    bigquery.SchemaField('FLAG_OWN_CAR', 'STRING'),
    bigquery.SchemaField('FLAG_OWN_REALTY', 'STRING'),
    bigquery.SchemaField('CNT_CHILDREN', 'INT64'),
    bigquery.SchemaField('AMT_INCOME_TOTAL', 'FLOAT64'),
    bigquery.SchemaField('NAME_INCOME_TYPE', 'STRING'),
    bigquery.SchemaField('NAME_EDUCATION_TYPE', 'STRING'),
    bigquery.SchemaField('NAME_FAMILY_STATUS', 'STRING'),
    bigquery.SchemaField('NAME_HOUSING_TYPE', 'STRING'),
    bigquery.SchemaField('DAYS_BIRTH', 'INT64'),
    bigquery.SchemaField('DAYS_EMPLOYED', 'INT64'),
    bigquery.SchemaField('FLAG_MOBIL', 'INT64'),
    bigquery.SchemaField('FLAG_WORK_PHONE', 'INT64'),
    bigquery.SchemaField('FLAG_PHONE', 'INT64'),
    bigquery.SchemaField('FLAG_EMAIL', 'INT64'),
    bigquery.SchemaField('OCCUPATION_TYPE', 'STRING'),
    bigquery.SchemaField('CNT_FAM_MEMBERS', 'FLOAT64')
    
]

table_ref = bigquery.TableReference(dataset, table_name)
table = bigquery.Table(table_ref, schema=schema)
client.create_table(table, exists_ok=True)

def read_messages():
    consumer_config = {"bootstrap.servers": "localhost:9092",
                       "schema.registry.url": "http://localhost:8081",
                       "group.id": "application_record.avro.consumer.2",
                       "auto.offset.reset": "earliest"}

    consumer = AvroConsumer(consumer_config)
    consumer.subscribe(["practice.application_record_Training"])

    while True:
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message is not None:
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
                # INSERT STREAM TO BIGQUERY
                client.insert_rows(table, [message.value()])
            else:
                print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    read_messages()
