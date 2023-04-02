from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import csv
from time import sleep

def load_avro_schema_from_file():
    key_schema = avro.load("application_record_key.avsc")
    value_schema = avro.load("application_record_value.avsc")

    return key_schema, value_schema

def send_record():
    key_schema, value_schema = load_avro_schema_from_file()
    
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1"
    }

    producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

    file = open('/home/Archie/final-project/datasets/application_record_edit.csv')
    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
            key = {"ID":  int(row[0])}
            value = {
                "ID": int(row[0]),
                "CODE_GENDER": str(row[1]),
                "FLAG_OWN_CAR": str(row[2]),
                "FLAG_OWN_REALTY": str(row[3]),
                "CNT_CHILDREN": int(row[4]),
                "AMT_INCOME_TOTAL": float(row[5]),
                "NAME_INCOME_TYPE": str(row[6]),
                "NAME_EDUCATION_TYPE": str(row[7]),
                "NAME_FAMILY_STATUS": str(row[8]),
                "NAME_HOUSING_TYPE": str(row[9]),
                "DAYS_BIRTH": int(row[10]),
                "DAYS_EMPLOYED": int(row[11]),
                "FLAG_MOBIL": int(row[12]),
                "FLAG_WORK_PHONE": int(row[13]),
                "FLAG_PHONE": int(row[14]),
                "FLAG_EMAIL": int(row[15]),
                "OCCUPATION_TYPE": str(row[16]),
                "CNT_FAM_MEMBERS": float(row[17])
            }

            try:
                producer.produce(topic='practice.application_record_Training', key=key, value=value)
            except Exception as e:
                print(f"Exception while producing record value - {value}: {e}")
            else:
                print(f"Successfully producing record value - {value}")

            producer.flush()
            sleep(1)

if __name__ == "__main__":
    send_record()
