import random
import sys
import time
from faker import Faker
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

BROKER_HOST = "72.61.140.176:9092"
SCHEMA_REGISTRY = "http://72.61.140.176:8081"
TOPIC = "treatment_data"

TREATMENT_AVRO_SCHEMA = """
{
    "type": "record",
    "name": "Treatment",
    "namespace": "demo.Treatment",
    "fields": [ 
        {"name": "treatment_id",        "type": "string"},
        {"name": "patient_id",          "type": "string"},
        {"name": "timestamp",           "type": "string"},
        {"name": "age",                 "type": "int"},
        {"name": "gender",              "type": "string"},
        {"name": "blood_type",          "type": "string"},
        {"name": "diagnosis",           "type": "string"},
        {"name": "doctor",              "type": "string"},
        {"name": "room",                "type": "string"},
        {"name": "payment_method",      "type": "string"},
        {"name": "amount",              "type": "double"},
        {"name": "rating",              "type": "int"},
        {"name": "outcome",             "type": "string"}
    ]
}
"""
GENDER = ["Male", "Female"]
BLOOD = ["A","B", "AB", "O"]
DIAGNOSIS = ["Dengue Fever","Diabetes","Asthma","Pneumonia","Heart Disease","Stroke"]
DOCTORS = ["Dr. Citra","Dr. Zhafar","Dr. Dhika","Dr. Claudya"]
ROOMS = ["ICU", "Emergency Room", "General Ward", "VIP Room"]
PAYMENT = ["BPJS", "Insurance","Self-Pay"]
OUTCOME = ["Recovered", "Referred", "Deceased"]

def generate_fake_data(fake: Faker):
    return {
        "treatment_id": fake.uuid4(),
        "patient_id": fake.uuid4(),
        "timestamp": fake.iso8601(),
        "age": random.randint(0, 100),
        "gender": random.choice(GENDER),
        "blood_type": random.choice(BLOOD),
        "diagnosis": random.choice(DIAGNOSIS),
        "doctor": random.choice(DOCTORS),
        "room": random.choice(ROOMS),
        "payment_method": random.choice(PAYMENT),
        "amount": round(random.uniform(100_000, 10_000_000), 2),
        "rating": random.randint(1, 5),
        "outcome": random.choice(OUTCOME)
    }


def main():
    fake = Faker()

    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY})
    avro_serializer = AvroSerializer(schema_registry_client=sr, schema_str=TREATMENT_AVRO_SCHEMA)

    producer = SerializingProducer({
        "bootstrap.servers": BROKER_HOST,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": avro_serializer,
        "linger.ms": 50,
        "enable.idempotence": True,
        "batch.num.messages": 1000,
        "compression.type": "zstd"
    })

    def on_delivery(err, msg):
        if err is not None:
            print(f"Delivery failed: {err}", file=sys.stderr)
        else:
            print(f"Delivered key={msg.key()} to {msg.topic()}[{msg.partition()}]@{msg.offset()}")

    print(f"Producing messages to topic '{TOPIC}' ...")

    try:
        while True:
            record = generate_fake_data(fake)
            producer.produce(
                topic=TOPIC,
                key=record["treatment_id"],
                value=record,
                on_delivery=on_delivery
            )
            producer.poll(0.1)
            time.sleep(random.uniform(0.5, 2))
    except KeyboardInterrupt:
        print("\nCtrl+C received — draining in-flight messages...")
    finally:
        remaining = producer.flush(timeout=15)
        if remaining == 0:
            print("All messages delivered. Bye.")
        else:
            print(f"{remaining} message(s) still pending after timeout.")

if __name__ == "__main__":
    main()
