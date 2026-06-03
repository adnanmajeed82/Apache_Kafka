from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def publish_student(student):
    """
    Send student data to Kafka topic
    """

    payload = {
        "student_id": student.id,
        "student_name": student.std_name,
        "program": {
            "id": student.program.id,
            "name": student.program.name,
            "plo": student.program.plo,
            "peo": student.program.peo
        }
    }

    producer.send(
        "student-topic",
        value=payload
    )

    producer.flush()

    print(f"Student {student.std_name} sent to Kafka")
