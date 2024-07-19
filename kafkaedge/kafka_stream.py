import json
import time
import random
from kafka import KafkaProducer


def convert_to_json(data):
    keys = [
        'id', 'Marital status', 'Application mode', 'Application order', 'Course',
        'Daytime/evening attendance', 'Previous qualification', 'Previous qualification (grade)',
        'Nacionality', "Mother's qualification", "Father's qualification", "Mother's occupation",
        "Father's occupation", 'Admission grade', 'Displaced', 'Educational special needs',
        'Debtor', 'Tuition fees up to date', 'Gender', 'Scholarship holder', 'Age at enrollment',
        'International', 'Curricular units 1st sem (credited)', 'Curricular units 1st sem (enrolled)',
        'Curricular units 1st sem (evaluations)', 'Curricular units 1st sem (approved)',
        'Curricular units 1st sem (grade)', 'Curricular units 1st sem (without evaluations)',
        'Curricular units 2nd sem (credited)', 'Curricular units 2nd sem (enrolled)',
        'Curricular units 2nd sem (evaluations)', 'Curricular units 2nd sem (approved)',
        'Curricular units 2nd sem (grade)', 'Curricular units 2nd sem (without evaluations)',
        'Unemployment rate', 'Inflation rate', 'GDP'
    ]
    
    values = data.split(',')
    data_dict = {}

    for i in range(len(keys)):
        key = keys[i]
        value = values[i].strip()

        if key in ['id', 'Marital status', 'Application mode', 'Application order', 'Course',
                   'Daytime/evening attendance', 'Previous qualification', 'Nacionality',
                   "Mother's qualification", "Father's qualification", "Mother's occupation",
                   "Father's occupation", 'Displaced', 'Educational special needs', 'Debtor',
                   'Tuition fees up to date', 'Gender', 'Scholarship holder', 'Age at enrollment',
                   'International', 'Curricular units 1st sem (credited)',
                   'Curricular units 1st sem (enrolled)', 'Curricular units 1st sem (evaluations)',
                   'Curricular units 1st sem (approved)', 'Curricular units 1st sem (without evaluations)',
                   'Curricular units 2nd sem (credited)', 'Curricular units 2nd sem (enrolled)',
                   'Curricular units 2nd sem (evaluations)', 'Curricular units 2nd sem (approved)',
                   'Curricular units 2nd sem (without evaluations)']:
            data_dict[key] = int(value)
        elif key in ['Previous qualification (grade)', 'Admission grade',
                     'Curricular units 1st sem (grade)', 'Curricular units 2nd sem (grade)',
                     'Unemployment rate', 'Inflation rate', 'GDP']:
            data_dict[key] = float(value)
        else:
            data_dict[key] = value

    return data_dict


producer = KafkaProducer(bootstrap_servers="192.168.80.83:9092",
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))


# send 30 messages for testing purpose
i=0
with open('/home/labsoe/kafkaedge/test.csv', 'r') as f:
    for line in f:
        if i != 0:
            try:
                data = convert_to_json(line.strip())
                producer.send("cs411_student_performance", data)
                time.sleep(random.randint(1, 5))
                print(data)
            except Exception as e:
                print (f"Error: {e}")
            
        i+=1
        if i==30:
            break

