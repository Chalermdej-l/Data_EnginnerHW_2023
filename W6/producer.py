import csv
from time import sleep
from typing import Dict
from kafka import KafkaProducer
# import logging
# logging.basicConfig(level=logging.DEBUG)
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH_FHV,INPUT_DATA_PATH_GREEN, PRODUCE_TOPIC_RIDES_CSV_GREEN,PRODUCE_TOPIC_RIDES_CSV_FHV


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideCSVProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
        # self.producer = Producer(producer_props)

    @staticmethod
    def read_records(resource_path: str,type : str):
        records, ride_keys = [], []
        i = 0
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            if type=='green':                
                for row in reader:
                    records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[6]}, {row[7]}, {row[8]}, {row[9]},{row[16]},{row[17]}')
                    ride_keys.append(str(row[0]))
                    i += 1
                    if i == 5000:
                        break
            elif type =='fhv':
                for row in reader:
                    records.append(f'{row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}')
                    ride_keys.append(str(row[0]))
                    i += 1
                    if i == 5000:
                        break
            else:
                print('Pass in wrong type.')
                return None
        return zip(ride_keys, records)

    def publish(self, topic: list, records_green: [str, str],records_fhv: [str, str]):

        for to in topic:
            if to =='rides_green':
                records = records_green
            elif to =='rides_fhv' :
                records = records_fhv
            else:
                print('Wronge record pass')
                return None

            for key_value in records:
                key, value = key_value
                try:
                    self.producer.send(topic=to, key=key, value=value)
                    print(f"Producing record for <key: {key}, value:{value}>")
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"Exception while producing record - {value}: {e}")

            self.producer.flush()
            sleep(1)


if __name__ == "__main__":
    config = {
        'bootstrap_servers': [BOOTSTRAP_SERVERS],
        'api_version':(0,11,5),
        'key_serializer': lambda x: x.encode('utf-8'),
        'value_serializer': lambda x: x.encode('utf-8')
    }

    producer = RideCSVProducer(props=config)
    ride_records_green = producer.read_records(resource_path=INPUT_DATA_PATH_GREEN,type='green')
    ride_records_fhv = producer.read_records(resource_path=INPUT_DATA_PATH_FHV,type='fhv')


    producer.publish(topic=[PRODUCE_TOPIC_RIDES_CSV_FHV,PRODUCE_TOPIC_RIDES_CSV_GREEN], records_fhv=ride_records_fhv,records_green=ride_records_green)
