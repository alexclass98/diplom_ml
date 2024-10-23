# importing the required libraries
from time import sleep
from json import dumps
from kafka import KafkaProducer

# initializing the Kafka producer

my_producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# generating the numbers ranging from 1 to 500
message = "1"
while message != "0":
    message = input("Type your message: ")
    my_producer.send("testnum", value=message)

    sleep(2)


