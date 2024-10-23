from flask import Flask, request, jsonify
from kafka import KafkaConsumer, KafkaProducer
import json
from threading import Thread
import numpy as np
from sklearn.linear_model import LinearRegression
import time

app = Flask(__name__)


def optimization():
    reader_id = request.json.get('readerId')
    count_per_sec = request.json.get('countsPerSec')
    print(reader_id, count_per_sec)
    return reader_id, count_per_sec


def train_model(X, y):
    model = LinearRegression()
    model.fit(X, y)
    return model


@app.route('/optimization', methods=['POST'])
def get_optimization():
    reader_id, target_count = optimization()

    last_change_time = time.time()
    max_idle_time = 300  # Максимальное время простоя в секундах

    def apply_optimization():
        X = np.array([])
        y = np.array([])
        data_sequence = []  # Инициализация data_sequence для каждой попытки оптимизации
        model = None  # Инициализация модели для каждой попытки оптимизации
        predicted_count = 0

        last_change_time = time.time()  # Объявление переменной здесь

        bootstrap_servers = 'localhost:9092'
        consumer = KafkaConsumer('rfid_emulation',
                                 bootstrap_servers=bootstrap_servers,
                                 group_id='optimizationGroup',
                                 auto_offset_reset='earliest')
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

        for message in consumer:
            data = json.loads(message.value.decode('utf-8'))
            reader_data = json.loads(data['Reader'])
            reader = reader_data['Reader']
            id = reader['Id']
            count_per_sec = data['CountPerSec']
            print('Id:', id, 'CountPerSec:', count_per_sec)

            if id == reader_id:
                data_sequence.append(data)
                X = np.arange(len(data_sequence)).reshape(-1, 1)
                y = np.array([d['CountPerSec'] for d in data_sequence])

                if len(data_sequence) >= 2:
                    if model is None:
                        model = train_model(X, y)

                    # predicted_count = model.predict(np.array([[len(data_sequence)]]))[0]
                    # print(predicted_count)

                    if predicted_count < target_count:
                        predicted_count = predicted_count+1
                    else:
                        predicted_count = predicted_count-1

                    time.sleep(1)
                    
                    data = {
                        'ReaderId': reader_id,
                        'CountsPerSecTimeMin': max((int(predicted_count) - 2), 0),
                        'CountsPerSecTimeMax': int(predicted_count) + 2,
                        'UpperRssiLevelMin': None,
                        'UpperRssiLevelMax': None,
                        'Tags': None
                    }

                    message_to_send = json.dumps(data).encode('utf-8')
                    print("Sending message to optimizationTopic:", message_to_send)
                    producer.send('optimizationTopic', message_to_send)

                    if predicted_count == target_count:
                        print('ready!')
                        break

                    if int(predicted_count) == data_sequence[-1]['CountPerSec']:
                        current_time = time.time()
                        if current_time - last_change_time > max_idle_time:
                            print('error')
                            return jsonify({'error': 'Данное значение невозможно достичь'}), 400
                        else:
                            last_change_time = current_time

        consumer.close()
        producer.close()

    optimization_thread = Thread(target=apply_optimization)
    optimization_thread.start()

    return {'CountPerSec': target_count}


if __name__ == '__main__':
    app.run(host='localhost', port=5000)