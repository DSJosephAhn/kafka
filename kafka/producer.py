from kafka import KafkaProducer
import json

msg = {
        'SENSOR_ID': 'DCU02_SPU01_box002_TEMP', 
        'FIELDS': 
        {
            'TEMP_VALUE': '53', 'TEMP_DT': '20220929162210', 'TEMP_STATUS': '1'
            }
            }
print(f'test용 unit_event_msg :\n {msg}')

try:
        bootstrap_server = ['localhost:9092']
        # topicName = 'UnitEventTopic2'
        topicName = 'test-topic'
        producer = KafkaProducer(acks=0,  
        # acks=0 : 프로듀서는 자신이 보낸 메시지에 대해 카프카로부터 확인을 기다리지 않음
        # 메시지의 손실 가능성이 있음, 그러나 빠르게 메시지를 보내야 할 경우 사용
                                bootstrap_servers=bootstrap_server, 
                                api_version = (0,11,5),
        # api_version : 설치한 kafka 버전 혹은 그 보다 낮은 버전의 카프카(0.10.2 == safe default)
                                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        # json.dumps() : python 객체를 json 문자열로 변환
        # encode() : str을 bytes로 인코딩(카프카 메시지는 byte 형태로 송신)
        # value_serializer : 메시지 값을 직렬화
                                request_timeout_ms =2000)

        producer.send(topicName, msg)
        producer.flush()
        print('make_kafka_msg 실행 test용 unit_event_msg 전송')

except Exception as e:
    print(e)