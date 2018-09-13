import web
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

urls = (
    '/(.*)', 'KafkaProducer'
)

#
# Requires: web.py:    sudo easy_install web.py
#
# Executed as:
# nohup sudo python kafkaProducerService.py 8888 > kafka.log 2>&1 &
#
#
class KafkaProducer:
    def __init__(self):
        self.client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(self.client)

    def POST(self, topic):

        if topic:
            idx = topic.rfind("/")
            if idx != -1:
                topic = topic[idx+1:]
            data = web.data()
            self.producer.send_messages(topic, data)
            return "Added to " + topic
        return "No topic"

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()