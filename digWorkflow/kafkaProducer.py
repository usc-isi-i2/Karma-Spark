
import threading, time
import sys
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
from os import listdir
from os.path import isfile, join

#  pip install kafka-python
#
# Sample Invocation:
# python kafkaProducer.py localhost:9092 scharp-usmtf /Users/dipsy/github/scharp-data/USMTF/PACIFICA_DELTA_BDAREP

class Producer(threading.Thread):
    daemon = True
    def __init__(self, broker, topic, directory):
        threading.Thread.__init__(self)
        self.client = KafkaClient(broker)
        self.producer = SimpleProducer(self.client)
        self.message_dir = directory
        self.topic = topic

    def send_file(self, filename):
        print "Send:", filename
        with file(filename) as f:
           file_contents = f.read()
        self.producer.send_messages(self.topic, file_contents)

    def run(self):
        onlyfiles = [f for f in listdir(self.message_dir) if isfile(join(self.message_dir, f))]
        for f in onlyfiles:
            filename = join(self.message_dir, f)
            self.send_file(filename)
            time.sleep(10)


if __name__ == "__main__":
    Producer(sys.argv[1], sys.argv[2], sys.argv[3]).start()
    time.sleep(1000)
