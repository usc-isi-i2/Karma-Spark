#!/usr/bin/env python
import sys
import json

from py4j.java_gateway import java_import

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from digWorkflow.workflow import Workflow

'''
Executed as:

./makeSpark.sh

spark-submit \
      --jars ../jars/elasticsearch-hadoop-2.1.2.jar,../jars/spark-streaming-kafka-assembly_2.10-1.5.0.jar,~/github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
      --archives ../karma.zip \
      --py-files ../lib/python-lib.zip \
      --driver-class-path ~/github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
      karmaStreamingWorkflow.py localhost:9092 karma-cdr
'''

if __name__ == "__main__":
    sc = SparkContext(appName="TEST")
    ssc = StreamingContext(sc, 5)

    java_import(sc._jvm, "edu.isi.karma")

    brokers = sys.argv[1]
    topics = sys.argv[2].split(",")

    # Create the Kafka Stream, start reading messages from NOW
    # kvs = KafkaUtils.createDirectStream(ssc,
    #                                     topics,
    #                                     {"metadata.broker.list": brokers})

    # This is read everything on all partitions on the queue.
    # auto.offset.reset: smallest -> Read all data on the queue
    #                  : largest ->  Start reading from now, the largest offset. You can
    #                   also omit auto.offset.reset and that starts at teh largest offset
    #                   then
    kvs = KafkaUtils.createDirectStream(ssc,
                                        topics,
                                        {"metadata.broker.list": brokers,
                                         "auto.offset.reset": "smallest"})

    kvs.pprint()

    # Apply the karma Model
    workflow = Workflow(sc)

    inputDStream = kvs.map(lambda x: ("karma", json.loads(x[1])))
    outputDStream = inputDStream.transform(lambda rdd: workflow.run_karma(rdd,
                       "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/2.0/datasets/ht/CDR/ht-model.ttl",
                       "http://dig.isi.edu/ht/data/",
                       "http://schema.org/WebPage1",
                       "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/2.0/karma/context.json"))
    # outputDStream = inputDStream
    final = outputDStream.transform(lambda rdd: workflow.save_rdd_to_es(rdd, "localhost", "9200", "karma/Webpage"))
    final.pprint()

    # Start streaming
    ssc.start()
    ssc.start()
    ssc.awaitTermination()
