from optparse import OptionParser
from pyspark import SparkContext, SparkConf, StorageLevel
from digWorkflow.elastic_manager import ES
import json

__author__ = 'amandeep'

if __name__ == '__main__':
    parser = OptionParser()


    (c_options, args) = parser.parse_args()
    input_path = args[0]
    index = args[1]
    doc = args[2]

    sc = SparkContext(appName="DIG-LOAD_TO_ES")
    conf = SparkConf()

    es_write_conf = {
        "es.nodes": "10.1.94.103",
        "es.port": "9201",
        "es.nodes.discover": "false",
        'es.nodes.wan.only': "true",
        "es.resource": index + '/' + doc,  # use domain as `doc_type`
        "es.http.timeout": "30s",
        "es.http.retries": "20",
        "es.batch.write.retry.count": "20",  # maximum number of retries set
        "es.batch.write.retry.wait": "300s",  # on failure, time to wait prior to retrying
        "es.batch.size.entries": "200000",  # number of docs per batch
        "es.mapping.id": "cdr_id",  # use `doc_id` as Elasticsearch `_id`
        "es.input.json": "true"
    }

    es_man = ES(sc, conf, es_write_conf=es_write_conf)
    input_rdd = sc.sequenceFile(input_path)# .partitionBy(1000)
    print input_rdd.first()
    es_man.rdd2es(input_rdd)
