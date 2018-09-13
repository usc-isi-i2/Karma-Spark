__author__ = 'amandeep'

"""
USE THESE FOR HBASE
self.hbase_host = 'zk04.xdata.data-tactics-corp.com:2181'
self.hbase_table = 'test_ht_aman'
"""

"""
EXECUTE AS
spark-submit  --master local[*]     --executor-memory=4g     --driver-memory=4g    \
 --jars jars/elasticsearch-hadoop-2.2.0-m1.jar,jars/spark-examples_2.10-2.0.0-SNAPSHOT.jar,jars/random-0.0.1-SNAPSHOT-shaded.jar    \
   es2hbase.py     -hostname  els.istresearch.com -port 19200 \
   -username memex -password <es_password> -indexname <esindex> \
    -doctype <esdoctype> -hbasehostname <hbasehostname> \
    -hbasetablename <hbase_tablename>
"""

"""SAMPLE ES READ CONFIGURATION"""
"""    es_read_conf = {
        'es.resource': index + '/' + doc,
        'es.nodes': 'in-namenode02.nj.istresearch.com:39200',
        'es.http.timeout': '2m',
        'es.http.retries': '10',
        'es.index.read.missing.as.empty': 'true'

     }
"""

"""SAMPLE ES WRITE CONFIGURATION"""
""" es_write_conf = {
     "es.nodes" : "dig-es01.istresearch.com, dig-es02.istresearch.com, dig-es03.istresearch.com, dig-es04.istresearch.com, dig-es05.istresearch.com, dig-es06.istresearch.com, dig-es07.istresearch.com, dig-es08.istresearch.com, dig-es09.istresearch.com, dig-es10.istresearch.com",
     "es.port" : str(port),
     "es.nodes.discover" : "false",
     'es.nodes.wan.only': "true",
     "es.resource" : index + '/' + doc, # use domain as `doc_type`
     "es.http.timeout": "30s",
     "es.http.retries": "20",
     "es.batch.write.retry.count": "20", # maximum number of retries set
     "es.batch.write.retry.wait": "300s", # on failure, time to wait prior to retrying
     "es.batch.size.entries": "200000", # number of docs per batch
     "es.mapping.id": "uri" # use `uri` as Elasticsearch `_id`
     }
"""
""" MISC ES CONFIGURATION"""
"""     self.es_conf['es.net.http.auth.user'] = es_username
        self.es_conf['es.net.http.auth.pass'] = es_password
        self.es_conf['es.net.ssl'] = es_ssl
        self.es_conf['es.nodes.discovery'] = "false"
        self.es_conf['es.http.timeout'] = "1m"
        self.es_conf['es.http.retries'] = "1"
        self.es_conf['es.nodes.client.only'] = "false"
        self.es_conf['es.nodes.wan.only'] = "true"
"""
import requests
import json

class ES(object):
    def __init__(self, spark_context, spark_conf, es_read_conf=None, es_write_conf=None):
        self.name = "ES2HBase"
        self.sc = spark_context
        self.conf = spark_conf
        self.es_read_conf=es_read_conf
        self.es_write_conf=es_write_conf

    def es2rdd(self, query):
        self.es_read_conf['es.query'] = query
        es_rdd = self.sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
                                         keyClass="org.apache.hadoop.io.NullWritable",
                                         valueClass="org.apache.hadoop.io.Text",
                                         conf=self.es_read_conf)

        return es_rdd

    def create_index(self, index_name, mapping_file_url):
        node = self.es_write_conf["es.nodes"].split(",")[0].strip()
        command = "http://" + node + ":" + self.es_write_conf["es.port"] + "/" + index_name
        mapping_file = requests.get(mapping_file_url, verify=False)
        if mapping_file.status_code == 200:
            ret = requests.put(command, data=mapping_file.content, verify=False)
            if ret.status_code == 200:
                print("Successfully created index: ", index_name)
            else:
                print("Error creating index:", index_name, ", code:", ret.status_code, ", Response:", ret.content)
        else:
            print("Error: Cannot download mapping file: ", mapping_file_url)

    def create_alias(self, alias_name, indices):
        node = self.es_write_conf["es.nodes"].split(",")[0].strip()
        url = "http://" + node + ":" + self.es_write_conf["es.port"] + "/_aliases"
        command = {"actions":[
            {"remove": {"index":"*", "alias": alias_name}},
            {"add": {"indices": indices, "alias": alias_name}}
        ]}
        print("Post:", url + ", data=" + json.dumps(command))
        ret = requests.post(url, data=json.dumps(command))
        if ret.status_code == 200:
                print("Successfully create alias: ", alias_name)
        else:
            print("Error creating alias:", alias_name, ", code:", ret.status_code, ", Response:", ret.content)

    def rdd2es(self, rdd):

        rdd.saveAsNewAPIHadoopFile(
                path='-',
                outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                keyClass="org.apache.hadoop.io.NullWritable",
                valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                conf=self.es_write_conf)
        print("Done save to ES")