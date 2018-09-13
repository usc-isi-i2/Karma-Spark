#!/usr/bin/env python

from sys import argv

from py4j.java_gateway import java_import
from digSparkUtil.fileUtil import FileUtil

from pyspark import SparkContext
from digWorkflow.workflow import Workflow

'''
This workflow applies 2 models to an input datasets and then reduces the output JSON-LD from both
into a single JSON-LD dataset

 Executed as:

rm -rf ../sample-data/output; spark-submit \
      --master local[1] \
      --archives ../karma.zip \
      --py-files ../lib/python-lib.zip \
      --driver-class-path ~/github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
      karmaReduceWorkflow.py \
      ../sample-data/sample-unicode.json text ../sample-data/output
'''

if __name__ == "__main__":
    sc = SparkContext(appName="karmaReduceWorkflow")

    java_import(sc._jvm, "edu.isi.karma")

    inputFilename = argv[1]
    inputType = argv[2]
    outputFilename = argv[3]
    numPartitions = 1

    workflow = Workflow(sc)
    fileUtil = FileUtil(sc)

    # Read input
    inputRDD = fileUtil.load_file(inputFilename, inputType, "json")

    #1. Apply the first karma Model
    outputRDD1 = workflow.run_karma(inputRDD,
                       "https://raw.githubusercontent.com/dkapoor/test/master/sample-unicode-model1.ttl",
                       "http://dig.isi.edu/ht/data/",
                       "http://schema.org/WebPage1",
                       "https://raw.githubusercontent.com/dkapoor/test/master/sample-unicode-context.json",
                       num_partitions=numPartitions)


    #2. Apply the second Karma Model
    outputRDD2 = workflow.run_karma(inputRDD,
                       "https://raw.githubusercontent.com/dkapoor/test/master/sample-unicode-model2.ttl",
                       "http://dig.isi.edu/ht/data/",
                       "http://schema.org/WebPage1",
                       "https://raw.githubusercontent.com/dkapoor/test/master/sample-unicode-context.json",
                       num_partitions=numPartitions)


    #3. Combined the data and then apply the Karma JSON Reducer
    reducedRDD = workflow.reduce_rdds(numPartitions, outputRDD1, outputRDD2)

    #4. Save the output
    fileUtil.save_file(reducedRDD, outputFilename, "text", "json")
