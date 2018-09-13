#!/usr/bin/env python

from pyspark import SparkContext
from py4j.java_gateway import java_import
from digWorkflow.workflow import Workflow
from sys import argv
import sys
from digSparkUtil.fileUtil import FileUtil

'''
This workflow applies karma model on a json file and saves the output json-ld results
To output n3, see karmaWorkflowCSV.py that passes additional parameters to run_karma call.

If the workflow gives Out of memory error, you can increase the numPartitions to increase parallelism.


Executed as:

export KARMA_USER_HOME=your_karma_directory
rm -rf ../sample-data/output; spark-submit \
      --archives ../karma.zip \
      --py-files ../lib/python-lib.zip \
      --driver-class-path ~/github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
      karmaWorkflow.py ../sample-data/part-00002-seq sequence json 1 ../sample-data/output
'''

if __name__ == "__main__":
    sc = SparkContext(appName="karma")

    java_import(sc._jvm, "edu.isi.karma")

    inputFilename = argv[1]
    inputFileType = argv[2]
    inputDataType = argv[3]
    numPartitions = int(argv[4])
    outputFilename = argv[5]

    fileUtil = FileUtil(sc)
    workflow = Workflow(sc)

    #1. Read the input
    inputRDD = fileUtil.load_file(inputFilename, inputFileType, inputDataType).partitionBy(numPartitions)

    #2. Apply the karma Model
    contextUrl = "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/2.0/karma/context.json"
    outputRDD_karma = workflow.run_karma(inputRDD,
                       "https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/2.0/datasets/ht/CDR/ht-model.ttl",
                       "http://dig.isi.edu/ht/data/",
                       "http://schema.org/WebPage1",
                       contextUrl,
                       num_partitions=numPartitions,
                       data_type=inputDataType)

    #3. Apply the content
    outputRDD = workflow.apply_context(outputRDD_karma, contextUrl)

    #3. Save the output
    fileUtil.save_file(outputRDD, outputFilename, "text", "json")
    sys.exit(0)
