#!/usr/bin/env python

from sys import argv

from py4j.java_gateway import java_import
from digSparkUtil.fileUtil import FileUtil

from pyspark import SparkContext
from digWorkflow.workflow import Workflow


'''
This sample loads CSV data, applies karma model on the CSV data and as a sample, outputs n3.
You can change karma.output.format to json to output json.

If the workflow gives Out of memory error, you can increase the numPartitions to increase parallelism.

Executed as:


rm -rf ../sample-data/output; spark-submit \
      --archives ../karma.zip \
      --py-files ../lib/python-lib.zip \
      --driver-class-path ~/github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
      karmaWorkflowCSV.py ../sample-data/sample-unicode.txt ../sample-data/output
'''

if __name__ == "__main__":
    sc = SparkContext(appName="karmaCSV")

    java_import(sc._jvm, "edu.isi.karma")

    inputFilename = argv[1]
    outputFilename = argv[2]
    numPartitions = 1

    fileUtil = FileUtil(sc)
    workflow = Workflow(sc)

    #1. Read the input
    inputRDD = workflow.batch_read_csv(inputFilename)

    #2. Apply the karma Model
    outputRDD = workflow.run_karma(inputRDD,
                                   "https://raw.githubusercontent.com/dkapoor/test/master/sample-unicode-model1.ttl",
                                   "http://dig.isi.edu/data",
                                   "http://schema.org/WebPage1",
                                   "https://raw.githubusercontent.com/dkapoor/test/master/sample-unicode-context.json",
                                   num_partitions=numPartitions,
                                   data_type="csv",
                                   additional_settings={"karma.input.delimiter":"\t", "karma.output.format": "n3"})


    #3. Save the output
    outputRDD.map(lambda x: x[1]).saveAsTextFile(outputFilename)
