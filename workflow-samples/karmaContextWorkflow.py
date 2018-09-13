#!/usr/bin/env python

from sys import argv

from py4j.java_gateway import java_import
from digSparkUtil.fileUtil import FileUtil

from pyspark import SparkContext
from digWorkflow.workflow import Workflow

'''
This sample workflow takes in a JSON-LD document and applies context to it and saves the output

Executed as:


rm -rf ../sample-data/output; spark-submit \
      --master local[1] \
      --archives ../karma.zip \
      --py-files ../lib/python-lib.zip \
      --driver-class-path ~/github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
      karmaContextWorkflow.py \
      ../sample-data/sample-unicode-jsonld.json text \
      https://raw.githubusercontent.com/dkapoor/test/master/sample-unicode-context.json \
      ../sample-data/output
'''

if __name__ == "__main__":
    sc = SparkContext(appName="karmaContextWorkflow")

    java_import(sc._jvm, "edu.isi.karma")

    inputFilename = argv[1]
    inputType = argv[2]
    contextUrl = argv[3]
    outputFilename = argv[4]

    fileUtil = FileUtil(sc)
    workflow = Workflow(sc)

    # Read input
    inputRDD = fileUtil.load_file(inputFilename, inputType, "json")

    # Apply the context
    outputRDD = workflow.apply_context(inputRDD, contextUrl)

    # Save the output
    fileUtil.save_file(outputRDD, outputFilename, "text", "json")
