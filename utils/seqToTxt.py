#!/usr/bin/env python

from sys import argv

from py4j.java_gateway import java_import
from digSparkUtil.fileUtil import FileUtil

from pyspark import SparkContext

'''
# Executed as:
#
# ./makeSpark.sh
#
rm -rf ../sample-data/output; spark-submit \
      --master local[1] \
      --py-files ../lib/python-lib.zip \
      seqToTxt.py \
      ../sample-data/part-00002-seq ../sample-data/output

'''

if __name__ == "__main__":
    sc = SparkContext(appName="TEST")

    java_import(sc._jvm, "edu.isi.karma")

    inputFilename = argv[1]
    outputFilename = argv[2]

    fileUtil = FileUtil(sc)

    # Read input
    #inputRDD = sc.parallelize(fileUtil.load_file(inputFilename, "sequence", "json").take(100))
    inputRDD = fileUtil.load_file(inputFilename, "sequence", "json")

    # Save the output
    import json
    inputRDD.map(lambda x: x[0] + "\t" + json.dumps(x[1])).coalesce(1).saveAsTextFile(outputFilename)
    # fileUtil.save_file(inputRDD, outputFilename, "text", "json")
