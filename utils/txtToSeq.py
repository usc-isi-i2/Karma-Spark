#!/usr/bin/env python

from sys import argv
from pyspark import SparkContext

'''
# Executed as:

spark-submit \
      --master local[1] \
      txtToSeq.py \
      ../sample-data/part-00002-seq ../sample-data/output

'''

if __name__ == "__main__":
    sc = SparkContext(appName="txt-to-seq")

    inputFilename = argv[1]
    outputFilename = argv[2]

    # Read input
    inputRDD = sc.textFile(inputFilename).map(lambda x: json.loads(x))

    # Save the output
    import json
    inputRDD.map(lambda x: (x['url'], json.dumps(x))).coalesce(1).saveAsSequenceFile(outputFilename)

