from pyspark import SparkContext, SparkConf, StorageLevel
from optparse import OptionParser
from digSparkUtil.fileUtil import FileUtil

if __name__ == '__main__':
    parser = OptionParser()

    (c_options, args) = parser.parse_args()
    print "Got options:", c_options


    filename = args[0]
    file_format = args[1]
    out_filename = args[2]
    out_format = args[3]
    uris = args[4].split(",")

    print "Filename", filename, file_format
    print "Output:", out_filename, out_format
    print "Filter:", args[4]


    sc = SparkContext(appName="DIG-FILTER")
    conf = SparkConf()

    fileUtil = FileUtil(sc)
    input_rdd = fileUtil.load_file(filename, file_format, "json")
    output_rdd = input_rdd.filter(lambda x: x[0] in uris).coalesce(1)
    fileUtil.save_file(output_rdd, out_filename, out_format, "json")
