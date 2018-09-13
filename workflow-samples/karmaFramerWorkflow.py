#!/usr/bin/env python

from pyspark import SparkContext, StorageLevel

from py4j.java_gateway import java_import
from sys import argv
from digWorkflow.workflow import Workflow
from digSparkUtil.fileUtil import FileUtil

'''
This workflow applies multiple models on a JSON input dataset, reduces the results and then uses the framer to
output multiple frames/roots and publish the frames to elastic search

Note that whenever a RDD is being used more than once, it should be persisted by calling rdd.persist() as shown in the
code below, else spark could end up re-evaluating the rdd
Also, you need to unpersist the rdd after an action has been executed on the RDD.

export KARMA_USER_HOME=~/github/dig/dig-alignment/versions/3.0/karma

rm -rf ../sample-data/output; spark-submit \
      --master local[*] \
      --executor-memory 8g  --driver-memory 4g \
      --jars ../jars/elasticsearch-hadoop-2.1.2.jar \
      --archives ../karma.zip \
      --py-files ../lib/python-lib.zip \
      --driver-class-path ~/github/Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar \
      karmaFramerWorkflow.py \
      ../sample-data/part-framer-sample sequence 100 \
      ../sample-data/output False localhost 9200 dig

'''

if __name__ == "__main__":
    sc = SparkContext(appName="karmaFramerWorkflow")

    java_import(sc._jvm, "edu.isi.karma")

    inputFilename = argv[1]
    inputType = argv[2]  #text or sequence
    numPartitions = int(argv[3])

    outputFilename = argv[4]
    loadelasticsearch = argv[5] == "True"

    es_server = argv[6]
    es_port = argv[7]
    es_index = argv[8]

    #After applying karma, we would like to reduce the number of partitions
    numFramerPartitions = max(10, numPartitions / 10)
    
    github_base = 'https://raw.githubusercontent.com/usc-isi-i2/dig-alignment/development/versions/3.0'
    context_url = github_base + '/karma/karma-context.json'

    workflow = Workflow(sc)
    fileUtil = FileUtil(sc)

    rdd_list = list()

    #Read the input data
    escorts_rdd = inputRDD = fileUtil.load_file(inputFilename, inputType, "json").partitionBy(numPartitions)
    escorts_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    # Apply the main model
    main_rdd = workflow.run_karma(escorts_rdd,
                                  github_base + '/datasets/ht/CDRv2/main/ht-main-model.ttl',
                                  "http://dig.isi.edu/ht/data/",
                                  "http://schema.org/Offer1",
                                  context_url,
                                  numFramerPartitions)  #Partition the output data by numFramerPartitions and the rest
                                                        #of the workflow works with the same number of partitions
    main_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd_list.append(main_rdd)
    print "main model done"

    # Apply the AdultService Model
    adultservice_rdd = workflow.run_karma(escorts_rdd,
                                          github_base + '/datasets/ht/CDRv2/adultservice/ht-adultservice-model.ttl',
                                          "http://dig.isi.edu/ht/data/",
                                          "http://schema.dig.isi.edu/ontology/AdultService1",
                                          context_url,
                                          numFramerPartitions)
    adultservice_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd_list.append(adultservice_rdd)
    print "adult model done"

    # Apply the Webpage model
    webpage_rdd = workflow.run_karma(escorts_rdd,
                                        github_base + '/datasets/ht/CDRv2/webpage/ht-webpage-model.ttl',
                                        "http://dig.isi.edu/ht/data/",
                                        "http://schema.org/WebPage1",
                                        context_url,
                                        numFramerPartitions)
    webpage_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd_list.append(webpage_rdd)
    print "webpage model done"

    # Apply the Offer Model
    offer_rdd = workflow.run_karma(escorts_rdd,
                                   github_base + '/datasets/ht/CDRv2/offer/ht-offer-model.ttl',
                                   "http://dig.isi.edu/ht/data/",
                                   "http://schema.org/Offer1",
                                   context_url,
                                   numFramerPartitions)
    offer_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd_list.append(offer_rdd)
    print "offer model done"

    # Apply the Seller model
    seller_rdd = workflow.run_karma(escorts_rdd,
                                    github_base + '/datasets/ht/CDRv2/seller/ht-seller-model.ttl',
                                    "http://dig.isi.edu/ht/data/",
                                    "http://schema.dig.isi.edu/ontology/PersonOrOrganization1",
                                    context_url,
                                    numFramerPartitions)
    seller_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    rdd_list.append(seller_rdd)
    print "seller model done"

    # After applying all the karma models on the datasets, we not reduce them so that we can
    # join on same uris and remove duplicates
    reduced_rdd = workflow.reduce_rdds(numFramerPartitions, *rdd_list)
    reduced_rdd = reduced_rdd.persist(StorageLevel.MEMORY_AND_DISK)
    reduced_rdd.setName("reduced_rdd")

    #6. Apply the Framer and contexts

    # We define types to be all Classes in the model that have a uri on which we would
    # like the framer to do the joins
    types = [
        {"name": "AdultService", "uri": "http://schema.dig.isi.edu/ontology/AdultService"},
        {"name": "EmailAddress", "uri": "http://schema.dig.isi.edu/ontology/EmailAddress"},
        {"name": "GeoCoordinates", "uri": "http://schema.org/GeoCoordinates"},
        {"name": "Offer", "uri": "http://schema.org/Offer"},
        {"name": "Organization", "uri": "http://schema.org/Organization"},
        {"name": "PersonOrOrganization", "uri": "http://schema.dig.isi.edu/ontology/PersonOrOrganization"},
        {"name": "PhoneNumber", "uri": "http://schema.dig.isi.edu/ontology/PhoneNumber"},
        {"name": "Place", "uri": "http://schema.org/Place"},
        {"name": "PostalAddress", "uri": "http://schema.org/PostalAddress"},
        {"name": "PriceSpecification", "uri": "http://schema.org/PriceSpecification"},
        {"name": "WebPage", "uri": "http://schema.org/WebPage"},
        {"name": "ImageObject", "uri": "http://schema.org/ImageObject"}
    ]

    # We define the frames for each root that we need
    # the name of the frame will be the name of the type in elastic search and the
    # name of the output folder o hdfs
    frames = [
        {"name": "adultservice", "url": github_base + "/frames/adultservice.json"},
        {"name": "webpage", "url": github_base + "/frames/webpage.json"},
        {"name": "offer", "url": github_base + "/frames/offer.json"},
        {"name": "seller", "url": github_base + "/frames/seller.json"},
        {"name": "phone", "url": github_base + "/frames/phone.json"},
        {"name": "email", "url": github_base + "/frames/email.json"}
    ]

    type_to_rdd_json = workflow.apply_partition_on_types(reduced_rdd, types)
    for type_name in type_to_rdd_json:
        type_to_rdd_json[type_name]["rdd"] = type_to_rdd_json[type_name]["rdd"].persist(StorageLevel.MEMORY_AND_DISK)
        type_to_rdd_json[type_name]["rdd"].setName(type_name)

    framer_output = workflow.apply_framer(reduced_rdd, type_to_rdd_json, frames,
                                              numFramerPartitions,
                                              10)


    # We have the framer output. Now we can save it to disk and load it into Elastic Search
    for frame_name in framer_output:
            framer_output[frame_name] = framer_output[frame_name].coalesce(numFramerPartitions)\
                                                    .persist(StorageLevel.MEMORY_AND_DISK)
            fileUtil.save_file(framer_output[frame_name], outputFilename + "/" + frame_name, "text", "json")

            if not framer_output[frame_name].isEmpty():
                if loadelasticsearch:
                    workflow.save_rdd_to_es(framer_output[frame_name], es_server, es_port, es_index + "/" + frame_name)

    reduced_rdd.unpersist()
    for type_name in type_to_rdd_json:
        type_to_rdd_json[type_name]["rdd"] = type_to_rdd_json[type_name]["rdd"].unpersist()

    for frame_name in framer_output:
        framer_output[frame_name].unpersist()