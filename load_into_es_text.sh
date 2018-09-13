nohup spark-submit \
--master local[2] \
--executor-memory 4g   --driver-memory 4g \
--jars /Users/amandeep/softwares/jars/elasticsearch-hadoop-2.3.2.jar,/Users/amandeep/softwares/jars/spark-examples_2.10-1.0.0-cdh5.1.7.jar \
--py-files /Users/amandeep/Github/dig-workflows/pySpark-workflows/lib/python-lib.zip \
load_into_es_text.py  \
/Users/amandeep/Downloads/gt-v02   dig-nov-eval-gt-04 ads &
