

# Spark Installation Guide

## Requirements

In order to install Karma-Spark, you need to install this repo, and Web-Karma first.
Open a shell(command prompt) and use git clone commands to get these 2 project.

```
	git clone https://github.com/usc-isi-i2/Karma-Spark.git
	git clone https://github.com/usc-isi-i2/Web-Karma.git
```

## Install Karma

Karma is written in JAVA and will run on Mac, Linux and Windows. To Install Web Karma, you will need Java 1.7 and Maven 3.0.

Java 1.7

Download from http://www.oracle.com/technetwork/java/javase/downloads/index.html
Make sure JAVA_HOME environment variable is pointing to JDK 1.7

Maven 3.0

Make sure that M2_HOME and M2 environment variables are set as described in Maven Installation Instructions: http://maven.apache.org/download.cgi

When system requirements are satisfied, run the following commands:

```
	cd Web-Karma
	git checkout development
	mvn clean install –DskipTests
```


“BUILD SUCCESS” indicates build successfully. Then type:


```
	cd karma-spark
	mvn package -P shaded -Dmaven.test.skip=true
```

“BUILD SUCCESS” indicates build successfully. Then type:

```
	cd target
	ls
```


Go to find the karma-spark-0.0.1-SNAPSHOT-shaded.jar file and write down the absolute path of this file. For example,
…… /Web-Karma/karma-spark/target/karma-spark-0.0.1-SNAPSHOT-shaded.jar


  
## Install Spark

1. Download and untar [hadoop cloudera](http://archive.cloudera.com/cdh5/cdh/5/hadoop-2.6.0-cdh5.5.0.tar.gz) in `<hadoop_dir>`
2. Download and untar [spark cloudera version 1.5](http://archive.cloudera.com/cdh5/cdh/5/spark-1.5.0-cdh5.5.0.tar.gz) in `<spark_dir>`
3. Setup spark configuration. Create a new file `<spark_dir>/conf/spark-env.sh` with the following content. Replace `SPARK_HOME` and `HADOOP_HOME` folders with your paths

  ```
  #!/usr/bin/env bash
  
  # This file is sourced when running various Spark programs.
  # Copy it as spark-env.sh and edit that to configure Spark for your site.
  
  # Options read when launching programs locally with
  # ./bin/run-example or ./bin/spark-submit
  # - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
  export HADOOP_HOME=/Users/karma/hadoop-2.6.0-cdh5.5.0
  export SPARK_HOME=/Users/karma/spark-1.5.0-cdh5.5.0
  
  export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
  export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
  export SPARK_DIST_CLASSPATH=$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/contrib/capacity-scheduler/*.jar:$HADOOP_HOME/share/hadoop/tools/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$SPARK_HOME/lib/*
  ```
  
Also add `<spark_dir>\bin` to you `PATH` variable. Example:

```
	export PATH=/Users/karma/spark-1.5.0-cdh5.5.0/bin:$PATH
```


On the command prompt, type the following commands, you will see “Spark assembly has been built with Hive, including Datanucleus jars on classpath”.

```
	cd ..
	spark-submit
```

## Configuring workflow

```
	cd Karma-Spark
```
### Using make.sh

requirements.txt specifies the pip install repos and versions required for running ht workflow. You can install them by running 
```
    pip install -r requirements.txt
```
Now you have all the dependencies.
Next you can run make.sh, for this you have to configure the path. Please point path to site-packages variable where all the python libraries are there.
virtual environment is highly recommeded.

#### How to install and configure virtual environment
install virtualenvwrapper using
```
	pip install virtualenvwrapper
```
Now add the following environment variables to your 
```
	export WORKON_HOME=/Users/danny/Desktop/.virtualenvs
	export PROJECT_HOME=/Users/danny/Desktop/Devel
	source /opt/local/Library/Frameworks/Python.framework/Versions/2.7/bin/virtualenvwrapper.sh
	export VIRTUALENVWRAPPER_PYTHON=/opt/local/bin/python
```
Now create a virtual enviroment using
```
	mkvirtualenv env1
```

Refer [here](https://virtualenvwrapper.readthedocs.org/en/latest/) for any issues regarding virtual environment.

check where your site-packages for virtualenv are, for me it looks like
```
	path=/Users/rajagopal/Desktop/github_repos/.virtualenvs/env1/lib/python2.7/site-packages
```
After setting the path, you run make.sh now, this will create a python-lib.zip in lib folder which has all the .py files required for running ht workflow.

### Using makeSpark.sh for development purposes:

Edit the ``makeSpark.sh` file and chenge the first line `entity=~/blahblah/lib/python2.7/site-packages/dig-entity-merger`. Enter the path to your dig-entity-merger site-packages path. Similarly update the paths for other variables digsparkutil and digStylometry also.

Now Run:
```
	./makeSpark.sh
```
This bundles all files are required for supporting the workflow in `lib\python-lib.zip`

## Test with an example

Go to the file where you put Karma-Spark, then go to the folder `workflow-samples`. This contains sample code to run workflows.
* `karmaWorkflow.py` applies a karma model on a JSON file and saves the JSON-LD output
* `karmaWorkflowCSV.py` applies karma model on a CSV file and outputs data as N3
* `karmaReduceWorkflow.py` shows how to apply multiple models on a dataset and then combine/reduce them
* `karmaContextWorkflow.py` shows how to apply context to a JSON-LD file
* `karmaFramerWorkflow.py` applies multiple models on a JSON input dataset, reduces the results and then uses the framer to output multiple frames/roots and publish them to Elastic Search

The instructions on how to run the workflow are commented at the top of the workflow's python file.

