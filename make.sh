#!/usr/bin/env bash
#path to site-packages
path=/Users/rajagopal/Desktop/github_repos/.virtualenvs/tok06/lib/python2.7/site-packages
rm -rf lib
mkdir lib
cd lib
mkdir digSparkUtil
mkdir digTokenizer
mkdir digLshClustering
mkdir digStylometry

cp -r $path/digEntityMerger/ ./
cp -r $path/digSparkUtil/ digSparkUtil
cp -r $path/digTokenizer/ digTokenizer
cp -r $path/digLshClustering/ digLshClustering
cp -r $path/digStylometry/ digStylometry
mkdir digWorkflow
cp ../digWorkflow/* ./digWorkflow/
zip -r python-lib.zip *
cd ..

