#!/usr/bin/env bash
entity=~/Github/dig-entity-merger
sparkUtilVersion=1.0.14

sparkUtilFilename=digSparkUtil-$sparkUtilVersion
sparkutil=https://pypi.python.org/packages/source/d/digSparkUtil/$sparkUtilFilename.tar.gz#md5=5f8193c94f7e3e8076e5d69c2f7d6911
rm -rf lib
mkdir lib
cd lib
curl -o ./sparkUtil.tar.gz $sparkutil
tar -xf sparkUtil.tar.gz
mv -f $sparkUtilFilename/* ./
rm -rf $sparkUtilFilename
rm sparkUtil.tar.gz


mkdir digWorkflow
cp ../digWorkflow/* ./digWorkflow/
#cp ../digWorkflow/calender.py .
mkdir digEntityMerger
cp $entity/digEntityMerger/*.py ./digEntityMerger/

mkdir tldextract
#cp /Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages/tldextract/* tldextract/
cp ~/Downloads/tldextract-2.0.1/tldextract/* tldextract/
mkdir tldextract/idna
cp -r ~/Downloads/idna-2.1/idna/* ./tldextract/idna/
cp /Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/site-packages/requests_file.py ./tldextract

zip -r python-lib.zip *
cd ..
