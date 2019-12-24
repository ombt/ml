#!/bin/bash
#
# zip up athena flat files for now.
#
oldpwd=$PWD
#
find athena_flat/* -maxdepth 0 -type d -print |
while read dpath
do
    echo "Zipping $dpath ..."
    #
    cd $dpath
    #
    zip -r - *.R > main.zip
    #
    cd $oldpwd
done
#
exit 0
