#!/bin/bash
#
# for algorithm in cc_cuvette_integrity \
#                  cc_cuvette_wash \
#                  ia_dark_counts \
#                  ia_fe_pressure \
#                  ia_itv \
#                  ia_pipettor_sample_syringe_backlash \
#                  ia_process_jams_5756 \
#                  ia_process_jams_5758 \
#                  ia_vacuum_leak \
#                  ia_vacuum_pump \
#                  ia_vacuum_sensor \
#                  ia_washzone_aspiration
# do
#     cp ../config/dx/config.csv athena/$algorithm
#     cp rlib/old_common_utils.R athena/$algorithm
#     #
#     cp ../config/dx/config.csv spark/$algorithm
#     cp rlib/old_common_utils.R spark/$algorithm
#     #
#     cp ../config/dx/config.csv athena_flat/$algorithm
#     cp rlib/common_utils.R athena_flat/$algorithm
#     cp rlib/main.R athena_flat/$algorithm
#     #
#     cp ../config/dx/config.csv spark_flat/$algorithm
#     cp rlib/common_utils.R spark_flat/$algorithm
#     cp rlib/main.R spark_flat/$algorithm
# done
#
for file2copy in ../config/spark/config.csv rlib/old_common_utils.R 
do
    find spark/* -maxdepth 0 -type d -print |
    while read dpath
    do
        cp $file2copy $dpath
    done
done
#
for file2copy in ../config/spark/config.csv rlib/common_utils.R rlib/main.R 
do
    find spark_flat/* -maxdepth 0 -type d -print |
    while read dpath
    do
        cp $file2copy $dpath
    done
done
#
for file2copy in ../config/dx/config.csv rlib/old_common_utils.R 
do
    find athena/* -maxdepth 0 -type d -print |
    while read dpath
    do
        cp $file2copy $dpath
    done
done
#
for file2copy in ../config/dx/config.csv rlib/common_utils.R rlib/main.R 
do
    find athena_flat/* -maxdepth 0 -type d -print |
    while read dpath
    do
        cp $file2copy $dpath
    done
done
#
exit 0
