#!/bin/bash
#
for algorithm in cc_cuvette_integrity \
                 cc_cuvette_wash \
                 ia_dark_counts \
                 ia_fe_pressure \
                 ia_itv \
                 ia_pipettor_sample_syringe_backlash \
                 ia_process_jams_5756 \
                 ia_process_jams_5758 \
                 ia_vacuum_leak \
                 ia_vacuum_pump \
                 ia_vacuum_sensor \
                 ia_washzone_aspiration
do
    for data in flagged both
    do
        cp ../config/dx/config.csv $data/$algorithm
        cp rlib/utils.R $data/$algorithm
        cp rlib/main.R $data/$algorithm
    done
done
#
exit 0
