#!/bin/bash

# Launch redis-server for queues
redis-server &
sleep 2

source /opt/intel/computer_vision_sdk/bin/setupvars.sh

SRC_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/src
DATA_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/data
MODELS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/models
VIDEOS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/videos


# Launch the dashboard
python3 $SRC_FOLDER/NCS2_020_Q_AnimateGraph.py &
sleep 1

# Launch the program
python3 $SRC_FOLDER/NCS2_017_ThreadedAI_EdgeDemo_V2.py --labels $SRC_FOLDER/personLabels.txt -m_va $MODELS_FOLDER/vehicle-detection-adas-0002/FP16/vehicle-detection-adas-0002.xml -d 2 -pt_va 0.8 -i_va $VIDEOS_FOLDER/police_car_6095_shortened_960x540.mp4 -mx_d_va 3 -m_pa $MODELS_FOLDER/pedestrian/rmnet_ssd/0013/dldt/person-detection-retail-0013-fp16.xml -pt_pa 0.75 -mx_d_pa 50 -i_pa $VIDEOS_FOLDER/P1033651_SD.mp4 -df $DATA_FOLDER/object_data_file.csv


