#!/bin/bash

HOME_FOLDER=/home/s
SRC_FOLDER=$HOME_FOLDER/ai-workspace/experiments/NCS2/src
DATA_FOLDER=$HOME_FOLDER/ai-workspace/experiments/NCS2/data

INTEL_MODELS=/opt/intel/computer_vision_sdk/deployment_tools/intel_models
PRE_TRAINED_MODELS=/opt/intel/computer_vision_sdk/deployment_tools/model_downloader

MODELS_FOLDER=$HOME_FOLDER/ai-workspace/experiments/NCS2/videos

ps -ef | grep NCS2_031 | grep -v grep | awk '{print $2}' | xargs kill
sleep 1

python3 $SRC_FOLDER/NCS2_031_Q_AnimateGraph_Forecast.py &
sleep 2

python3 $SRC_FOLDER/NCS2_017_ThreadedAI_EdgeDemo_V2.py --labels $SRC_FOLDER/personLabels.txt -m_va $INTEL_MODELS/vehicle-detection-adas-0002/FP16/vehicle-detection-adas-0002.xml -d 2 -pt_va 0.8 -i_va $MODELS_FOLDER/police_car_6095_shortened_960x540.mp4 -mx_d_va 3 -m_pa $PRE_TRAINED_MODELS/Retail/object_detection/pedestrian/rmnet_ssd/0013/dldt/person-detection-retail-0013-fp16.xml -pt_pa 0.75 -mx_d_pa 50 -i_pa $MODELS_FOLDER/P1033651_SD.mp4 -df $DATA_FOLDER/object_data_file.csv

ps -ef | grep NCS2_031 | grep -v grep | awk '{print $2}' | xargs kill
sleep 1
