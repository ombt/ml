#!/bin/bash

# source /opt/intel/computer_vision_sdk/bin/setupvars.sh

source /opt/intel/openvino/bin/setupvars.sh

# SRC_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/src
# MODELS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/models
# VIDEOS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/videos

SRC_FOLDER=/root/sandbox/ml/src/ai-workspace/experiments/NCS2/src
MODELS_FOLDER=/root/sandbox/ml/src/ai-workspace/experiments/NCS2/models
VIDEOS_FOLDER=/root/sandbox/ml/src/ai-workspace/experiments/NCS2/videos


python3 $SRC_FOLDER/NCS2_001_Image_PersonDetection.py -m $MODELS_FOLDER/pedestrian/rmnet_ssd/0013/dldt/person-detection-retail-0013-fp16.xml -d MYRIAD -i $VIDEOS_FOLDER/bus_station_6094_960x540.mp4
