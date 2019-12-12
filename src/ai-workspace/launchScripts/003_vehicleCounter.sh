#!/bin/bash

source /opt/intel/computer_vision_sdk/bin/setupvars.sh

SRC_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/src
MODELS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/models
VIDEOS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/videos

python3 $SRC_FOLDER/NCS2_004_Vehicle_ID_Tracker.py --labels $SRC_FOLDER/personLabels.txt -m $MODELS_FOLDER/vehicle-detection-adas-0002/FP16/vehicle-detection-adas-0002.xml -d MYRIAD -pt 0.8 -i $VIDEOS_FOLDER/police_car_6095_shortened_960x540.mp4
