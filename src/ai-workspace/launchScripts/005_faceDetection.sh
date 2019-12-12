#!/bin/bash

source /opt/intel/computer_vision_sdk/bin/setupvars.sh

SRC_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/src
MODELS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/models
VIDEOS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/videos

python3 $SRC_FOLDER/NCS2_021_FaceEmotionDetection.py -m $MODELS_FOLDER/face/sqnet1.0modif-ssd/0004/dldt/face-detection-retail-0004-fp16.xml -pt 0.75 -i $VIDEOS_FOLDER/Starbucks_SD_1.mp4
