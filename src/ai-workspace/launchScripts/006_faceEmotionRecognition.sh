#!/bin/bash

source /opt/intel/computer_vision_sdk/bin/setupvars.sh

SRC_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/src
MODELS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/models
VIDEOS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/videos

redis-server &
sleep 2
python3 $SRC_FOLDER/NCS2_026_Q_EmotionGraph.py &
sleep 1


python3 $SRC_FOLDER/NCS2_023_ThreadedFaceEmotionDriver.py -m_fd $MODELS_FOLDER/face/pruned_mobilenet_reduced_ssd_shared_weights/dldt/face-detection-adas-0001-fp16.xml -m_er $MODELS_FOLDER/emotions-recognition-retail-0003/FP16/emotions-recognition-retail-0003.xml -pt_er 0.75 -pt_fd 0.75 -i $VIDEOS_FOLDER/Starbucks_SD_1.mp4
