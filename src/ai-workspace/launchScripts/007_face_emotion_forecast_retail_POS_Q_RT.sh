#!/bin/bash

# 12/27/2019 - mar
# cannot run. missing files from ai-source ...

# source /opt/intel/computer_vision_sdk/bin/setupvars.sh
source /opt/intel/openvino/bin/setupvars.sh

# HOME_FOLDER=/home/s
HOME_FOLDER=/root/

SRC_FOLDER=$HOME_FOLDER/ai-workspace/experiments/NCS2/src
DATA_FOLDER=$HOME_FOLDER/ai-workspace/experiments/NCS2/data

# INTEL_MODELS=/opt/intel/computer_vision_sdk/deployment_tools/intel_models
# PRE_TRAINED_MODELS=/opt/intel/computer_vision_sdk/deployment_tools/model_downloader

INTEL_MODELS=/opt/intel/openvino/deployment_tools/intel_models
PRE_TRAINED_MODELS=/opt/intel/openvino/deployment_tools/model_downloader

MODELS_FOLDER=$HOME_FOLDER/ai-workspace/experiments/NCS2/videos

ps -ef | grep NCS2_033 | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null
ps -ef | grep NCS2_042 | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null
sleep 1

python3 $SRC_FOLDER/NCS2_033_Q_EmotionForecastGraph.py &
sleep 2

python3 $SRC_FOLDER/NCS2_042_RetailSbucks_POS_DBoard.py &
sleep 2

python3 $SRC_FOLDER/NCS2_023_ThreadedFaceEmotionDriver.py -m_fd $PRE_TRAINED_MODELS/Transportation/object_detection/face/pruned_mobilenet_reduced_ssd_shared_weights/dldt/face-detection-adas-0001-fp16.xml -m_er $INTEL_MODELS/emotions-recognition-retail-0003/FP16/emotions-recognition-retail-0003.xml -pt_er 0.75 -pt_fd 0.75 -i $MODELS_FOLDER/Starbucks_SD_1.mp4

ps -ef | grep NCS2_033 | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null
ps -ef | grep NCS2_042 | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null
sleep 1

