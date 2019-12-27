#!/bin/bash

# source /opt/intel/computer_vision_sdk/bin/setupvars.sh

# SRC_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/src
# MODELS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/models
# VIDEOS_FOLDER=/home/aiuser/ai-workspace/experiments/NCS2/videos

source /opt/intel/openvino/bin/setupvars.sh

SRC_FOLDER=/root/ai-workspace/experiments/NCS2/src
MODELS_FOLDER=/root/ai-workspace/experiments/NCS2/models
VIDEOS_FOLDER=/root/ai-workspace/experiments/NCS2/videos

# redis-server &
# sleep 2

python3 $SRC_FOLDER/NCS2_026_Q_EmotionGraph.py &
sleep 1

FACE_MODEL_DIR=/opt/intel/openvino/deployment_tools/open_model_zoo/tools/downloader/intel/face-detection-adas-0001
EMOTION_MODEL_DIR=/opt/intel/openvino/deployment_tools/open_model_zoo/tools/downloader/intel/emotions-recognition-retail-0003

# python3 $SRC_FOLDER/NCS2_023_ThreadedFaceEmotionDriver.py -m_fd $MODELS_FOLDER/face/pruned_mobilenet_reduced_ssd_shared_weights/dldt/face-detection-adas-0001-fp16.xml -m_er $MODELS_FOLDER/emotions-recognition-retail-0003/FP16/emotions-recognition-retail-0003.xml -pt_er 0.75 -pt_fd 0.75 -i $VIDEOS_FOLDER/Starbucks_SD_1.mp4

python3 $SRC_FOLDER/NCS2_023_ThreadedFaceEmotionDriver.py -m_fd $FACE_MODEL_DIR/FP16/face-detection-adas-0001.xml -m_er $EMOTION_MODEL_DIR/FP16/emotions-recognition-retail-0003.xml -pt_er 0.6 -pt_fd 0.6 -i $VIDEOS_FOLDER/wb.mp4

