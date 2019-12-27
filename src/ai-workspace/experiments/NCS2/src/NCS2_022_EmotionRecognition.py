#!/usr/bin/env python
"""
 Copyright (c) 2019 Sandeep Sachdeva

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""
from __future__ import print_function
import sys
import os
from argparse import ArgumentParser
import cv2
import numpy as np
import logging as log
import time
from openvino.inference_engine import IENetwork, IEPlugin
from debian.debtags import output

# Will continue to use this as a standard for now.

def build_argparser():
    parser = ArgumentParser()
    parser = ArgumentParser()
    parser.add_argument("-m", "--model", help="Path to an .xml file with a trained model.", required=True, type=str)
    parser.add_argument("-i", "--input",
                        help="Path to video file or image. 'cam' for capturing video stream from camera", required=True,
                        type=str)
    parser.add_argument("-d", "--device",
                        help="Specify the target device to infer on; CPU, GPU, FPGA or MYRIAD is acceptable. Demo "
                             "will look for a suitable plugin for device specified (CPU by default)", default="CPU",
                        type=str)
    parser.add_argument("--labels", help="Labels mapping file", default=None, type=str)
    parser.add_argument("-pt", "--prob_threshold", help="Probability threshold for detections filtering",
                        default=0.5, type=float)

    return parser

# Load a model to box people in an image.
def main():
    log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.INFO, stream=sys.stdout)
    args = build_argparser().parse_args()

    # Step 1:  Loading the model.
    model_xml = args.model
    model_bin = os.path.splitext(model_xml)[0] + ".bin"

    # Step 2: Initialize the Plugin ( MYRIAD )
    # This will initialize the myriad device..
    plugin = IEPlugin(device="MYRIAD", plugin_dirs="")
    
    # Step 3: Initialiaze the network model.     
    net = IENetwork(model=model_xml, weights=model_bin)
    
    log.info("Net Loaded.")
    log.info( net.layers)
    
    # Step 3.1: Prepare the input area
    log.info("Preparing input blobs")
    input_blob = next(iter(net.inputs))
    
    # Steo 3.2: Prepare the output area
    log.info("Preparing output blobs")
    out_blob = next(iter(net.outputs))
    
    # Step 3.3: Set the batch size of the network.
    net.batch_size = len(args.input)
    
    # Step 3.4 Read and pre-process input images
    """
    N = Number in the batch
    C = Channels ( 3 for RGB or 1 for greyscale etc ).
    H = Height 
    W = Width.
    
    Generally for NCHW - this means.
    perm[0] = N
    perm[1] = C
    perm[2] = H
    perm[3] = W.
    
    To change between NCHW to NHWC you have to reset ( or transposse ). In simple terms - set 
    perm[0] = 0
    perm[1] = 2  ( H which was 2 earlier  )
    perm[2] = 3  ( W which was 3 earlier )
    perm[3] = 1  ( C which was 1 earlier ).
    
    Or use (ex in Tensorflow ) -
    tf.transpose(0,2,3,1)
    """
    
    net.batch_size = 1
    n, c, h, w = net.inputs[input_blob].shape
    log.info("Re Read Net inputs {} ".format(net.inputs[input_blob].shape) )
    # log.info(n, c, h, w)
    
    # Step 4: Loading model to the plugin
    log.info("Loading model to the plugin")
    exec_net = plugin.load(network=net)

    
    # Video processing starts here.
    
    # Step 5: Start the video stream.
    log.info(args.input)
    if args.input == 'cam':
        input_stream = 0
    else:
        input_stream = args.input
        assert os.path.isfile(args.input), "Specified input file doesn't exist"
    if args.labels:
        with open(args.labels, 'r') as f:
            labels_map = [x.strip() for x in f]
    else:
        labels_map = ["Neutral", "Happy", "Sad", "Surprise", "Angry"]

    cap = cv2.VideoCapture(input_stream)

    cur_request_id = 0

    log.info("Starting inference ...")
    render_time = 0
    frameCtr = 0
    
    while cap.isOpened():

        ret, frame = cap.read()
        if not ret:
            break

        frameCtr += 1
        
        #if frameCtr%3 == 0:
            
        inf_start = time.time()
        in_frame = cv2.resize(frame, (w, h))
        in_frame = in_frame.transpose((2, 0, 1))  # Change data layout from HWC to CHW
        in_frame = in_frame.reshape((n, c, h, w))
        exec_net.start_async(request_id=cur_request_id, inputs={input_blob: in_frame})
    
        if exec_net.requests[cur_request_id].wait(-1) == 0:
            inf_end = time.time()
            det_time = inf_end - inf_start

            # Parse detection results of the current request
            res = exec_net.requests[cur_request_id].outputs[out_blob]           
                        
            vals=[[0,res[0][0][0][0]],[1,res[0][1][0][0]],[2,res[0][2][0][0]],[3,res[0][3][0][0]],[4,res[0][4][0][0]]]
            log.info("EmotionEngine: Potential values are {}".format(vals))

            index, value = max(vals, key=lambda item: item[1])   
            log.info("EmotionEngine: Index, Value {},{}".format(index,value))
            
            # Draw only objects when probability more than specified threshold    
                
            # Draw performance stats
            emotion_message = "Emotion Found: {}".format(labels_map[index])
            inf_time_message = "Inference time: {:.3f} ms".format(det_time * 1000)
            render_time_message = "OpenCV rendering time: {:.3f} ms".format(render_time * 1000)
            
            cv2.putText(frame, inf_time_message, (15, 15), cv2.FONT_HERSHEY_COMPLEX, 0.5, (200, 10, 10), 1)
            cv2.putText(frame, render_time_message, (15, 30), cv2.FONT_HERSHEY_COMPLEX, 0.5, (10, 10, 200), 1)
            cv2.putText(frame, emotion_message, (15, 45), cv2.FONT_HERSHEY_COMPLEX, 0.5, (10, 200, 10), 1)
            
        # Indent upto this point for Skipping frames.
        
            render_start = time.time()
            res = cv2.resize(frame, (960,540))
            cv2.imshow("Detection Results", res)
            render_end = time.time()
            render_time = render_end - render_start
    
            key = cv2.waitKey(1)
            if key == 27:
                break

    cv2.destroyAllWindows()
    
    
    del exec_net
    del plugin

if __name__ == '__main__':
    sys.exit(main() or 0)
