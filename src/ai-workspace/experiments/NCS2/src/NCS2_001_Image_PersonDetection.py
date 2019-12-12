#!/usr/bin/env python
"""
 Copyright (c) 2018 Sandeep Sachdeva

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
    parser.add_argument("-l", "--cpu_extension",
                        help="MKLDNN (CPU)-targeted custom layers.Absolute path to a shared library with the kernels "
                             "impl.", type=str, default=None)
    parser.add_argument("-pp", "--plugin_dir", help="Path to a plugin folder", type=str, default=None)
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
    plugin = IEPlugin(device=args.device, plugin_dirs=args.plugin_dir)
    
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
        labels_map = None

    cap = cv2.VideoCapture(input_stream)

    cur_request_id = 0
    next_request_id = 1

    log.info("Starting inference in async mode...")
    log.info("To switch between sync and async modes press Tab button")
    log.info("To stop the demo execution press Esc button")
    is_async_mode = False
    render_time = 0
    ret, frame = cap.read()
    while cap.isOpened():
        if is_async_mode:
            ret, next_frame = cap.read()
        else:
            ret, frame = cap.read()
        if not ret:
            break
        initial_w = cap.get(3)
        initial_h = cap.get(4)
        # Main sync point:
        # in the truly Async mode we start the NEXT infer request, while waiting for the CURRENT to complete
        # in the regular mode we start the CURRENT request and immediately wait for it's completion
        inf_start = time.time()
        if is_async_mode:
            in_frame = cv2.resize(next_frame, (w, h))
            in_frame = in_frame.transpose((2, 0, 1))  # Change data layout from HWC to CHW
            in_frame = in_frame.reshape((n, c, h, w))
            exec_net.start_async(request_id=next_request_id, inputs={input_blob: in_frame})
        else:
            in_frame = cv2.resize(frame, (w, h))
            in_frame = in_frame.transpose((2, 0, 1))  # Change data layout from HWC to CHW
            in_frame = in_frame.reshape((n, c, h, w))
            exec_net.start_async(request_id=cur_request_id, inputs={input_blob: in_frame})
        if exec_net.requests[cur_request_id].wait(-1) == 0:
            inf_end = time.time()
            det_time = inf_end - inf_start

            # Parse detection results of the current request
            res = exec_net.requests[cur_request_id].outputs[out_blob]
            for obj in res[0][0]:
                # Draw only objects when probability more than specified threshold
                if obj[2] > args.prob_threshold:
                    xmin = int(obj[3] * initial_w)
                    ymin = int(obj[4] * initial_h)
                    xmax = int(obj[5] * initial_w)
                    ymax = int(obj[6] * initial_h)
                    class_id = int(obj[1])
                    # Draw box and label\class_id
                    # color = (min(class_id * 12.5, 255), min(class_id * 7, 255), min(class_id * 5, 255))
                    # Highlight Green box.
                    color = (10, 200, 50)
                    cv2.rectangle(frame, (xmin, ymin), (xmax, ymax), color, 2)
                    det_label = labels_map[class_id] if labels_map else str(class_id)
                    cv2.putText(frame, det_label + ' ' + str(round(obj[2] * 100, 1)) + ' %', (xmin, ymin - 7),
                                cv2.FONT_HERSHEY_COMPLEX, 0.6, color, 1)

            # Draw performance stats
            inf_time_message = "Inference time: N\A for async mode" if is_async_mode else \
                "Inference time: {:.3f} ms".format(det_time * 1000)
            render_time_message = "OpenCV rendering time: {:.3f} ms".format(render_time * 1000)
            async_mode_message = "Async mode is on. Processing request {}".format(cur_request_id) if is_async_mode else \
                "Async mode is off. Processing request {}".format(cur_request_id)

            cv2.putText(frame, inf_time_message, (15, 15), cv2.FONT_HERSHEY_COMPLEX, 0.5, (200, 10, 10), 1)
            cv2.putText(frame, render_time_message, (15, 30), cv2.FONT_HERSHEY_COMPLEX, 0.5, (10, 10, 200), 1)
            cv2.putText(frame, async_mode_message, (10, int(initial_h - 20)), cv2.FONT_HERSHEY_COMPLEX, 0.5,
                        (10, 10, 200), 1)

        #
        render_start = time.time()
        cv2.imshow("Detection Results", frame)
        render_end = time.time()
        render_time = render_end - render_start

        if is_async_mode:
            cur_request_id, next_request_id = next_request_id, cur_request_id
            frame = next_frame

        key = cv2.waitKey(1)
        if key == 27:
            break
        if (9 == key):
            is_async_mode = not is_async_mode
            log.info("Switched to {} mode".format("async" if is_async_mode else "sync"))

    cv2.destroyAllWindows()
    
    
    del exec_net
    del plugin

if __name__ == '__main__':
    sys.exit(main() or 0)
