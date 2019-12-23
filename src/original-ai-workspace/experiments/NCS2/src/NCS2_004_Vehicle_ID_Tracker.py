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
from scipy.spatial import distance as dist
from collections import OrderedDict


# Will continue to use this as a standard for now.

def build_argparser():
    parser = ArgumentParser()
    parser.add_argument("-m", "--model", 
                        help="Path to a vehicle detection .xml file with a trained model.", required=True, type=str)
    #parser.add_argument("-m_va", "--model_va", 
    #                   help="Path to a vehicle attributes .xml file with a trained model.", required=True, type=str)
    #parser.add_argument("-m_lpr", "--model_lpr", 
    #                    help="Path to a license plate detection .xml file with a trained model.", required=True, type=str)

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
    parser.add_argument("-pp", "--plugin_dir", help="Path to a plugin folder", type=str, default=None)

    return parser     
      
# Load a model to box people in an image.
def main():
    log.basicConfig(format="[ %(levelname)s ] %(message)s", level=log.DEBUG, stream=sys.stdout)
    args = build_argparser().parse_args()

    # Step 1:  Loading all the model.
    # Vehicle detection
    model_xml = args.model
    model_bin = os.path.splitext(model_xml)[0] + ".bin"

    # Step 2: Initialize the Plugin ( MYRIAD )
    # This will initialize the myriad device..
    plugin = IEPlugin(device=args.device, plugin_dirs=args.plugin_dir)
    
    # Step 3: Initialiaze the network model.     
    net = IENetwork(model=model_xml, weights=model_bin)    
    
    log.info("DET - Net Loaded.")
    log.info( net.layers)
    
    # Step 3.1: Prepare the input area
    log.info("Preparing input blobs Det, Attr, Read {}".format(net.inputs))
    
    input_blob = next(iter(net.inputs))
    
    # Steo 3.2: Prepare the output area
    log.info("Preparing output blobs")
    out_blob = next(iter(net.outputs))
    
    # Step 3.3: Set the batch size of the network.  
    # Reset these to 1
    net.batch_size = 1
    
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
    
    log.info("DET - Read Net  inputs {} ".format(net.inputs[input_blob].shape) )
    n, c, h, w = net.inputs[input_blob].shape
    
    # Step 4
    log.info("DET - Loading model to the plugin")
    exec_net = plugin.load(network=net)

    # Video processing starts here.
    # initialize our centroid tracker and frame dimensions
    
    # TODO Check rect logic. This seems to have both the existing and the new.
    # Ideally it should only have the new objects - and not have the existing ones.
    # But the D calc should be with the existing array so that we can swap out the right ones.
    nextObjectID = 0
    objects = OrderedDict()
    disappeared = OrderedDict()
    maxDisappeared = 7

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
    
    # Person Counter logic
 
    while cap.isOpened():
        rects = []
        numObjects = 0
        
        if is_async_mode:
            ret, next_frame = cap.read()
        else:
            ret, frame = cap.read()
        if not ret:
            break
        initial_w = cap.get(3)
        initial_h = cap.get(4)

        log.info("Initial_w and Initial_h are {}, {}".format(initial_w, initial_h))

        
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
            
            res = exec_net.requests[cur_request_id].outputs[out_blob]
            # log.info("Objects in result are {}".format(res[0][0]))
            
            for obj in res[0][0]:
                # Draw only objects when probability more than specified threshold
                                    
                if obj[2] > args.prob_threshold:
                    xmin = int(obj[3] * initial_w)
                    ymin = int(obj[4] * initial_h)
                    xmax = int(obj[5] * initial_w)
                    ymax = int(obj[6] * initial_h)
                    class_id = int(obj[1])
                    numObjects += 1
                    
                    # Hold the objects to get their centroid.
                    box = np.array([xmin, ymin, xmax, ymax])                   
                    rects.append(box.astype("int"))
                    # log.info("UPDATED Array {}".format(rects))

                    # Draw box and label\class_id
                    # color = (min(class_id * 12.5, 255), min(class_id * 7, 255), min(class_id * 5, 255))
                    # Highlight Green box.

                    color = (10, 200, 50)
                    cv2.rectangle(frame, (xmin, ymin), (xmax, ymax), color, 2)
                    
                    addlabel= False
                    # Open this if you want labels on the box.
                    if addlabel:
                        det_label = labels_map[class_id] if labels_map else str(class_id)
                        cv2.putText(frame, det_label + ' ' + str(round(obj[2] * 100, 1)) + ' %', (xmin, ymin - 7),
                                cv2.FONT_HERSHEY_COMPLEX, 0.6, color, 1)
                    
                    
            # Calc the centroids.
            log.info("Num Objects in ONE FRAME are {} length of rect is {}".format(numObjects, len(rects)))
            # Only go through the centroid process if there are persons in the frame.
            # If they are hidden - then so be it - then nothing to paint on the frame.
            
            inputCentroids = np.zeros((len(rects), 2), dtype="int")
            # Centroid logic here
            # loop over the bounding box rectangles
            for (i, (startX, startY, endX, endY)) in enumerate(rects):
                # use the bounding box coordinates to derive the centroid
                cX = int((startX + endX) / 2.0)
                cY = int((startY + endY) / 2.0)
                inputCentroids[i] = (cX, cY)
            
            # Store these centroids in the Object array.
            if len(objects) == 0:
                for i in range(0, len(inputCentroids)):
                    objects[nextObjectID] = inputCentroids[i]
                    nextObjectID += 1
            else: 
                # Check the cdist between the two centroids.
                # Leveraget teh cdist to identify match rows and cols.
                # Replace existing centroids when distance ia acceptable. 
                # Add the new ones into the centroid.
                existIds = list(objects.keys())
                existCentroids = list(objects.values())
                
                log.info("CDIST - Existing ObjectIDs are {} Values are {}".format(existIds, existCentroids))
                log.info("Input Centroids - {}".format(list(inputCentroids)))
            
                if len(inputCentroids) > 0: 
                    cdistValues = dist.cdist(np.array(existCentroids), inputCentroids, 'euclidean')
                    log.info("CDIST - Value of are {}".format(cdistValues))
                    
                    # Now that we have the cdist.
                    # Find min rows and cols. 
                    rows = cdistValues.min(axis=1).argsort()
                    log.info("CDIST - Rows are {}".format(rows))
    
                    cols = cdistValues.argmin(axis=1)[rows]
                    log.info("CDIST - Cols are {}".format(cols))
                    
                    usedRows = set()
                    usedCols = set()
                    
                    for (row, col) in zip(rows, cols):
                        # If the row is used / col is used to no substitue.
                        # Here there are cases where one object may leave and another enters at the same time.
                        # For now - if the cdist > 100 even if this the min - do not add it.
                        if row in usedRows or col in usedCols or cdistValues[row][col] > 100:
                            continue
                        
                        trackerID = existIds[row]
                        # Swap out with the new object
                        objects[trackerID] = inputCentroids[col]
                        disappeared[trackerID] = 0;
                        
                        # This combination of row and col is now "used"
                        usedRows.add(row)
                        usedCols.add(col)
                        
                    # compute both the row and column index we have NOT yet
                    # examined
                    unusedRows = set(range(0, cdistValues.shape[0])).difference(usedRows)
                    unusedCols = set(range(0, cdistValues.shape[1])).difference(usedCols)
                    log.info("CDIST - Unused Rows and Cols are {} {}".format(str(unusedRows), str(unusedCols)))
                    
                    # Add the unused objects.
                    for col in unusedCols:
                        log.info("CDIST - Adding New - {}".format(inputCentroids[col]))
                        objects[nextObjectID] = inputCentroids[col]
                        disappeared[nextObjectID] = 0
                        nextObjectID += 1
                    
                    # Get the Tracking ID of the row that needs to be deleted.
                    for row in unusedRows:
                        trackerID = existIds[row]
                        # if trackerID != row:
                        log.info("CDIST - tracker ID and row num {} {}".format(str(trackerID), str(row)))
                        disappeared[trackerID] += 1
                        if disappeared[trackerID] > maxDisappeared:
                            log.info("CDIST - Deleting - {}".format(objects[trackerID]))
                            del objects[trackerID]
                            del disappeared[trackerID]
            
            log.info("Updated Existing Centroids - {}".format(list(objects.values())))
            log.info("Disappeared IDs and Values {}".format(str(disappeared)))
            
            for (objectID, centroid) in objects.items():
                # Only print if the object is in view.
                # draw both the ID of the object and the centroid of the
                # object on the output frame
                
                if len(disappeared) > 0 and int(disappeared[objectID]) == 0 :
                    text = "ID {}".format(objectID)
                    cv2.putText(frame, text, (centroid[0] - 10, centroid[1] - 10),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)
                    cv2.circle(frame, (centroid[0], centroid[1]), 4, (0, 255, 0), -1)

        
        # Draw performance stats
        inf_time_message = "Inference time: N\A for async mode" if is_async_mode else \
            "Inference time: {:.3f} ms".format(det_time * 1000)
        render_time_message = "OpenCV rendering time: {:.3f} ms".format(render_time * 1000)
        async_mode_message = "Async mode is on. Processing request {}".format(cur_request_id) if is_async_mode else \
            "Async mode is off. Processing request {}".format(cur_request_id)
        person_counter_message = "Count of Vehicles: {} ".format(str(nextObjectID))

        cv2.putText(frame, inf_time_message, (15, 15), cv2.FONT_HERSHEY_COMPLEX, 0.5, (200, 10, 10), 1)
        cv2.putText(frame, render_time_message, (15, 30), cv2.FONT_HERSHEY_COMPLEX, 0.5, (10, 10, 200), 1)
        cv2.putText(frame, person_counter_message, (15, 45), cv2.FONT_HERSHEY_COMPLEX, 0.5, (10, 255, 10), 1)
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
    
    # Clean up.
    del exec_net
    del plugin

if __name__ == '__main__':
    sys.exit(main() or 0)

