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
import os
import cv2 as cv
import numpy as np
import logging as log
import time 
import datetime
from openvino.inference_engine import IENetwork, IEPlugin
from scipy.spatial import distance as dist
from collections import OrderedDict
from RedisQueue import RedisQueue

class FaceDetection(object):
    
    def __init__(self, id, emotionRec, fvs, args):
        self._id = id
        self.cur_request_id = 0
        self.next_request_id = 1
        self.numObjects = 0
        self.net = IENetwork()
        self.n = 0
        self.c = 0
        self.h = 0
        self.w = 0
        self.input_blob = 1
        self.out_blob = 1
        self.exec_net = 1
        self.plugin = 1
        self._q = RedisQueue('test')
        
        self.fvs = fvs
        self.emRec = emotionRec
        
        self.obj_type = "Faces"
        self.cv2 = cv
        self.input_stream = args.input
        self.prob_threshold = args.prob_threshold_fd
        self.model_xml= args.model_fd
                
        self._load_model(self.model_xml, args)
        self.cv2.setUseOptimized(True)
    
    def _load_model(self, model_xml, args):
    
        # Step 1:  Loading all the model.
        # Vehicle detection
        model_bin = os.path.splitext(model_xml)[0] + ".bin"
    
        # Step 2: Initialize the Plugin ( MYRIAD )
        # This will initialize the myriad device..
        self.plugin = IEPlugin("MYRIAD", plugin_dirs="")
        # self.plugin = IEPlugin("CPU", plugin_dirs="")
        
        # Step 3: Initialiaze the network model.     
        self.net = IENetwork(model=model_xml, weights=model_bin)    
        
        log.info("FaceDetection : Net Loaded.")
        log.info( self.net.layers)
    
        # Step 3.1: Prepare the input area
        log.info("FaceDetection : Preparing input blobs Det, Attr, Read {}".format(self.net.inputs))
        
        self.input_blob = next(iter(self.net.inputs))
        
        # Steo 3.2: Prepare the output area
        log.info("FaceDetection : Preparing output blobs")
        self.out_blob = next(iter(self.net.outputs))
        
        # Step 3.3: Set the batch size of the network.  
        # Reset these to 1
        self.net.batch_size = 1
        
        # Step 3.4 Read and pre-process input images       
        log.info("FaceDetection : Read Net  inputs {} ".format(self.net.inputs[self.input_blob].shape) )
        self.n, self.c, self.h, self.w = self.net.inputs[self.input_blob].shape
    
        # Step 4
        log.info("FaceDetection :  Loading model to the plugin")
        self.exec_net = self.plugin.load(network=self.net)
        

    def executeModelThreaded(self, args):    
    
        # Video processing starts here.
        
        nextObjectID = 0
        objects = OrderedDict()
        disappeared = OrderedDict()
    
        # Step 5: Start the video stream.
        labels_map = None
        #BGR
        emotions_labels_map = ["Neutral","Happy","Sad","Surprise","Angry"]
        emotions_color_map = [[200,200,10],[10,200,10],[200,10,10],[10,200,200],[10,10,200]]
        
        log.info("FaceDetection : THREADED frames from File...")
        self.cur_request_id = 0
        render_time = 0
        
        # Person Counter logic
        frame_counter = 0
        
        while self.fvs.more():
            frame_counter += 1
                
            if frame_counter%2 == 0:
                
                rects = []
                numObjects = 0
                  
                # Added Threaded code.           
                # ret, frame = cap.read()
                frame = self.fvs.read()          
                if frame is None:
                    break;
                
                (initial_h, initial_w) = frame.shape[:2]
                
                log.info("FaceDetection : Initial_w and Initial_h are {}, {}".format(initial_w, initial_h))
        
                
                # Main sync point:
                # in the truly Async mode we start the NEXT infer request, while waiting for the CURRENT to complete
                # in the regular mode we start the CURRENT request and immediately wait for it's completion
                inf_start = time.time()
                in_frame = self.cv2.resize(frame, (self.w, self.h))
                in_frame = in_frame.transpose((2, 0, 1))  # Change data layout from HWC to CHW
                in_frame = in_frame.reshape((self.n, self.c, self.h, self.w))
                self.exec_net.start_async(request_id=self.cur_request_id, inputs={self.input_blob: in_frame})
    
                inf_end = time.time()
                det_time = inf_end - inf_start
                
                if self.exec_net.requests[self.cur_request_id].wait(-1) == 0:
                    
                    res = self.exec_net.requests[self.cur_request_id].outputs[self.out_blob]
                    # log.info("Objects in result are {}".format(res[0][0]))
                    emotions_in_frame = [0,0,0,0,0]
                    for obj in res[0][0]:
                        # Draw only objects when probability more than specified threshold
                                            
                        if obj[2] > self.prob_threshold:
                            xmin = int(obj[3] * initial_w)
                            ymin = int(obj[4] * initial_h)
                            xmax = int(obj[5] * initial_w)
                            ymax = int(obj[6] * initial_h)
                            class_id = int(obj[1])
                            numObjects += 1
                            
                            blobROI = self.cv2.resize(frame[ymin:ymax, xmin:xmax], (120,160))
                            self.cv2.imshow("The RESIZED Frame is ", blobROI)
                            index = self.emRec.getEmotion(blobROI)
                            log.info("FaceDetection: Index returned is {}".format(emotions_labels_map[index]))
                            emotions_in_frame[index] += 1
                            
                            # Hold the objects to get their centroid.
                            box = np.array([xmin, ymin, xmax, ymax])                   
                            rects.append(box.astype("int"))
        
                            color = emotions_color_map[index]
                            self.cv2.rectangle(frame, (xmin, ymin), (xmax, ymax), color, 2)
                            
                            addlabel= False
                            # Open this if you want labels on the box.
                            if addlabel:
                                det_label = labels_map[class_id] if labels_map else str(class_id)
                                self.cv2.putText(frame, det_label + ' ' + str(round(obj[2] * 100, 1)) + ' %', (xmin, ymin - 7),
                                        self.cv2.FONT_HERSHEY_COMPLEX, 0.6, color, 1)
                            
                            
                    msg = self.obj_type+","+str(datetime.datetime.fromtimestamp(time.time()).isoformat())+","+str(frame_counter)+","+str(numObjects)+","+str(emotions_in_frame[0])
                    msg = msg+","+str(emotions_in_frame[1])+","+str(emotions_in_frame[2])+","+str(emotions_in_frame[3])+","+str(emotions_in_frame[4])
                    self._q.put(msg)
                    # Calc the centroids.
                    log.info("FaceDetection : Num Objects in ONE FRAME are {} length of rect is {}, MSG {}".format(numObjects, len(rects), msg))
                    
                    #obj_data_file.write("{},{},{},{},{}\n".format(self.obj_type, datetime.datetime.fromtimestamp(time.time()).isoformat(), frame_counter, numObjects, nextObjectID))
                    frame_counter += 1
                    
                    # Only go through the centroid process if there are persons in the frame.
                    # If they are hidden - then so be it - then nothing to paint on the frame.
                    
                    
                inf_time_message = "Inference time: {:.3f} ms".format(det_time * 1000)
                render_time_message = "OpenCV rendering time: {:.3f} ms".format(render_time * 1000)
        
                self.cv2.putText(frame, inf_time_message, (15, 15), self.cv2.FONT_HERSHEY_COMPLEX, 0.5, (200, 10, 10), 1)
                self.cv2.putText(frame, render_time_message, (15, 30), self.cv2.FONT_HERSHEY_COMPLEX, 0.5, (10, 10, 200), 1)
        
                render_start = time.time()
               
                if self._id == 0:
                    # res = self.cv2.resize(frame, (960,540))
                    res = self.cv2.resize(frame, (720,405))
                    self.cv2.imshow("Face Detection Results", res)
                    
                render_end = time.time()
                render_time = render_end - render_start
            
                key = self.cv2.waitKey(1)
                if key == 27:
                    break        
        
        if self._id == 0:
            self.cv2.destroyWindow("Face Detection Results")
    
        # Clean up.
        
        del self.exec_net
        del self.plugin
